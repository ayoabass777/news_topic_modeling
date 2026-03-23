"""
SemanticSearchService
=====================

Translates a natural-language query into a vector, searches pgvector,
then hydrates the results with full article metadata from SQLite.

Analogy: keyword search is like ctrl-F in a book — it only finds exact words.
Semantic search is like asking someone who *read* the book — they understand
meaning, so "transfer rumours" also finds articles about "player swap deals".

Usage
-----
    service = SemanticSearchService()
    results = service.search("Arsenal midfield transfer targets", limit=10)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from sentence_transformers import SentenceTransformer

from src.storage.vector_store import VectorStore, EMBEDDING_DIM
from src.utils.logger import get_logger

logger = get_logger("services.semantic_search")

MODEL_NAME = "all-MiniLM-L6-v2"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "src" / "data" / "articles.db"


class SemanticSearchService:
    """
    End-to-end semantic search over ingested news articles.

    Flow:
        user query (text)
            → encode with same model used at ingest time
            → cosine-similarity search in pgvector
            → hydrate with full article metadata from SQLite
            → return ranked results
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        lazy_model: bool = True,
    ) -> None:
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self._model: SentenceTransformer | None = None
        self._store: VectorStore | None = None

        # Eager-load the vector store (cheap), lazy-load the model (expensive)
        self._store = VectorStore()
        if not lazy_model:
            self._load_model()

        logger.info(
            "semantic_search: initialised (db=%s, embeddings=%d)",
            self.db_path,
            self._store.count(),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        limit: int = 10,
        min_similarity: float = 0.0,
        league: Optional[str] = None,
        signal_type: Optional[str] = None,
    ) -> list[dict]:
        """
        Search for articles semantically similar to *query*.

        Returns a list of dicts, each containing full article metadata
        plus a `similarity` score (0-1, higher = more relevant).
        """
        if not query.strip():
            return []

        model = self._load_model()

        # Encode query with the SAME model used at ingest → vectors live in the same space
        query_embedding = model.encode(
            query, normalize_embeddings=True
        ).tolist()

        # Fetch more than `limit` from pgvector so we can post-filter
        raw_hits = self._store.search(query_embedding, limit=limit * 3)

        # Apply similarity floor
        hits = [h for h in raw_hits if h["similarity"] >= min_similarity]

        # Hydrate with full article metadata from SQLite
        content_ids = [h["content_id"] for h in hits]
        metadata_map = self._load_metadata(content_ids)

        results = []
        for hit in hits:
            cid = hit["content_id"]
            meta = metadata_map.get(cid, {})

            # Post-filter by league / signal_type if requested
            if league and meta.get("league") != league and meta.get("bucket") != league:
                continue
            if signal_type and meta.get("signal_type") != signal_type:
                continue

            result = {
                **meta,
                "content_id": cid,
                "url": hit.get("url") or meta.get("url", ""),
                "title": hit.get("title") or meta.get("title"),
                "similarity": round(hit["similarity"], 4),
            }
            results.append(result)

            if len(results) >= limit:
                break

        logger.info(
            "semantic_search: query=%r → %d results (top sim=%.3f)",
            query[:60],
            len(results),
            results[0]["similarity"] if results else 0.0,
        )
        return results

    def stats(self) -> dict:
        """Return basic stats about the semantic search index."""
        return {
            "embedding_count": self._store.count(),
            "embedding_dim": EMBEDDING_DIM,
            "model": MODEL_NAME,
            "db_path": str(self.db_path),
        }

    def close(self) -> None:
        if self._store:
            self._store.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_model(self) -> SentenceTransformer:
        if self._model is None:
            logger.info("semantic_search: loading model %s", MODEL_NAME)
            self._model = SentenceTransformer(MODEL_NAME)
        return self._model

    def _load_metadata(self, content_ids: list[str]) -> dict[str, dict]:
        """
        Fetch full article rows from SQLite for a list of content_ids.
        Returns {content_id: {column: value, ...}}.
        """
        if not content_ids or not self.db_path.exists():
            return {}

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            placeholders = ",".join("?" for _ in content_ids)
            rows = conn.execute(
                f"SELECT * FROM meta WHERE content_id IN ({placeholders})",
                content_ids,
            ).fetchall()
            return {row["content_id"]: dict(row) for row in rows}
        finally:
            conn.close()
