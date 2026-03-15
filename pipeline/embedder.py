"""
Embedder
========

Batch job that runs after the ingestion pipeline.

Reads persisted articles from SQLite (data/articles.db), generates sentence
embeddings using all-MiniLM-L6-v2, and upserts them into pgvector.

Design
------
- Queries SQLite directly for articles not yet in pgvector (idempotent)
- Processes in batches of BATCH_SIZE for efficient CPU utilisation
- Text = title + ". " + content (title anchors the semantic meaning)

Usage
-----
    python -m src.pipeline.embedder
"""

import sqlite3
from pathlib import Path

from sentence_transformers import SentenceTransformer

from src.storage.vector_store import VectorStore
from src.utils.logger import get_logger

logger = get_logger("pipeline.embedder")

MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 32
DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "articles.db"


def build_text(article: dict) -> str:
    """Combine title and content into a single string for embedding."""
    title = (article.get("title") or "").strip()
    content = (article.get("content") or "").strip()
    if title and content:
        return f"{title}. {content}"
    return title or content


def load_unembedded_articles(db_path: Path, already_done: set[str]) -> list[dict]:
    """
    Query SQLite for articles that don't yet have embeddings.
    Filters out content_ids already present in pgvector.
    """
    if not db_path.exists():
        logger.warning("embedder: database not found at %s", db_path)
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT content_id, url, title, content FROM meta WHERE content_id IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()

    articles = [dict(row) for row in rows if row["content_id"] not in already_done]
    logger.info(
        "embedder: %d total articles in db, %d pending embedding",
        len(rows),
        len(articles),
    )
    return articles


def run_embedder() -> None:
    """
    Main entry point for the embedding job.

    1. Connect to pgvector and get already-embedded content_ids
    2. Query SQLite for articles not yet embedded
    3. Generate embeddings in batches
    4. Upsert to pgvector
    """
    store = VectorStore()
    store.create_table()

    # Get all content_ids currently in pgvector
    already_done = store.already_embedded_all()

    pending = load_unembedded_articles(DB_PATH, already_done)

    if not pending:
        logger.info("embedder: nothing to embed, exiting")
        store.close()
        return

    logger.info("embedder: loading model %s", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)

    total_upserted = 0

    for batch_start in range(0, len(pending), BATCH_SIZE):
        batch = pending[batch_start : batch_start + BATCH_SIZE]
        texts = [build_text(a) for a in batch]

        # Returns numpy array shape (batch_size, 384)
        embeddings = model.encode(
            texts, show_progress_bar=False, normalize_embeddings=True
        )

        records = [
            {
                "content_id": article["content_id"],
                "url": article.get("url", ""),
                "title": article.get("title"),
                "embedding": embedding.tolist(),
            }
            for article, embedding in zip(batch, embeddings)
        ]

        total_upserted += store.upsert_batch(records)
        logger.info(
            "embedder: batch %d/%d done",
            min(batch_start + BATCH_SIZE, len(pending)),
            len(pending),
        )

    store.close()
    logger.info("embedder: finished — %d embeddings upserted", total_upserted)


if __name__ == "__main__":
    run_embedder()
