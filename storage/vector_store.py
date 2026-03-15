"""
VectorStore
===========

Handles pgvector connection and embedding upserts.

Each article is stored as a row with:
  - content_id  : SHA1 hex digest (primary key, matches articles.db)
  - url         : article URL
  - title       : article title
  - embedding   : 384-dim float vector (all-MiniLM-L6-v2)
  - embedded_at : timestamp of when the embedding was generated
"""

import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from pgvector.psycopg2 import register_vector

from src.utils.logger import get_logger

logger = get_logger("storage.vector_store")

EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output dimension

CREATE_TABLE_SQL = f"""
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS article_embeddings (
    content_id   TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    title        TEXT,
    embedding    vector({EMBEDDING_DIM}),
    embedded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO article_embeddings (content_id, url, title, embedding, embedded_at)
VALUES (%(content_id)s, %(url)s, %(title)s, %(embedding)s, %(embedded_at)s)
ON CONFLICT (content_id) DO UPDATE SET
    embedding   = EXCLUDED.embedding,
    embedded_at = EXCLUDED.embedded_at;
"""


def _get_conn() -> psycopg2.extensions.connection:
    """Create a psycopg2 connection from environment variables."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "news_pipeline"),
        user=os.getenv("POSTGRES_USER", "ayo"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    register_vector(conn)
    return conn


class VectorStore:
    """Manages pgvector connection and article embedding upserts."""

    def __init__(self) -> None:
        self.conn = _get_conn()
        logger.info("vector_store: connected to pgvector")

    def create_table(self) -> None:
        """Create the article_embeddings table and vector extension if not present."""
        with self.conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self.conn.commit()
        logger.info("vector_store: table ready (dim=%d)", EMBEDDING_DIM)

    def upsert_batch(self, records: list[dict]) -> int:
        """
        Upsert a batch of embedding records.

        Each record must have:
          content_id : str
          url        : str
          title      : str | None
          embedding  : list[float]  (length = EMBEDDING_DIM)

        Returns the number of rows upserted.
        """
        if not records:
            return 0

        rows = [
            {
                "content_id": r["content_id"],
                "url": r["url"],
                "title": r.get("title"),
                "embedding": r["embedding"],
                "embedded_at": datetime.now(timezone.utc),
            }
            for r in records
        ]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows)
        self.conn.commit()

        logger.info("vector_store: upserted %d embeddings", len(rows))
        return len(rows)

    def already_embedded(self, content_ids: list[str]) -> set[str]:
        """
        Return the subset of content_ids that already have embeddings.
        Used to skip re-embedding articles that haven't changed.
        """
        if not content_ids:
            return set()

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT content_id FROM article_embeddings WHERE content_id = ANY(%s)",
                (content_ids,),
            )
            return {row[0] for row in cur.fetchall()}

    def already_embedded_all(self) -> set[str]:
        """Return all content_ids currently stored in pgvector."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT content_id FROM article_embeddings")
            return {row[0] for row in cur.fetchall()}

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("vector_store: connection closed")
