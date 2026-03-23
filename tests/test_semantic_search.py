"""
Tests for the semantic search layer.

These tests verify the VectorStore.search() method and the
SemanticSearchService end-to-end flow against a real pgvector instance.
Requires Docker Compose services running (docker compose up -d).
"""

import os
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Unit tests (no database required)
# ---------------------------------------------------------------------------

class TestVectorStoreSearchSQL:
    """Verify that VectorStore.search() builds the right query."""

    @patch("src.storage.vector_store._get_conn")
    def test_search_returns_list_of_dicts(self, mock_get_conn):
        """search() should return list[dict] with content_id, url, title, similarity."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("content_id",), ("url",), ("title",), ("similarity",)
        ]
        mock_cursor.fetchall.return_value = [
            ("abc123", "https://example.com/1", "Test Article", 0.92),
            ("def456", "https://example.com/2", "Another Article", 0.85),
        ]
        mock_conn.cursor.return_value.__enter__ = lambda self: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        from src.storage.vector_store import VectorStore
        store = VectorStore()

        fake_embedding = [0.1] * 384
        results = store.search(fake_embedding, limit=5)

        assert len(results) == 2
        assert results[0]["content_id"] == "abc123"
        assert results[0]["similarity"] == 0.92
        assert results[1]["title"] == "Another Article"

    @patch("src.storage.vector_store._get_conn")
    def test_search_empty_results(self, mock_get_conn):
        """search() with no matches should return an empty list."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("content_id",), ("url",), ("title",), ("similarity",)
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = lambda self: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        from src.storage.vector_store import VectorStore
        store = VectorStore()

        results = store.search([0.0] * 384, limit=10)
        assert results == []


class TestSemanticSearchServiceUnit:
    """Unit tests for SemanticSearchService (mocked dependencies)."""

    @patch("src.news_topic_model.services.semantic_search_service.VectorStore")
    @patch("src.news_topic_model.services.semantic_search_service.SentenceTransformer")
    def test_empty_query_returns_empty(self, mock_model_cls, mock_store_cls):
        mock_store = MagicMock()
        mock_store.count.return_value = 100
        mock_store_cls.return_value = mock_store

        from src.news_topic_model.services.semantic_search_service import SemanticSearchService
        service = SemanticSearchService(db_path="/tmp/fake.db", lazy_model=True)

        results = service.search("", limit=10)
        assert results == []

    @patch("src.news_topic_model.services.semantic_search_service.VectorStore")
    @patch("src.news_topic_model.services.semantic_search_service.SentenceTransformer")
    def test_search_calls_vector_store(self, mock_model_cls, mock_store_cls):
        """search() should encode the query and call VectorStore.search()."""
        import numpy as np

        mock_store = MagicMock()
        mock_store.count.return_value = 50
        mock_store.search.return_value = [
            {"content_id": "abc", "url": "https://x.com", "title": "Title", "similarity": 0.9}
        ]
        mock_store_cls.return_value = mock_store

        mock_model = MagicMock()
        mock_model.encode.return_value = np.zeros(384)
        mock_model_cls.return_value = mock_model

        from src.news_topic_model.services.semantic_search_service import SemanticSearchService
        service = SemanticSearchService(db_path="/tmp/fake.db", lazy_model=False)

        results = service.search("transfer rumours", limit=5)

        mock_model.encode.assert_called_once()
        mock_store.search.assert_called_once()
        assert len(results) == 1
        assert results[0]["similarity"] == 0.9


# ---------------------------------------------------------------------------
# Integration test (requires running pgvector via Docker Compose)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.getenv("RUN_INTEGRATION_TESTS"),
    reason="Set RUN_INTEGRATION_TESTS=1 to run integration tests",
)
class TestVectorStoreIntegration:
    """Integration tests against a real pgvector database."""

    def test_upsert_and_search_roundtrip(self):
        from sentence_transformers import SentenceTransformer
        from src.storage.vector_store import VectorStore

        store = VectorStore()
        store.create_table()

        model = SentenceTransformer("all-MiniLM-L6-v2")
        texts = [
            "Arsenal sign new midfielder in January window",
            "Liverpool beat Manchester City 3-1",
            "Serie A transfer deadline day recap",
        ]
        embeddings = model.encode(texts, normalize_embeddings=True)

        records = [
            {
                "content_id": f"test_{i}",
                "url": f"https://test.com/{i}",
                "title": text,
                "embedding": embedding.tolist(),
            }
            for i, (text, embedding) in enumerate(zip(texts, embeddings))
        ]
        store.upsert_batch(records)

        query_emb = model.encode("Arsenal transfer news", normalize_embeddings=True).tolist()
        results = store.search(query_emb, limit=3)

        assert len(results) == 3
        # The Arsenal article should rank highest
        assert results[0]["content_id"] == "test_0"
        assert results[0]["similarity"] > 0.5

        store.close()
