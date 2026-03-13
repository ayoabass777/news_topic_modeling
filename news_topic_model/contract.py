"""
Contracts for inter-stage messages
=================================

This module defines the *stable wire contracts* shared across pipeline stages,
so business logic (producer/fetcher/persistor) does not depend on the transport
(`asyncio.Queue` vs Kafka). The contracts are versioned to allow safe evolution.

Usage
-----
from news_topic_model.contract import UrlDiscovered, ArticleFetched

msg = UrlDiscovered(url="https://example.com/a", discovered_at=utcnow())
json_bytes = msg.to_json_bytes()
restored = UrlDiscovered.from_json_bytes(json_bytes)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Literal
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

__all__ = [
    "utcnow",
    "UrlDiscovered",
    "ArticleFetched",
]


# -------------------------
# Time helpers
# -------------------------

def utcnow() -> datetime:
    """Return a timezone-aware UTC datetime (always with tzinfo=UTC)."""
    return datetime.now(timezone.utc)


# -------------------------
# Base contract
# -------------------------

class Contract(BaseModel):
    """Base for all contracts with common utilities.

    - Provides JSON encode/decode helpers compatible with Kafka and files.
    - Sets a default `schema_version` for evolution.
    """

    schema_version: Literal["v1"] = Field(
        default="v1",
        description="Schema version for compatibility across transport and storage.",
    )

    model_config = dict(
        # Render datetimes as ISO-8601 with timezone
        ser_json_timedelta="iso8601",
        ser_json_bytes="utf8",
        # Pydantic v2 will include timezone info for aware datetimes
    )

    # --- Convenience ---
    def to_json_bytes(self) -> bytes:
        """Serialize the model to UTF-8 JSON bytes (transport-friendly)."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, b: bytes) -> "Contract":
        """Deserialize from UTF-8 JSON bytes to a model instance."""
        return cls.model_validate_json(b)


# -------------------------
# UrlDiscovered
# -------------------------

class UrlDiscovered(Contract):
    """A URL that has been discovered and is ready to be fetched.

    Attributes
    ----------
    url : str
        The discovered URL (already normalized by the producer).
    discovered_at : datetime
        When the URL was discovered (UTC).
    query : Optional[str]
        Optional free-form query that produced this URL (e.g., GDELT query).
    provider : Optional[str]
        Name of the upstream provider (e.g., "gdelt").
    meta : dict
        Free-form metadata for debugging and enrichment.
        Typical contents include discovery/debug context such as the raw unnormalized URL, crawl depth, search rank, provider-specific identifiers, or trace of discovery steps.
    """

    url: str = Field(..., description="Normalized canonical URL to fetch.")
    discovered_at: datetime = Field(default_factory=utcnow, description="UTC timestamp when discovered.")
    query: Optional[str] = Field(default=None, description="Upstream query string that yielded this URL.")
    provider: Optional[str] = Field(default=None, description="Source provider name (e.g., 'gdelt').")
    meta: Dict[str, Any] = Field(default_factory=dict, description="Free-form metadata.")

    @field_validator("url")
    @classmethod
    def _strip_url(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("url must be a non-empty string")
        return v

    # A natural key for idempotence (used by storages or dedupe maps)
    def natural_key(self) -> Tuple[str]:
        return (self.url,)


# -------------------------
# ArticleFetched
# -------------------------

class ArticleFetched(Contract):
    """A fetched and extracted article ready for persistence.

    Attributes
    ----------
    url : str
        The canonical URL for the article.
    html_sha1 : Optional[str]
        SHA1 of the fetched HTML body (hex). Useful for idempotence.
    title : Optional[str]
        Extracted title (if available).
    text : Optional[str]
        Main extracted text content.
    source_domain : Optional[str]
        Effective source domain/host (normalized).
    published_at : Optional[datetime]
        Published datetime if parsable; otherwise None.
    fetched_at : datetime
        When the fetch completed (UTC).
    lang : Optional[str]
        ISO 639-1 code if detected; defaults to 'en' in our pipeline.
    meta : dict
        Free-form metadata including extraction/runtime hints.
        Typical contents include runtime/debug context such as HTTP status, fetch timings, parser used, extraction confidence, redirect chain, content type, lightweight text stats, or retry/proxy info.
    """

    url: str = Field(..., description="Canonical URL for the article")
    html_sha1: Optional[str] = Field(default=None, description="SHA1 of raw HTML (hex)")
    title: Optional[str] = Field(default=None, description="Extracted title")
    text: Optional[str] = Field(default=None, description="Extracted plain text")
    source_domain: Optional[str] = Field(default=None, description="Normalized source domain/host")
    published_at: Optional[datetime] = Field(default=None, description="Published datetime in UTC if known")
    fetched_at: datetime = Field(default_factory=utcnow, description="Fetch completion time in UTC")
    lang: Optional[str] = Field(default=None, description="Detected language code (e.g., 'en')")
    meta: Dict[str, Any] = Field(default_factory=dict, description="Free-form metadata")

    @field_validator("url")
    @classmethod
    def _strip_url(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("url must be a non-empty string")
        return v

    def natural_key(self) -> Tuple[str, Optional[str]]:
        """Return a deterministic key for idempotent persistence.

        Using (url, html_sha1) differentiates multiple crawls of the same URL
        when content changes. If `html_sha1` is missing, the key falls back to
        (url, None), allowing storages to decide their upsert behavior.
        """
        return (self.url, self.html_sha1)
