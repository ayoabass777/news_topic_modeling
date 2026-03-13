

from __future__ import annotations

import hashlib
import re
from typing import Optional

try:
    import trafilatura  # type: ignore
    _HAVE_TRAFILATURA = True
except Exception:
    _HAVE_TRAFILATURA = False

__all__ = [
    "TEXT_MIN_CHARS",
    "normalize_text",
    "sha1_hex",
    "extract_text",
]

# Minimum length for extracted article text to be considered valid
TEXT_MIN_CHARS = 400

_ws_re = re.compile(r"\s+")


def normalize_text(s: str) -> str:
    """Normalize text for hashing/comparison.

    - Collapse all whitespace to single spaces
    - Strip leading/trailing whitespace
    - Lowercase (case-insensitive dedupe)
    """
    if s is None:
        return ""
    return _ws_re.sub(" ", s).strip().lower()


def sha1_hex(s: str) -> str:
    """Return hex-encoded SHA1 of the input string (UTF-8)."""
    h = hashlib.sha1()
    h.update(s.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _extract_with_trafilatura(html: str, url: Optional[str] = None) -> Optional[str]:
    """Best-effort article extraction using trafilatura.

    Returns plain text or None if extraction fails or is too short.
    """
    if not _HAVE_TRAFILATURA:
        raise RuntimeError(
            "trafilatura is not installed and is required for text extraction. "
            "Please install trafilatura to use this feature."
        )

    # Configure trafilatura for precision over recall
    try:
        extracted = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=False,
            favor_recall=False,
            no_fallback=True,
            output_format="txt",
        )
    except Exception:
        return None

    if not extracted:
        return None

    # Light cleanup: collapse whitespace
    text = _ws_re.sub(" ", extracted).strip()
    if len(text) < TEXT_MIN_CHARS:
        return None
    return text


def extract_text(html: str, url: Optional[str] = None) -> Optional[str]:
    """Extract article text from raw HTML.

    Currently uses trafilatura when available. Returns None if no adequate
    article body is found or if the text is below TEXT_MIN_CHARS.
    """
    if not html:
        return None

    # Try trafilatura first
    text = _extract_with_trafilatura(html, url)
    if text:
        return text

    # Future: add other extractors (readability-lxml, Goose, etc.)
    return None