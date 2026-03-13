

from __future__ import annotations

import re
import datetime as dt
from typing import Optional

__all__ = [
    "to_gdelt_utc",
    "parse_window_to_timedelta",
]

# ---------------------------------------------------------------------------
# Strict datetime parsing (local → UTC → GDELT format)
# Allowed inputs (naive = interpreted as LOCAL time, then converted to UTC):
#   1) YYYY-MM-DD
#   2) YYYY-MM-DD HH:MM
#   3) YYYY-MM-DD HH:MM:SS
# No relative forms, no timezone suffixes, no 'T' separator.
# ---------------------------------------------------------------------------
_STRICT_DT_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$"
)


def to_gdelt_utc(s: str) -> str:
    """Parse a strict human datetime and return UTC in GDELT format (YYYYMMDDHHMMSS).

    Input is interpreted as **local time** if no timezone is present (only strict
    formats are accepted). It is then converted to UTC and formatted for GDELT.

    Accepted inputs:
      - 'YYYY-MM-DD'
      - 'YYYY-MM-DD HH:MM'
      - 'YYYY-MM-DD HH:MM:SS'

    Raises:
      ValueError: if the string does not match the accepted formats.
    """
    s = (s or "").strip()
    m = _STRICT_DT_RE.fullmatch(s)
    if not m:
        raise ValueError(
            "Invalid datetime. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]' (local time)."
        )

    year, month, day, hh, mm, ss = m.groups()
    y = int(year)
    mo = int(month)
    d = int(day)
    H = int(hh) if hh is not None else 0
    M = int(mm) if mm is not None else 0
    S = int(ss) if ss is not None else 0

    # Build a LOCAL-TIME datetime then convert to UTC
    local_tz = dt.datetime.now().astimezone().tzinfo
    t_local = dt.datetime(y, mo, d, H, M, S, tzinfo=local_tz)
    t_utc = t_local.astimezone(dt.timezone.utc)
    return t_utc.strftime("%Y%m%d%H%M%S")


# ---------------------------------------------------------------------------
# Window parsing (e.g., '30min', '1h', '1d', '2w') → timedelta
# ---------------------------------------------------------------------------
_WINDOW_RE = re.compile(r"^(\d+)\s*(min|m|h|d|w)$", re.IGNORECASE)


def parse_window_to_timedelta(window: str) -> dt.timedelta:
    """Convert a short human window string to a timedelta.

    Accepted examples: '30min', '15m', '6h', '1d', '2w'.

    Raises:
      ValueError: if the string does not match the accepted pattern.
    """
    s = (window or "").strip()
    m = _WINDOW_RE.fullmatch(s)
    if not m:
        raise ValueError("--window must be like '30min', '15m', '6h', '1d', or '2w'")

    amount = int(m.group(1))
    unit = m.group(2).lower()

    if unit in ("min", "m"):
        return dt.timedelta(minutes=amount)
    if unit == "h":
        return dt.timedelta(hours=amount)
    if unit == "d":
        return dt.timedelta(days=amount)
    if unit == "w":
        return dt.timedelta(weeks=amount)

    # Should never reach here due to regex, but keep a guard.
    raise ValueError(f"Unsupported window unit: {unit}")