from __future__ import annotations

import asyncio
import datetime as dt
from typing import Optional, Set, Tuple

import httpx

from news_topic_model.adapters.gdelt import (
    DiscoveredItem,
    discover_gdelt,
    discover_gdelt_paged,
)
from news_topic_model.config import UA
from news_topic_model.utils.dtparse import to_gdelt_utc, parse_window_to_timedelta
from news_topic_model.utils.urlnorm import normalize_url


# ---- Logging setup (self-contained, base_out-aware) ----
import logging
from pathlib import Path

_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")


def _get_producer_logger(base_out: str) -> logging.Logger:
    """Return a producer logger that always writes to `<base_out>/logs/producer.log`.
    This retargets file handlers each call so runs from different working dirs
    still land in the correct log folder.
    """
    logger = logging.getLogger("news_topic_model.producer")  # unique, no collision
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    logs_dir = Path(base_out) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "producer.log"

    # Remove any existing FileHandlers (e.g., created in a previous run/dir)
    to_remove = []
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            to_remove.append(h)
    for h in to_remove:
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_formatter)
    logger.addHandler(fh)

    return logger


def _compute_bounds(
    *,
    start_arg: Optional[str],
    end_arg: Optional[str],
    window_arg: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Return (start_utc14, end_utc14) based on strict args.

    Rules:
      - If both --start and --end are provided: use them (validate ordering)
      - If only --start is provided: end = start + window
      - If only --end is provided: start = end - window
      - If neither is provided: return (None, None) and let the adapter use timespan
    """
    start_utc = to_gdelt_utc(start_arg) if start_arg else None
    end_utc = to_gdelt_utc(end_arg) if end_arg else None

    if (start_utc and not end_utc) or (end_utc and not start_utc):
        delta = parse_window_to_timedelta(window_arg)
        if start_utc and not end_utc:
            start_dt = dt.datetime.strptime(start_utc, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
            end_dt = start_dt + delta
            end_utc = end_dt.strftime("%Y%m%d%H%M%S")
        elif end_utc and not start_utc:
            end_dt = dt.datetime.strptime(end_utc, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
            start_dt = end_dt - delta
            start_utc = start_dt.strftime("%Y%m%d%H%M%S")

    if start_utc and end_utc and start_utc > end_utc:
        raise ValueError(f"--start must be <= --end (got {start_arg} > {end_arg})")

    return start_utc, end_utc


async def producer(args, url_queue: asyncio.Queue[Optional[tuple[str, DiscoveredItem]]]) -> None:
    """Discover candidate URLs and enqueue them for fetching.

    Currently supports the GDELT Doc 2.0 adapter. Respects strict --start/--end
    with single-bound fill using --window. If neither bound is given, falls back
    to timespan (window) in the adapter.
    """
    logger = _get_producer_logger(getattr(args, "base_out", "data"))
    logger.info("producer: log file -> %s/logs/producer.log", getattr(args, "base_out", "data"))
    logger.info("producer: starting discovery for query='%s'", getattr(args, "query", ""))
    # Short-circuit if provider list excludes gdelt
    providers = set(getattr(args, "providers", []) or [])
    if providers and "gdelt" not in providers:
        logger.info("producer: no supported providers selected; nothing to do")
        return

    start_utc, end_utc = _compute_bounds(
        start_arg=getattr(args, "start", None),
        end_arg=getattr(args, "end", None),
        window_arg=getattr(args, "window", "1d"))

    logger.debug("producer: computed bounds start=%s end=%s window=%s", start_utc, end_utc, getattr(args, "window", "1d"))

    total_limit = max(1, int(getattr(args, "limit", 100)))
    window = getattr(args, "window", "1d")
    logger.debug("producer: total_limit=%d window=%s", total_limit, window)

    discovered = 0
    unique = 0
    enqueued = 0
    seen_urls: Set[str] = set()

    req_timeout = httpx.Timeout(connect=10.0, read=45.0, write=20.0, pool=45.0)
    headers = {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.8",
        # Intentionally omit Accept-Encoding to avoid compressed edge cases
    }
    async with httpx.AsyncClient(timeout=req_timeout, headers=headers, follow_redirects=True, http2=False) as client:
        if start_utc and end_utc:
            # Use paged discovery to traverse the explicit window backward
            batch_size = min(100, total_limit)
            logger.info("producer: using paged discovery for explicit bounds [%s, %s] (batch_size=%d, max_total=%d)", start_utc, end_utc, batch_size, total_limit)
            async for batch in discover_gdelt_paged(
                query=args.query,
                start_utc=start_utc,
                end_utc=end_utc,
                client=client,
                batch_size=batch_size,
                max_total=total_limit,
            ):
                discovered += len(batch)
                for item in batch:
                    logger.debug("producer: considering item url=%s", getattr(item, "url", None))
                    normalized_url = normalize_url(item.url)
                    logger.debug("producer: normalized_url=%s", normalized_url)
                    if not normalized_url:
                        continue
                    if normalized_url in seen_urls:
                        logger.debug("producer: duplicate url skipped %s", normalized_url)
                        continue
                    seen_urls.add(normalized_url)
                    unique += 1
                    await url_queue.put((normalized_url, item))
                    enqueued += 1
        else:
            # Single call using timespan=window; API caps to 100 per request
            logger.info("producer: using single-call discovery with timespan=%s (cap=100, limit=%d)", window, total_limit)
            items = await discover_gdelt(
                query=args.query,
                timespan=window,
                maxrecords=min(100, total_limit),
                client=client,
                start=None,
                end=None,
            )
            discovered += len(items)
            for item in items:
                logger.debug("producer: considering item url=%s", getattr(item, "url", None))
                normalized_url = normalize_url(item.url)
                logger.debug("producer: normalized_url=%s", normalized_url)
                if not normalized_url:
                    continue
                if normalized_url in seen_urls:
                    logger.debug("producer: duplicate url skipped %s", normalized_url)
                    continue
                seen_urls.add(normalized_url)
                unique += 1
                await url_queue.put((normalized_url, item))
                enqueued += 1

    logger.info("producer: discovered=%d unique=%d queued=%d", discovered, unique, enqueued)
    if discovered == 0:
        logger.warning("producer: discovered=0; provider may be slow/blocked or query too strict (%r)", getattr(args, "query", ""))