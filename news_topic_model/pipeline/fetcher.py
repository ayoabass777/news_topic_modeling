"""Fetcher
=======

Consumes canonical URLs from `url_queue`, downloads pages using **browser-like headers**
(from `config.Settings.default_headers`), extracts readable text, and pushes normalized
results to `html_queue` for persistence. The fetcher also consults and updates the
per-domain `domain_policy` (via `SeenStore.domain_policy()`) so we avoid repeatedly
hitting domains that block direct HTML access.

Domain rules (summary):
- HTTP 403/406 → treat as hard block: record outcome and switch mode to
  `alternate:rss` (if RSS is known) else `alternate:reader`.
- HTTP 429 → treat as throttle: parse `Retry-After` when present, store cooldown,
  set mode to `throttle`, and retry with capped backoff.
- 408/5xx/network errors → transient: limited retries with backoff + jitter.

Queues:
- `url_queue` items: `(canonical_url, discovered_item)`; sentinel: `None`.
- `html_queue` items: `(canonical_url, discovered_item, text, content_sha1)`; sentinel: `None`.
- `policy_queue` items: `PolicyEvent` instances for asynchronous policy updates.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import random
import time
from typing import Dict, Optional, Tuple
from pathlib import Path
from collections import Counter

import httpx
import ssl

from news_topic_model.adapters.gdelt import DiscoveredItem
from news_topic_model.config import (
    UA,
    RETRY_STATUS,
    MAX_RETRIES,
    BACKOFF_INITIAL,
    BACKOFF_FACTOR,
    JITTER_MAX,
    Settings,
)  # Ensure MAX_RETRIES=3 for three attempts before skipping
from news_topic_model.storage.seen_store import SeenStore
from news_topic_model.utils.lang import is_probably_english
from news_topic_model.utils.text import extract_text, normalize_text, sha1_hex
from news_topic_model.utils.urlnorm import get_domain

from news_topic_model.contract.policy_event import (
    PolicyEvent,
    PolicyEventType,
    RecordOutcome,
    SetMode,
    SetRetryAfter,
    UpdateRss,
)

from news_topic_model.config import fetcher_logger as logger, rejections_logger as reject_logger

# Running counts of rejection reasons (process-local)
REJECT_COUNTS: Counter = Counter()

def _log_reject_summary(prefix: str = "fetcher: reject summary") -> None:
    try:
        if not REJECT_COUNTS:
            logger.info("%s: no rejections recorded", prefix)
            return
        parts = [f"{k}={v}" for k, v in REJECT_COUNTS.most_common()]
        logger.info("%s: %s", prefix, ", ".join(parts))
    except Exception:
        pass

def _log_reject(*, reason: str, url: str, domain: str, status: Optional[int] = None, extra: Optional[dict] = None) -> None:
    try:
        REJECT_COUNTS[reason] += 1
        count = REJECT_COUNTS[reason]
        reject_logger.info(
            "reason=%s count=%d url=%s domain=%s status=%s extra=%s",
            reason, count, url, domain, status, extra or {}
        )
    except Exception:
        # never fail the pipeline on logging issues
        pass

# Default browser-like headers from config; fallback if Settings fails
try:
    _DEFAULT_HEADERS = dict(Settings().default_headers)
except Exception:
    _DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
_DEFAULT_HEADERS.pop("Accept-Encoding", None)

# If UA is set in config, prefer it in headers
if UA and isinstance(_DEFAULT_HEADERS, dict):
    _DEFAULT_HEADERS["User-Agent"] = UA


async def fetch_html(url: str, client: httpx.AsyncClient, headers: Optional[dict] = None) -> Tuple[Optional[int], Dict[str, str], Optional[str]]:
    """Fetch a URL with retry/backoff/jitter.

    Returns a tuple: `(status_code, response_headers, text)` where `text` is None
    on failure or when the content-type is not HTML/XML. `status_code` may be None
    for network-level failures where no response was received.
    """
    attempt = 0
    backoff = BACKOFF_INITIAL
    hdrs = headers or _DEFAULT_HEADERS
    logger.debug("fetch_html: start url=%s", url)

    while True:
        try:
            resp = await client.get(url, headers=hdrs)
            status = resp.status_code
            if status == 429:
                # Do not retry here; let caller enforce throttle policy immediately
                return status, dict(resp.headers), None
            # Retry on configured retryable status codes
            if status in RETRY_STATUS:
                raise httpx.HTTPStatusError("retryable status", request=resp.request, response=resp)
            # Raise for other 4xx/5xx to let caller inspect status
            resp.raise_for_status()
            logger.debug("fetch_html: got %s for %s", status, url)
            ctype = resp.headers.get("content-type", "").lower()
            if "html" not in ctype and "xml" not in ctype and not resp.text.lstrip().startswith("<"):
                logger.debug("fetch_html: non-HTML content-type '%s' for %s; skipping", ctype, url)
                return status, dict(resp.headers), None
            return status, dict(resp.headers), resp.text
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError, httpx.ConnectError, ssl.SSLError) as e:
            # network/SSL transient – retry up to MAX_RETRIES, then skip
            if attempt >= MAX_RETRIES:
                logger.error("fetch_html: giving up after %s attempts on %s (net/ssl)", attempt + 1, url)
                return None, {}, None
            logger.warning(
                "fetch_html: transient net/ssl error on %s (attempt %s/%s): %s",
                url, attempt + 1, MAX_RETRIES, e,
            )
            await asyncio.sleep(backoff + random.random() * JITTER_MAX)
            backoff *= BACKOFF_FACTOR
            attempt += 1
        except httpx.HTTPStatusError as e:
            resp = e.response
            status = resp.status_code if resp is not None else None
            # If status is configured as retryable, backoff; else return immediately
            if status in RETRY_STATUS and attempt < MAX_RETRIES:
                logger.warning(
                    "fetch_html: retryable HTTP %s on %s (attempt %s/%s)",
                    status, url, attempt + 1, MAX_RETRIES,
                )
                await asyncio.sleep(backoff + random.random() * JITTER_MAX)
                backoff *= BACKOFF_FACTOR
                attempt += 1
                continue
            # Non-retryable or retries exhausted: surface status/headers
            headers_map = dict(resp.headers) if resp is not None else {}
            return status, headers_map, None
        except Exception as e:
            logger.exception("fetch_html: unexpected error on %s: %s", url, e)
            return None, {}, None


async def fetcher(
    args,
    url_queue: asyncio.Queue[Optional[Tuple[str, DiscoveredItem]]],
    html_queue: asyncio.Queue[Optional[Tuple[str, DiscoveredItem, str, str]]],
    policy_queue: asyncio.Queue,
) -> None:
    """Consume normalized URLs, fetch/extract content, and enqueue for persistence.

    Skips URLs already present in url_seen; applies hybrid English-only filtering;
    dedupes by content_sha1 before enqueueing.

    Args:
        args: command-line or runtime arguments.
        url_queue: queue of (canonical_url, DiscoveredItem) tuples to fetch.
        html_queue: queue to enqueue (canonical_url, DiscoveredItem, text, content_sha1).
        policy_queue: queue to enqueue PolicyEvent instances for asynchronous policy updates.
    """
    try:
        base_out = args.out
        seen = None
        logger.info("fetcher: starting with base_out=%s", base_out)
        domain_sems: Dict[str, asyncio.Semaphore] = {}
        processed = 0

        indexes_dir = Path(base_out) / "indexes"
        try:
            indexes_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.exception("fetcher: failed to create indexes dir at %s: %s", indexes_dir, e)
            raise
        seen = SeenStore(str(indexes_dir / "seen.sqlite"))
        policy = seen.domain_policy()

        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                while True:
                    msg = await url_queue.get()
                    try:
                        # Sentinel: just exit (orchestrator will forward downstream)
                        if msg is None:
                            logger.info("fetcher: received sentinel; exiting")
                            return

                        normalized_url, item = msg
                        logger.debug("fetcher: dequeued %s", normalized_url)

                        # Fast skip if URL already recorded
                        if seen.has_url(normalized_url):
                            host = item.source_domain or get_domain(normalized_url) or "unknown"
                            _log_reject(reason="already_seen_url", url=normalized_url, domain=host)
                            logger.debug("fetcher: URL already seen %s", normalized_url)
                            continue

                        # Per-domain throttling
                        host = item.source_domain or get_domain(normalized_url) or "unknown"
                        mode = policy.choose_mode(host)
                        if mode == "throttle":
                            rec = policy.get(host)
                            ra = rec.get("retry_after_until_ts") if rec else None
                            if ra:
                                try:
                                    remain = (dt.datetime.fromisoformat(ra) - dt.datetime.now(dt.timezone.utc)).total_seconds()
                                    if remain > 0:
                                        await asyncio.sleep(min(remain, 30.0))  # cap sleep to avoid long stalls
                                    else:
                                        await asyncio.sleep(min(BACKOFF_INITIAL, 1.0))
                                except Exception:
                                    await asyncio.sleep(min(BACKOFF_INITIAL, 1.0))
                            else:
                                await asyncio.sleep(min(BACKOFF_INITIAL, 1.0))

                        sem = domain_sems.setdefault(host, asyncio.Semaphore(max(1, getattr(args, "domain_concurrency", 2))))
                        logger.debug("fetcher: domain=%s acquiring semaphore", host)

                        async with sem:
                            status_code, resp_headers, html = await fetch_html(normalized_url, client, headers=_DEFAULT_HEADERS)
                        logger.debug("fetcher: fetched html=%s for %s", bool(html), normalized_url)

                        # If we repeatedly failed to connect (no response, no html), switch this domain to reader mode
                        if status_code is None and html is None:
                            try:
                                await PolicyEvent.enqueue(policy_queue, SetMode(domain=host, mode="alternate:reader", reason="net_ssl_error"))
                                logger.warning("fetcher: %s marked alternate:reader due to net/ssl errors", host)
                                
                            except Exception:
                                logger.exception("fetcher: failed to enqueue SetMode(net/ssl) for %s", host)

                        # Record outcome to policy via event queue
                        try:
                            await PolicyEvent.enqueue(
                                policy_queue,
                                RecordOutcome(domain=host, status=(status_code if status_code is not None else -1), extract_ok=bool(html))
                            )
                        except Exception:
                            logger.exception("fetcher: failed to enqueue RecordOutcome for %s", host)

                        # Hard blocks: 403/406 → switch to alternate path
                        if status_code in {403, 406}:
                            rec = policy.get(host)
                            next_mode = "alternate:rss" if rec.get("rss_url") else "alternate:reader"
                            try:
                                await PolicyEvent.enqueue(policy_queue, SetMode(domain=host, mode=next_mode, reason=f"http_{status_code}"))
                            except Exception:
                                logger.exception("fetcher: failed to enqueue SetMode for %s", host)
                            _log_reject(reason="hard_block", url=normalized_url, domain=host, status=status_code)
                            logger.warning("fetcher: %s hard-blocked (%s); switching to %s", host, status_code, next_mode)
                            continue

                        # Throttle: 429 → honor Retry-After if present
                        if status_code == 429:
                            ra = resp_headers.get("Retry-After") if resp_headers else None
                            # Compute until_iso from header seconds if available; otherwise fallback to BACKOFF_INITIAL seconds from now
                            try:
                                if ra is not None:
                                    secs = int(ra)
                                    until_ts = time.time() + max(0, secs)
                                else:
                                    until_ts = time.time() + max(1.0, float(BACKOFF_INITIAL))
                                until_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(until_ts))
                            except Exception:
                                until_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + max(1.0, float(BACKOFF_INITIAL))))
                            # enqueue policy updates
                            try:
                                await PolicyEvent.enqueue(policy_queue, SetRetryAfter(domain=host, until_iso=until_iso))
                                await PolicyEvent.enqueue(policy_queue, SetMode(domain=host, mode="throttle", reason="http_429"))
                            except Exception:
                                logger.exception("fetcher: failed to enqueue throttle policy for %s", host)
                            # proceed with retry/backoff behavior already handled in fetch_html
                            if not html:
                                _log_reject(reason="throttle_429", url=normalized_url, domain=host, status=status_code, extra={"retry_after": ra})
                                continue

                        if not html:
                            # Distinguish non-HTML vs generic failure using headers
                            ctype = (resp_headers or {}).get("content-type", "").lower() if resp_headers else ""
                            reason = "non_html" if ("html" not in ctype and "xml" not in ctype) else "fetch_failed"
                            _log_reject(reason=reason, url=normalized_url, domain=host, status=status_code, extra={"ctype": ctype})
                            continue

                        text = extract_text(html, normalized_url)
                        logger.debug("fetcher: extracted text=%s for %s", bool(text), normalized_url)
                        if not text:
                            _log_reject(reason="extract_failed", url=normalized_url, domain=host)
                            continue

                        # Hybrid English-only filtering
                        if not is_probably_english(text, item.title or ""):
                            _log_reject(reason="lang_non_en", url=normalized_url, domain=host)
                            logger.info("fetcher: non-EN detected; skipping %s", normalized_url)
                            continue

                        # Content-level dedupe
                        content_hash = sha1_hex(normalize_text(text))
                        if seen.has_sha(content_hash):
                            _log_reject(reason="dup_content", url=normalized_url, domain=host)
                            logger.debug("fetcher: content already seen %s", normalized_url)
                            continue

                        logger.debug("fetcher: enqueue for persist %s", normalized_url)
                        await html_queue.put((normalized_url, item, text, content_hash))
                        processed += 1
                        if processed % 10 == 0:
                            logger.info("fetcher: processed %d items so far", processed)
                    finally:
                        url_queue.task_done()
        finally:
            _log_reject_summary()
            logger.info("fetcher: shutting down; closing SeenStore")
            # Always close DB connection for this fetcher
            if seen is not None:
                seen.close()
    except Exception:
        logger.exception("fetcher: unhandled exception; aborting")
        raise