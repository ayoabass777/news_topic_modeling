from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, AsyncIterator
from news_topic_model.config import gdelt_adapter_logger  as logger

import httpx
import asyncio
import random
import logging



from news_topic_model.config import UA


@dataclass(slots=True)
class DiscoveredItem:
    """Lightweight carrier for discovery results from GDELT Doc 2.0.

    Fields mirror what the pipeline needs downstream.
    - title: Article title (may be empty)
    - url: Original URL
    - published_at: ISO 8601 (UTC) mapped from GDELT's seendate if present; else None
    - source_domain: GDELT domain (e.g., example.com)
    - source_name: Source common name when present
    - discovered_via: string tag describing adapter ("gdelt:doc2")
    """

    title: str
    url: str
    published_at: Optional[str]
    source_domain: str
    source_name: str
    discovered_via: str = "gdelt:doc2"


def _gdelt14_to_iso(yyyymmddhhmmss: str) -> Optional[str]:
    """Convert GDELT's 14-digit UTC string to ISO-8601 Z. Return None on bad input."""
    try:
        t = dt.datetime.strptime(yyyymmddhhmmss, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
        return t.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _minus_one_second(utc14: str) -> str:
    t = dt.datetime.strptime(utc14, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
    return (t - dt.timedelta(seconds=1)).strftime("%Y%m%d%H%M%S")


async def discover_gdelt(
    query: str,
    timespan: str = "1d",
    maxrecords: int = 100,
    client: Optional[httpx.AsyncClient] = None,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[DiscoveredItem]:
    """Call GDELT Doc 2.0 and return up to `maxrecords` DiscoveredItem objects.

    If `start` and `end` are both provided (UTC in YYYYMMDDHHMMSS), they override `timespan`.
    `maxrecords` is capped to 100 by the API and by this function.
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    size = max(1, min(100, int(maxrecords or 100)))

    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        # Restrict to English-only sources (GDELT expects uppercase)
        "sourcelang": "ENGLISH",
        "maxrecords": str(size),
    }
    if start and end:
        params["STARTDATETIME"] = start
        params["ENDDATETIME"] = end
    else:
        params["timespan"] = timespan

    logger.info(
        "gdelt: request query=%r size=%d bounds=%s..%s",
        query,
        size,
        params.get("STARTDATETIME"),
        params.get("ENDDATETIME"),
    )
    logger.info("gdelt: full params: %s", params)
    
    own_client = client is None
    # More forgiving, explicit timeouts
    req_timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=30.0)

    if own_client:
        client = httpx.AsyncClient(timeout=req_timeout, headers={"User-Agent": UA}, follow_redirects=True)
    else:
        pass

    items: List[DiscoveredItem] = []
    try:
        # Lightweight retry on transient failures
        max_retries = 3
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                r = await client.get(
                    url, params=params, timeout=req_timeout, headers={"User-Agent": UA}
                )
                r.raise_for_status()
                break  # success
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                if attempt == max_retries:
                    logger.warning(
                        "gdelt: timeout after %d attempts; params=%s; error=%s",
                        attempt, params, repr(e)
                    )
                    return items
                sleep_s = backoff * (0.9 + 0.2 * random.random())
                logger.info(
                    "gdelt: timeout on attempt %d/%d; backing off %.2fs",
                    attempt, max_retries, sleep_s
                )
                await asyncio.sleep(sleep_s)
                backoff *= 2
                continue
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code in {502, 503, 504}:
                    if attempt == max_retries:
                        logger.warning(
                            "gdelt: HTTP %s after %d attempts; params=%s",
                            code, attempt, params
                        )
                        return items
                    sleep_s = backoff * (0.9 + 0.2 * random.random())
                    logger.info(
                        "gdelt: HTTP %s attempt %d/%d; backing off %.2fs",
                        code, attempt, max_retries, sleep_s
                    )
                    await asyncio.sleep(sleep_s)
                    backoff *= 2
                    continue
                if code == 429:
                    ra = e.response.headers.get("Retry-After")
                    try:
                        sleep_s = float(ra) if ra is not None else backoff
                    except ValueError:
                        sleep_s = backoff
                    if attempt == max_retries:
                        logger.warning("gdelt: 429 after %d attempts; params=%s", attempt, params)
                        return items
                    logger.info("gdelt: 429; sleeping %.2fs then retry", sleep_s)
                    await asyncio.sleep(sleep_s)
                    backoff *= 2
                    continue
                # Non-retryable status
                logger.warning("gdelt: HTTP %s; params=%s", code, params)
                return items
        try:
            data = r.json()
            logger.info("data stored")
        except Exception:
            snippet = r.text[:200] if hasattr(r, "text") else ""
            level = logging.WARNING if ("Timespan is too short" in snippet) else logging.ERROR
            logger.info(
                "gdelt: JSON parse failed. status=%s body[:200]=%r",
                getattr(r, "status_code", "?"), snippet,
            )
            return items

        arts = data.get("articles") or []
        for a in arts:
            # Known GDELT fields in ArtList mode (robust to missing keys)
            href = a.get("url") or ""
            if not href:
                continue
            title = a.get("title") or ""
            domain = a.get("domain") or ""
            source_name = a.get("sourceCommonName") or a.get("sourcecountry") or ""
            # seendate is usually 14-digit UTC string; fall back to None
            iso_seen: Optional[str] = None
            sd = a.get("seendate")
            if isinstance(sd, str):
                iso_seen = _gdelt14_to_iso(sd)
            elif isinstance(sd, int):
                iso_seen = _gdelt14_to_iso(str(sd))

            items.append(
                DiscoveredItem(
                    title=title,
                    url=href,
                    published_at=iso_seen,
                    source_domain=domain,
                    source_name=source_name,
                )
            )
        return items
    finally:
        if own_client and client is not None:
            await client.aclose()


async def discover_gdelt_paged(
    query: str,
    start_utc: str,
    end_utc: str,
    client: httpx.AsyncClient,
    *,
    batch_size: int = 100,
    max_total: Optional[int] = None,
) -> AsyncIterator[List[DiscoveredItem]]:
    """Iterate newest→older through the [start_utc, end_utc] window in batches.

    Paging strategy: after each batch, move `cursor_end` to (oldest seendate − 1s)
    to avoid duplicates and progress strictly older.
    Stops when: empty batch, reached `start_utc`, or `max_total` reached.
    """
    cursor_end = end_utc
    remaining = max_total

    while True:
        req_size = max(1, min(100, batch_size if remaining is None else min(batch_size, remaining)))
        batch = await discover_gdelt(
            query=query,
            timespan="1d",  # ignored because we pass explicit bounds
            maxrecords=req_size,
            client=client,
            start=start_utc,
            end=cursor_end,
        )
        if not batch:
            break

        yield batch

        if remaining is not None:
            remaining -= len(batch)
            if remaining <= 0:
                break

        # Find the oldest published_at we got (using seendate→ISO). If none, stop.
        seen_utc14s: List[str] = []
        for it in batch:
            if it.published_at:
                try:
                    t = dt.datetime.fromisoformat(it.published_at)
                    seen_utc14s.append(t.strftime("%Y%m%d%H%M%S"))
                except Exception:
                    continue
        if not seen_utc14s:
            break

        oldest = min(seen_utc14s)
        next_end = _minus_one_second(oldest)
        if next_end < start_utc:
            break
        cursor_end = next_end