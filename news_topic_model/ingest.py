

#!/usr/bin/env python3
"""
Lightweight news ingestion pipeline (MVP)

- Adapters: GDELT Doc 2.0 (free). RapidAPI/Newsdata can be added later as adapters.
- Flow (single run): discover -> canonicalize -> seen check -> fetch HTML -> extract -> hash -> write JSONL (+ optional Parquet)
- Dedupe: canonical_url + content_sha1 via a tiny SQLite store.

Usage examples:
  # quick run using GDELT for discovery
  python ingest.py --out data --providers gdelt \
      --query "(technology OR ai OR android OR iphone OR pixel OR tesla)" \
      --window 1h --limit 100

  # write Parquet alongside JSONL
  python ingest.py --out data --providers gdelt --window 1h --parquet

  # or explicit window using strict local datetime bounds
  # python ingest.py --out data --providers gdelt --query "(technology OR ai)" --start "2025-08-16 00:00" --end "2025-08-16 10:00"

Notes:
- Set a descriptive User-Agent via UA env if desired.
- Optional: `pip install langdetect` to enable content-based English filtering (hybrid with GDELT sourcelang).
- This is a single-shot run (no scheduler). Use cron to run periodically.
"""

from __future__ import annotations
import argparse
import asyncio
import contextlib
import datetime as dt
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
import urllib.parse as up
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
import logging
from collections import Counter

# Retry policy for network fetches (tune here)
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 3
BACKOFF_INITIAL = 0.5   # seconds
BACKOFF_FACTOR = 2.0    # exponential backoff multiplier
JITTER_MAX = 0.25       # add up to this many seconds of random jitter

MAX_QUEUE_SIZE = 500


try:
    import trafilatura  # type: ignore
except Exception as e:
    print("[error] trafilatura is required. Install with: conda install -c conda-forge trafilatura")
    raise

# Optional Parquet support
try:
    import pandas as pd  # type: ignore
    _HAVE_PANDAS = True
except Exception:
    _HAVE_PANDAS = False

# Optional language detection (hybrid filtering)
try:
    from langdetect import detect_langs  # type: ignore
    from langdetect.detector_factory import DetectorFactory  # type: ignore
    DetectorFactory.seed = 0  # make detection deterministic
    _HAVE_LANGDETECT = True
except Exception:
    _HAVE_LANGDETECT = False

# -----------------------------
# Helpers
# -----------------------------


TRACKING_PARAMS = {
    # UTM params (covered by startswith below, but included for completeness)
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    # ad/analytics click ids
    "fbclid", "gclid", "dclid", "msclkid", "twclid", "yclid",
    # social/app shares & marketing platforms
    "igshid", "_hsmi", "_hsenc", "mc_cid", "mc_eid", "vero_conv", "vero_id",
    # generic campaign refs frequently used by publishers
    "ref", "ref_src", "campaign", "cmp", "cmpid", "mbid",
}

def is_tracking_param(k: str) -> bool:
    """Return True if query parameter name is a known tracking/attribution tag."""
    k = (k or "").lower()
    if k.startswith("utm_"):
        return True
    return k in TRACKING_PARAMS


UA = os.getenv("UA", "news-topic-model/1.0 (+https://github.com/ayoabass777)")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest")

# Simple run-wide counters
metrics: Counter = Counter()


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

# --- Datetime parsing helper for --start/--end CLI (STRICT)
# Allowed input formats (naive = interpreted as local time, then converted to UTC):
#   1) YYYY-MM-DD
#   2) YYYY-MM-DD HH:MM
#   3) YYYY-MM-DD HH:MM:SS
# No relative forms, no ISO T, no timezone suffixes.
_STRICT_DT_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$")

def to_gdelt_utc(s: str) -> str:
    """Parse a strict human datetime into GDELT UTC format YYYYMMDDHHMMSS.
    Accepted: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS'.
    Naive inputs are assumed to be in local time and converted to UTC.
    """
    s = (s or "").strip()
    m = _STRICT_DT_RE.fullmatch(s)
    if not m:
        raise ValueError(
            "Invalid datetime. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]' (local time)."
        )
    year, month, day, hh, mm, ss = m.groups()
    y = int(year); mo = int(month); d = int(day)
    H = int(hh) if hh is not None else 0
    M = int(mm) if mm is not None else 0
    S = int(ss) if ss is not None else 0
    # Build naive local-time dt then convert to UTC
    local_tz = dt.datetime.now().astimezone().tzinfo
    t_local = dt.datetime(y, mo, d, H, M, S, tzinfo=local_tz)
    t_utc = t_local.astimezone(dt.timezone.utc)
    return t_utc.strftime("%Y%m%d%H%M%S")

# --- Helper to parse --window into timedelta
WINDOW_RE = re.compile(r"^(\d+)\s*(min|m|h|d|w)$", re.IGNORECASE)

def parse_window_to_timedelta(window: str) -> dt.timedelta:
    """Parse window strings like '30min', '1h', '1d', '2w' into timedelta."""
    s = (window or "").strip()
    m = WINDOW_RE.fullmatch(s)
    if not m:
        raise ValueError("--window must be like '30min', '1h', '1d', '2w'")
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
    raise ValueError(f"Unsupported window unit: {unit}")


def get_domain(url: str) -> str:
    try:
        return up.urlsplit(url).netloc.lower()
    except Exception:
        return ""



def canonicalize_url(url: str) -> str:
    """Normalize a URL for stable de-duplication.

    Steps:
      1) ensure scheme (default https) and re-parse scheme-less inputs
      2) lowercase host, optionally rewrite leading "m." -> "www." (common news pattern)
      3) drop default ports (:80 for http, :443 for https)
      4) collapse multiple slashes in path and trim trailing slash
      5) drop known tracking params (utm_*, fbclid, gclid, etc.) and sort remaining params
      6) drop fragments
    """
    try:
        u = up.urlsplit(url)
        # Handle scheme-less inputs like "example.com/a" by re-parsing with https://
        if not u.scheme and not u.netloc and "." in u.path and " " not in u.path:
            u = up.urlsplit("https://" + url)

        scheme = (u.scheme or "https").lower()

        # Host normalization: use u.hostname and u.port, lowercase, m. -> www., drop default ports
        host = u.hostname.lower() if u.hostname else ""
        if host.startswith("m."):
            host = "www." + host[2:]
        # Port handling: drop if default, else append
        port = u.port
        if port is not None:
            if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
                pass  # drop port
            else:
                host = f"{host}:{port}"

        # Path normalization: collapse duplicates and trim trailing slash
        path = re.sub(r"//+", "/", u.path or "/").rstrip("/")
        if not path:
            path = "/"

        # Query: filter tracking params and sort deterministically
        raw_q = up.parse_qsl(u.query, keep_blank_values=False)
        kept_q = [(k, v) for (k, v) in raw_q if not is_tracking_param(k)]
        kept_q.sort(key=lambda kv: (kv[0].lower(), kv[1]))
        query = up.urlencode(kept_q)

        return up.urlunsplit((scheme, host, path, query, ""))
    except Exception:
        return url


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


# Hybrid language detection helper
def is_probably_english(text: str, title: str = "", min_prob: float = 0.80) -> bool:
    """Return True if the combined text/title is likely English.
    Uses langdetect if available; if not installed, returns True (to avoid blocking ingestion).
    """
    if not _HAVE_LANGDETECT:
        return True
    sample = (title + "\n" + (text or "")).strip()
    if len(sample) < 40:
        # Too short for reliable detection; let it pass (GDELT filter already applied)
        return True
    try:
        # detect_langs returns a list like [en:0.99, fr:0.01]
        langs = detect_langs(sample[:5000])  # cap to avoid heavy work on huge pages
        for lp in langs:
            if lp.lang == 'en' and lp.prob >= min_prob:
                return True
        return False
    except Exception:
        # On any detection failure, fall back to permissive (don't drop)
        return True


# -----------------------------
# Seen store (SQLite)
# -----------------------------


SCHEMA_CONTENT = """
CREATE TABLE IF NOT EXISTS content (
  content_sha1 TEXT PRIMARY KEY,
  first_seen_ts TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  last_seen_ts  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

SCHEMA_URL_SEEN = """
CREATE TABLE IF NOT EXISTS url_seen (
  canonical_url TEXT PRIMARY KEY,
  content_sha1  TEXT NOT NULL,
  source_domain TEXT,
  published_at  TEXT,
  first_seen_ts TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  last_seen_ts  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  FOREIGN KEY(content_sha1) REFERENCES content(content_sha1)
);
"""

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_url_seen_sha ON url_seen(content_sha1);
"""



class SeenStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute(SCHEMA_CONTENT)
        self.conn.execute(SCHEMA_URL_SEEN)
        self.conn.execute(SCHEMA_INDEXES)
        self.conn.commit()

    def has_url(self, canonical_url: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM url_seen WHERE canonical_url = ?", (canonical_url,))
        return cur.fetchone() is not None

    def has_sha(self, content_sha1: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM content WHERE content_sha1 = ?", (content_sha1,))
        return cur.fetchone() is not None

    def upsert(self, canonical_url: str, content_sha1: str, source_domain: str, published_at: Optional[str]):
        with self.conn:
            # 1) upsert content row (one per content_sha1); DB sets timestamps
            self.conn.execute(
                """
                INSERT INTO content (content_sha1)
                VALUES (?)
                ON CONFLICT(content_sha1) DO UPDATE SET
                  last_seen_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                """,
                (content_sha1,),
            )
            # 2) upsert per-URL row; DB sets first_seen_ts on insert; bump last_seen_ts on conflict
            self.conn.execute(
                """
                INSERT INTO url_seen (canonical_url, content_sha1, source_domain, published_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(canonical_url) DO UPDATE SET
                  content_sha1 = excluded.content_sha1,
                  source_domain = excluded.source_domain,
                  published_at = COALESCE(excluded.published_at, url_seen.published_at),
                  last_seen_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                """,
                (canonical_url, content_sha1, source_domain, published_at),
            )

    def close(self):
        conn = getattr(self, "conn", None)
        # prevent accidental reuse after close
        self.conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception as e:
            log.warning(f"SeenStore.close(): {e}")
            raise


# -----------------------------
# Discovery adapters
# -----------------------------

@dataclass
class DiscoveredItem:
    title: str
    url: str
    published_at: Optional[str]
    source_domain: str
    source_name: str
    discovered_via: str


async def discover_gdelt(
    query: str,
    timespan: str,
    maxrecords: int,
    client: Optional[httpx.AsyncClient] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[DiscoveredItem]:
    """GDELT Doc 2.0 discovery (free). Returns a list of DiscoveredItem.
    Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        # Restrict to English-only sources (per GDELT docs: use uppercase)
        "sourcelang": "ENGLISH",
    }
    if start and end:
        params["STARTDATETIME"] = start
        params["ENDDATETIME"] = end
    else:
        params["timespan"] = timespan
    if maxrecords:
        params["maxrecords"] = maxrecords
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=20)
    try:
        r = await client.get(url, params=params, headers={"User-Agent": UA})
        r.raise_for_status()
        try:
            data = r.json()
        except Exception :
            snippet = (r.text or "")[:200]
            print(f"[error] GDELT JSON parse failed. Status={r.status_code} Body[:200]={snippet!r}")
            return []

        arts = data.get("articles") or []
        out: List[DiscoveredItem] = []
        for a in arts:
            link = a.get("url") or ""
            title = a.get("title") or ""
            source_name = a.get("sourceCommonName") or ""
            pub = a.get("seendate")  # often YYYYMMDDHHMMSS UTC
            pub_iso = None
            if pub and re.match(r"^\d{14}$", pub):
                pub_iso = dt.datetime.strptime(pub, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc).isoformat()
            dom = get_domain(link)
            if link:
                out.append(DiscoveredItem(title=title, url=link, published_at=pub_iso, source_domain=dom, source_name=source_name, discovered_via="gdelt"))
        return out
    finally:
        if own_client:
            await client.aclose()  # type: ignore


# Placeholder for RapidAPI (discovery only). You can implement once you decide the exact provider endpoint.
async def discover_rapidapi_stub(*args, **kwargs) -> List[DiscoveredItem]:
    return []


# -----------------------------
# Fetch & extract
# -----------------------------

async def fetch_html(url: str, client: httpx.AsyncClient) -> Optional[str]:
    for attempt in range(MAX_RETRIES + 1):
        retryable = False
        try:
            r = await client.get(url, follow_redirects=True, headers={"User-Agent": UA})
            if r.status_code < 400:
                return r.text
            # mark retryable status codes (e.g., 429/5xx)
            retryable = r.status_code in RETRY_STATUS
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.PoolTimeout):
            retryable = True
        except Exception:
            # non-retryable unexpected error
            retryable = False

        # stop if not retryable or we've exhausted attempts
        if not retryable or attempt == MAX_RETRIES:
            # Tiny visibility so we know a URL was dropped after retries
            log.warning(f"fetch_html giving up: {url}")
            return None

        # exponential backoff with jitter
        delay = BACKOFF_INITIAL * (BACKOFF_FACTOR ** attempt) + random.uniform(0, JITTER_MAX)
        await asyncio.sleep(delay)

    return None


def extract_text(html: str, url: str, min_chars: int = 400) -> Optional[str]:
    try:
        txt = trafilatura.extract(html, url=url, include_comments=False, include_tables=False)
        txt = normalize_text(txt or "")
        return txt if len(txt) >= min_chars else None
    except Exception:
        return None


# -----------------------------
# Writing
# -----------------------------

@dataclass
class Article:
    doc_id: str
    canonical_url: str
    url_original: str
    source_domain: str
    title: str
    text: str
    published_at: Optional[str]
    crawl_ts: str
    lang: str
    content_sha1: str
    discovered_via: str
    extraction_method: str

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)


def ensure_dirs(base_out: str):
    os.makedirs(base_out, exist_ok=True)
    os.makedirs(os.path.join(base_out, "bronze_jsonl"), exist_ok=True)
    os.makedirs(os.path.join(base_out, "silver_parquet"), exist_ok=True)
    os.makedirs(os.path.join(base_out, "indexes"), exist_ok=True)
    os.makedirs(os.path.join(base_out, "logs"), exist_ok=True)


def jsonl_writer(base_out: str):
    day = dt.datetime.utcnow().strftime("%Y-%m-%d")
    folder = os.path.join(base_out, "bronze_jsonl", day)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"part-{int(time.time())}.jsonl")
    f = open(path, "a", encoding="utf-8")
    return f, path


def parquet_sink(base_out: str):
    day = dt.datetime.utcnow().strftime("%Y-%m-%d")
    folder = os.path.join(base_out, "silver_parquet", f"ingest_date={day}")
    os.makedirs(folder, exist_ok=True)
    return folder


# -----------------------------
# Main pipeline
# -----------------------------

# Global queue for discovered URLs
url_queue: asyncio.Queue[Optional[Tuple[str, DiscoveredItem]]] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
html_queue: asyncio.Queue[Optional[Tuple[str, DiscoveredItem, str, str]]]= asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
async def producer(args: argparse.Namespace) -> None:
    """Producer function to discover items and put them into the queue."""
    
# 1) DISCOVER
    discovered: List[DiscoveredItem] = []
    seen_can: set[str] = set()

    async with httpx.AsyncClient(timeout=20) as client:
        if "gdelt" in args.providers:
            start_utc = to_gdelt_utc(args.start) if getattr(args, "start", None) else None
            end_utc = to_gdelt_utc(args.end) if getattr(args, "end", None) else None

            # Friendly behavior: if only one bound is provided, compute the other using --window
            if (start_utc and not end_utc) or (end_utc and not start_utc):
                delta = parse_window_to_timedelta(args.window)
                if start_utc and not end_utc:
                    start_dt = dt.datetime.strptime(start_utc, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
                    end_dt = start_dt + delta
                    end_utc = end_dt.strftime("%Y%m%d%H%M%S")
                elif end_utc and not start_utc:
                    end_dt = dt.datetime.strptime(end_utc, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
                    start_dt = end_dt - delta
                    start_utc = start_dt.strftime("%Y%m%d%H%M%S")

            # Validate ordering if both are present
            if start_utc and end_utc and start_utc > end_utc:
                raise ValueError(f"--start must be <= --end (got {args.start} > {args.end})")

            items = await discover_gdelt(
                args.query, args.window, args.limit, client, start=start_utc, end=end_utc
            )
            discovered.extend(items)
        if "rapidapi" in args.providers:
            discovered.extend(await discover_rapidapi_stub())
        
        # Canonicalize & unique URLs (pre-seen check)
        for item in discovered:
            can = canonicalize_url(item.url)
            if not can:
                continue
            if can not in seen_can:
                seen_can.add(can)
                await url_queue.put((can, item))   

        metrics["discovered"] += len(discovered)
        metrics["unique_urls"] += len(seen_can)
        queued_count = url_queue.qsize()
        metrics["queued"] += queued_count
        log.info(f"producer: discovered={len(discovered)} unique={len(seen_can)} queued={queued_count}")
        
        num_fetchers = max(1, args.number_of_fetchers)
        for _ in range(num_fetchers):
            await url_queue.put(None)  # Signal end of queue


async def fetcher(args: argparse.Namespace) -> None:
    """ fetch html and extract text from URLs in the queue."""
    base_out = args.out
    ensure_dirs(base_out)

    seen = SeenStore(os.path.join(base_out, "indexes", "seen.sqlite"))
    domain_sems: Dict[str, asyncio.Semaphore] = {}
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            while True:
                msg = await url_queue.get()
                try:
                    if msg is None:
                        # End-of-stream: cascade sentinel downstream and exit
                        await html_queue.put(None)
                        return

                    can, item = msg

                    if seen.has_url(can):
                        metrics["dedup_url"] += 1
                        log.debug(f"dedup url: {can}")
                        continue

                    # per-domain throttle
                    dom = item.source_domain or get_domain(can)
                    if not dom:
                        netloc = up.urlsplit(can).netloc
                        dom = netloc or ("unknown-" + sha1_hex(can)[:8])
                    sem = domain_sems.setdefault(dom, asyncio.Semaphore(max(1, args.domain_concurrency)))

                    async with sem:
                        html = await fetch_html(can, client)

                    if not html:
                        metrics["fetch_fail"] += 1
                        continue

                    text = extract_text(html, can, args.min_chars)
                    if not text:
                        metrics["extract_fail"] += 1
                        continue

                    # Hybrid language filtering: GDELT was already asked for ENGLISH sources;
                    # now verify with a content-based detector to catch mislabels.
                    if not is_probably_english(text, title=item.title or ""):
                        metrics["lang_skip"] += 1
                        log.debug(f"lang skip non-EN: {can}")
                        continue

                    content_hash = sha1_hex(normalize_text(text))
                    if seen.has_sha(content_hash):
                        metrics["dedup_hash"] += 1
                        log.debug(f"dedup hash: {can}")
                        continue

                    metrics["ready_to_write"] += 1
                    await html_queue.put((can, item, text, content_hash))
                finally:
                    # Mark the queue item as processed
                    url_queue.task_done()
    finally:
        # Always close DB connection for this fetcher
        seen.close()


async def persistor(args: argparse.Namespace) -> None:
    base_out = args.out
    ensure_dirs(base_out)

    seen = SeenStore(os.path.join(base_out, "indexes", "seen.sqlite"))
    jf, jpath = jsonl_writer(base_out)
    count = 0
    # Parquet batching (optional)
    PARQUET_BATCH = getattr(args, "parquet_batch", 200)
    parquet_rows: List[Dict[str, Any]] = []
    parquet_folder = parquet_sink(base_out) if args.parquet and _HAVE_PANDAS else None

    remaining_sentinels = max(1, args.number_of_fetchers)
    try:
        while True:
            msg = await html_queue.get()
            if msg is None:
                html_queue.task_done()
                remaining_sentinels -= 1
                if remaining_sentinels == 0:
                    break
                continue

            canonical_url, item, text, content_hash = msg
            dom = item.source_domain or get_domain(canonical_url)
            if not dom:
                netloc = up.urlsplit(canonical_url).netloc
                dom = netloc or ("unknown-" + sha1_hex(canonical_url)[:8])
            doc_id = sha1_hex(canonical_url)
            title = item.title or canonical_url

            art = Article(
                doc_id=doc_id,
                canonical_url=canonical_url,
                url_original=item.url,
                source_domain=dom,
                title=title,
                text=text,
                published_at=item.published_at,
                crawl_ts=now_utc(),
                lang="en",
                content_sha1=content_hash,
                discovered_via=item.discovered_via,
                extraction_method="trafilatura",
            )

            # Guard against concurrent duplicates that slipped past fetchers
            if seen.has_sha(content_hash):
                # Still ensure URL mapping is up-to-date, but skip writing JSONL to avoid dupes
                seen.upsert(canonical_url, content_hash, dom, item.published_at)
                continue

            # First record content + URL mapping, then write to sinks
            seen.upsert(canonical_url, content_hash, dom, item.published_at)
            try:
                jf.write(art.to_json() + "\n")
                count += 1
                metrics["written_jsonl"] += 1
                # Buffer for Parquet if enabled
                if args.parquet and _HAVE_PANDAS:
                    parquet_rows.append(art.__dict__)
                    if len(parquet_rows) >= PARQUET_BATCH:
                        try:
                            import pandas as _pd  # local import to keep startup light
                            outp = os.path.join(parquet_folder, f"part-{int(time.time())}.parquet")  # type: ignore[arg-type]
                            _pd.DataFrame(parquet_rows).to_parquet(outp, index=False)
                            parquet_rows.clear()
                            log.info(f"parquet: wrote batch to {outp}")
                        except Exception as e:
                            log.warning(f"parquet batch write failed: {e}")
            finally:
                if (count % 50) == 0:
                    # Flush every 50 records to avoid too many small writes
                    jf.flush()
                html_queue.task_done()
    finally:
        jf.close()
        seen.close()
        # Flush remaining Parquet rows if any
        if args.parquet and _HAVE_PANDAS and parquet_rows:
            try:
                import pandas as _pd  # local import
                outp = os.path.join(parquet_folder, f"part-{int(time.time())}.parquet")  # type: ignore[arg-type]
                _pd.DataFrame(parquet_rows).to_parquet(outp, index=False)
                log.info(f"parquet: wrote final batch to {outp}")
            except Exception as e:
                log.warning(f"parquet final write failed: {e}")
        elif args.parquet and not _HAVE_PANDAS:
            log.warning("--parquet requested but pandas/pyarrow not available; skipped Parquet")
        print(f"[persistor] Wrote {count} records to {jpath}")

        



# Queue-based orchestrator
async def run_queue(args: argparse.Namespace) -> None:
    """Run the queue-based pipeline: producer -> fetcher(s) -> persistor."""
    base_out = args.out
    ensure_dirs(base_out)

    # Start consumers first so sentinels have readers
    num_fetchers = max(1, args.number_of_fetchers)
    fetch_tasks = [asyncio.create_task(fetcher(args)) for _ in range(num_fetchers)]
    persist_task = asyncio.create_task(persistor(args))

    # Start producer
    prod_task = asyncio.create_task(producer(args))

    # Wait for producer to finish enqueueing (including sentinels)
    await prod_task

    # Wait for the queues to drain fully
    await url_queue.join()
    await html_queue.join()

    # Wait for consumers to finish
    await asyncio.gather(*fetch_tasks, return_exceptions=True)
    await persist_task

    # Run summary
    if metrics:
        summary = ", ".join(f"{k}={v}" for k, v in metrics.items())
        log.info(f"run summary: {summary}")


# -----------------------------
# CLI
# -----------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="News ingestion (MVP)")
    p.add_argument("--out", default="data", help="Output base folder (default: data)")
    p.add_argument("--providers", nargs="+", default=["gdelt"], choices=["gdelt", "rapidapi"], help="Discovery providers to use")
    p.add_argument("--query", default="(technology OR ai)", help="Provider search query (GDELT style)")
    p.add_argument("--window", default="1d", help="GDELT timespan window (e.g., 30min, 1h, 2h)")
    p.add_argument("--start", default=None,
                   help="Start datetime in local time: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]'. If provided with --end, overrides --window.")
    p.add_argument("--end", default=None,
                   help="End datetime in local time: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]'. If provided with --start, overrides --window.")
    p.add_argument("--limit", type=int, default=100, help="Max records to request from provider (per call)")
    p.add_argument("--domain-concurrency", type=int, default=3, help="Max concurrent fetches per domain")
    p.add_argument("--parquet", action="store_true", help="Also write Parquet (requires pandas/pyarrow)")
    p.add_argument("--number_of_fetchers", type=int, default=1, help="Number of fetchers to run in parallel (default: 1)")
    p.add_argument("--min-chars", type=int, default=400,
                   help="Minimum article text length to keep (default: 400)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        asyncio.run(run_queue(args))
        return 0
    except KeyboardInterrupt:
        print("[info] Cancelled by user")
        return 130
    except Exception as e:
        print(f"[error] {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())