"""
Domain Policy & Seen Store
==========================

This module implements two related concerns:

1) URL/content deduplication and metadata via SQLite tables:
   - `content(content_sha1 PK, first_seen_ts, last_seen_ts)`
   - `url_seen(canonical_url PK, content_sha1 FK, source_domain, published_at, first_seen_ts, last_seen_ts)`

2) Per-domain routing policy via SQLite table `domain_policy` so the fetcher can
   decide how to treat each site on future runs based on observed behavior.

Domain Policy — What & Why
--------------------------
- `mode` controls *how* to fetch next time for a domain:
  - `direct`            → normal HTML fetch
  - `throttle`          → HTML fetch but slow down / honor Retry-After
  - `alternate:rss`     → prefer RSS; avoid direct HTML fetches
  - `alternate:reader`  → use a reader/extractor backend
  - `excluded`          → skip the domain entirely
- Additional signals stored: `rss_url`, `hard_block`, `robots_disallow`,
  `success_count`, `fail_count`, `last_status`, `retry_after_until_ts`.

Fetcher integration (current & future)
--------------------------------------
**Current**: fetcher performs direct HTML requests and records outcomes to
`domain_policy` using `record_outcome()`. You can optionally flip `mode` on
hard statuses (403/406) with `set_mode()` and set a cooldown with
`set_retry_after()` when 429 is observed.

**Future**: if `choose_mode(domain)` returns `alternate:rss`, the fetcher should
consume the domain's RSS feed (kept in `rss_url`) and normalize entries to the
same document schema passed to the persistor. The persistor is unchanged; it
stores normalized documents regardless of origin.

Design choices
--------------
- All timestamps are DB-stamped in ISO-8601 UTC to avoid skew.
- WAL + busy_timeout improves concurrency for multiple fetchers.
- `executescript` is used for schema strings with multiple statements.
- The router (`choose_mode`) is intentionally simple and fast; refine as needed.
"""

from __future__ import annotations

import os
import sqlite3
import datetime as dt
from typing import Optional
import logging
import time
import random

logger = logging.getLogger(__name__)

# --- Normalized schema with DB-stamped ISO-8601 UTC timestamps ---
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
CREATE INDEX IF NOT EXISTS idx_url_seen_last_seen ON url_seen(last_seen_ts);
CREATE INDEX IF NOT EXISTS idx_url_seen_domain_date ON url_seen(source_domain, published_at);
"""

SCHEMA_DOMAIN_POLICY = """
CREATE TABLE IF NOT EXISTS domain_policy (
  domain               TEXT PRIMARY KEY,
  mode                 TEXT NOT NULL DEFAULT 'direct',
  rss_url              TEXT,
  robots_disallow      INTEGER NOT NULL DEFAULT 0,
  hard_block           INTEGER NOT NULL DEFAULT 0,
  last_policy_reason   TEXT,
  success_count        INTEGER NOT NULL DEFAULT 0,
  fail_count           INTEGER NOT NULL DEFAULT 0,
  last_status          INTEGER,
  retry_after_until_ts TEXT,
  last_probe_ts        TEXT,
  policy_set_ts        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_ts           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_domain_policy_mode    ON domain_policy(mode);
CREATE INDEX IF NOT EXISTS idx_domain_policy_updated ON domain_policy(updated_ts);
"""


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 with trailing 'Z'."""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_iso(ts: str | None) -> dt.datetime | None:
    """Parse ISO-8601 timestamps accepting both 'Z' and explicit offsets.
    Returns a timezone-aware datetime in UTC, or None on failure.
    """
    if not ts:
        return None
    s = ts.strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    try:
        dt_obj = dt.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(dt.timezone.utc)


class SeenStore:
    """SQLite-backed store for dedupe + metadata.

    Tables:
      - content(content_sha1 PK, first_seen_ts, last_seen_ts)
      - url_seen(canonical_url PK, content_sha1 FK, source_domain, published_at,
                 first_seen_ts, last_seen_ts)
    """

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(
            path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        # Improve concurrency & robustness
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
            self.conn.execute("PRAGMA busy_timeout=15000;")  # ms
        except Exception as e:
            logger.debug("SeenStore PRAGMAs warning: %s", e)

        # Use executescript because schema strings may contain multiple statements
        self._executescript_with_retry(SCHEMA_CONTENT)
        self._executescript_with_retry(SCHEMA_URL_SEEN)
        self._executescript_with_retry(SCHEMA_INDEXES)
        self._executescript_with_retry(SCHEMA_DOMAIN_POLICY)
        self.conn.commit()

        # Best-effort migration from legacy single-table 'seen' if present
        try:
            self._migrate_legacy_seen()
        except Exception as e:
            # Non-fatal: surface but continue
            logger.warning("Legacy 'seen' migration skipped/failed: %s", e)

    def _execute_with_retry(self, sql: str, params: tuple = (), max_attempts: int = 8, base_sleep: float = 0.05):
        """Execute a single statement with retries on SQLITE_BUSY/locked.
        Uses exponential backoff with jitter. Returns the cursor.
        """
        attempt = 0
        while True:
            try:
                return self.conn.execute(sql, params)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if ("database is locked" in msg) or ("database is busy" in msg):
                    if attempt + 1 >= max_attempts:
                        raise
                    sleep = min(base_sleep * (2 ** attempt), 0.8)
                    # add 0.7x..1.3x jitter to avoid herding
                    sleep *= (0.7 + 0.6 * random.random())
                    time.sleep(sleep)
                    attempt += 1
                    continue
                raise

    def _executescript_with_retry(self, script: str, max_attempts: int = 4, base_sleep: float = 0.05):
        attempt = 0
        while True:
            try:
                return self.conn.executescript(script)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if ("database is locked" in msg) or ("database is busy" in msg):
                    if attempt + 1 >= max_attempts:
                        raise
                    sleep = min(base_sleep * (2 ** attempt), 0.8)
                    # add 0.7x..1.3x jitter to avoid herding
                    sleep *= (0.7 + 0.6 * random.random())
                    time.sleep(sleep)
                    attempt += 1
                    continue
                raise

    # --- Basic lookups ---
    def has_url(self, canonical_url: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM url_seen WHERE canonical_url = ? LIMIT 1", (canonical_url,)
        )
        return cur.fetchone() is not None

    def has_sha(self, content_sha1: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM content WHERE content_sha1 = ? LIMIT 1", (content_sha1,)
        )
        return cur.fetchone() is not None

    # --- Upsert ---
    def upsert(
        self,
        *,
        canonical_url: str,
        content_sha1: str,
        source_domain: Optional[str],
        published_at: Optional[str],
    ) -> None:
        """Upsert content+URL rows. DB stamps timestamps; bumps last_seen_ts on conflict."""
        with self.conn:
            # 1) content: ensure a row exists; bump last_seen_ts on conflict
            self._execute_with_retry(
                """
                INSERT INTO content (content_sha1)
                VALUES (?)
                ON CONFLICT(content_sha1) DO UPDATE SET
                  last_seen_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                """,
                (content_sha1,),
            )
            # 2) url_seen: insert or update; keep published_at if new is NULL; bump last_seen_ts
            self._execute_with_retry(
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

    # --- Domain policy access ---
    def domain_policy(self) -> "DomainPolicyStoreSQL":
        return DomainPolicyStoreSQL(self.conn)

    # --- Close / context mgmt ---
    def close(self) -> None:
        conn = getattr(self, "conn", None)
        # prevent accidental reuse after close
        self.conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception as e:
            logger.warning("SeenStore.close() error: %s", e)
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --- Migration helpers ---
    def _table_exists(self, name: str) -> bool:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        )
        return cur.fetchone() is not None

    def _migrate_legacy_seen(self) -> None:
        """Migrate rows from legacy 'seen' table if present.

        Legacy expected columns: canonical_url, content_sha1, source_domain,
        published_at, first_seen_ts, last_seen_ts.
        """
        if not self._table_exists("seen"):
            return

        # Ensure new tables exist
        self._executescript_with_retry(SCHEMA_CONTENT)
        self._executescript_with_retry(SCHEMA_URL_SEEN)
        self._executescript_with_retry(SCHEMA_INDEXES)

        with self.conn:  # transactional migration
            cur = self.conn.execute(
                "SELECT canonical_url, content_sha1, source_domain, published_at, first_seen_ts, last_seen_ts FROM seen"
            )
            rows = cur.fetchall()
            for canonical_url, content_sha1, source_domain, published_at, first_seen_ts, last_seen_ts in rows:
                if not content_sha1:
                    # Cannot normalize without a content hash
                    continue
                # content upsert: preserve earliest first_seen, latest last_seen
                self._execute_with_retry(
                    """
                    INSERT INTO content (content_sha1, first_seen_ts, last_seen_ts)
                    VALUES (?, ?, ?)
                    ON CONFLICT(content_sha1) DO UPDATE SET
                      first_seen_ts = COALESCE(content.first_seen_ts, excluded.first_seen_ts),
                      last_seen_ts  = MAX(COALESCE(content.last_seen_ts, excluded.last_seen_ts), excluded.last_seen_ts)
                    """,
                    (
                        content_sha1,
                        first_seen_ts or last_seen_ts or now_utc_iso(),
                        last_seen_ts or first_seen_ts or now_utc_iso(),
                    ),
                )
                # url_seen upsert
                if canonical_url:
                    self._execute_with_retry(
                        """
                        INSERT INTO url_seen (canonical_url, content_sha1, source_domain, published_at, first_seen_ts, last_seen_ts)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(canonical_url) DO UPDATE SET
                          content_sha1 = excluded.content_sha1,
                          source_domain = excluded.source_domain,
                          published_at = COALESCE(excluded.published_at, url_seen.published_at),
                          first_seen_ts = COALESCE(url_seen.first_seen_ts, excluded.first_seen_ts),
                          last_seen_ts  = MAX(url_seen.last_seen_ts, excluded.last_seen_ts)
                        """,
                        (
                            canonical_url,
                            content_sha1,
                            source_domain,
                            published_at,
                            first_seen_ts or last_seen_ts or now_utc_iso(),
                            last_seen_ts or first_seen_ts or now_utc_iso(),
                        ),
                    )
            # Preserve legacy table to avoid re-migrating
            try:
                self._execute_with_retry("ALTER TABLE seen RENAME TO seen_legacy")
            except sqlite3.OperationalError as e:
                # If it was already renamed by another worker, ignore
                logger.debug("Legacy table rename skipped: %s", e)


class DomainPolicyStoreSQL:
    """SQLite-backed per-domain policy store using the same connection.
    Keeps lightweight, resumable state about how to fetch a domain: direct, throttle,
    alternate:rss, alternate:reader, excluded. Also tracks coarse health stats.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        # Attach a lightweight retry wrapper locally for writes
        def _exec(sql: str, params: tuple = ()):
            attempt = 0
            while True:
                try:
                    return self.conn.execute(sql, params)
                except sqlite3.OperationalError as e:
                    msg = str(e).lower()
                    if ("database is locked" in msg) or ("database is busy" in msg):
                        if attempt >= 7:
                            raise
                        sleep = min(0.05 * (2 ** attempt), 0.8)
                        sleep *= (0.7 + 0.6 * random.random())
                        time.sleep(sleep)
                        attempt += 1
                        continue
                    raise
        self._exec = _exec

    # --- core helpers ---
    def _seed_if_missing(self, domain: str) -> None:
        self._exec(
            """
            INSERT OR IGNORE INTO domain_policy(domain)
            VALUES (?)
            """,
            (domain,),
        )

    def get(self, domain: str) -> dict:
        self._seed_if_missing(domain)
        cur = self.conn.execute(
            """
            SELECT domain, mode, rss_url, robots_disallow, hard_block,
                   last_policy_reason, success_count, fail_count, last_status,
                   retry_after_until_ts, last_probe_ts, policy_set_ts, updated_ts
            FROM domain_policy WHERE domain = ?
            """,
            (domain,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        (domain, mode, rss_url, robots_disallow, hard_block,
         last_policy_reason, success_count, fail_count, last_status,
         retry_after_until_ts, last_probe_ts, policy_set_ts, updated_ts) = row
        return {
            "domain": domain,
            "mode": mode,
            "rss_url": rss_url,
            "robots_disallow": robots_disallow,
            "hard_block": hard_block,
            "last_policy_reason": last_policy_reason,
            "success_count": success_count,
            "fail_count": fail_count,
            "last_status": last_status,
            "retry_after_until_ts": retry_after_until_ts,
            "last_probe_ts": last_probe_ts,
            "policy_set_ts": policy_set_ts,
            "updated_ts": updated_ts,
        }

    def get_mode(self, domain: str) -> str:
        rec = self.get(domain)
        return rec.get("mode", "direct")

    # --- updates ---
    def set_mode(self, domain: str, mode: str, reason: Optional[str] = None) -> None:
        self._seed_if_missing(domain)
        self._exec(
            """
            UPDATE domain_policy
               SET mode = ?,
                   last_policy_reason = ?,
                   policy_set_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                   updated_ts      = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE domain = ?
            """,
            (mode, reason, domain),
        )

    def update_rss(self, domain: str, rss_url: Optional[str]) -> None:
        self._seed_if_missing(domain)
        if rss_url:
            self._exec(
                """
                UPDATE domain_policy
                   SET rss_url = COALESCE(rss_url, ?),
                       updated_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                 WHERE domain = ?
                """,
                (rss_url, domain),
            )

    def set_retry_after(self, domain: str, until_iso: Optional[str]) -> None:
        self._seed_if_missing(domain)
        self._exec(
            """
            UPDATE domain_policy
               SET retry_after_until_ts = ?,
                   updated_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE domain = ?
            """,
            (until_iso, domain),
        )

    def record_outcome(self, domain: str, *, status: Optional[int], extract_ok: Optional[bool]) -> None:
        self._seed_if_missing(domain)
        succ_inc = 1 if extract_ok is True else 0
        fail_inc = 1 if (extract_ok is False) or (status is not None and status >= 400) else 0
        self._exec(
            """
            UPDATE domain_policy
               SET success_count   = success_count + ?,
                   fail_count      = fail_count    + ?,
                   last_status     = COALESCE(?, last_status),
                   last_probe_ts   = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                   updated_ts      = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE domain = ?
            """,
            (succ_inc, fail_inc, status, domain),
        )
        # Optional soft decay: on a clean 2xx with successful extraction, reduce fail_count by 1 (floor at 0)
        if status is not None and 200 <= status < 300 and extract_ok:
            self._exec(
                "UPDATE domain_policy SET fail_count = CASE WHEN fail_count > 0 THEN fail_count - 1 ELSE 0 END, updated_ts = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE domain = ?",
                (domain,),
            )

    # --- router ---
    def choose_mode(self, domain: str) -> str:
        rec = self.get(domain)
        # Honor explicit non-direct modes
        mode = rec.get("mode", "direct")
        if mode != "direct":
            return mode
        # Retry-After cooldown -> throttle
        ra_dt = _parse_iso(rec.get("retry_after_until_ts"))
        if ra_dt and ra_dt > dt.datetime.now(dt.timezone.utc):
            return "throttle"
        # Hard blocks / repeated 403/406 -> alternate
        if rec.get("hard_block"):
            return "alternate:rss" if rec.get("rss_url") else "alternate:reader"
        if rec.get("fail_count", 0) >= 5 and rec.get("last_status") in (403, 406):
            return "alternate:rss" if rec.get("rss_url") else "alternate:reader"
        if rec.get("last_status") == 429:
            return "throttle"
        return "direct"
"""Domain Policy usage (fetcher)
----------------------------
1) Instantiate once:

    seen = SeenStore(str(indexes_dir / "seen.sqlite"))
    policy = seen.domain_policy()

2) Before fetching an article URL:

    domain = get_domain(normalized_url)
    mode = policy.choose_mode(domain)
    if mode == "throttle":
        # honor retry-after and add small per-domain delay/jitter
    elif mode == "alternate:rss":
        # (future) use policy.rss_url to pull feed entries instead of HTML
    elif mode == "alternate:reader":
        # (future) call readability/reader backend instead of direct HTML
    elif mode == "excluded":
        # skip

3) After request/extraction:

    policy.record_outcome(domain, status=http_status, extract_ok=bool(text))
    if http_status in (403, 406):
        # optionally flip immediately
        policy.set_mode(domain, "alternate:rss" if has_rss else "alternate:reader", reason=f"http_{http_status}")
    elif http_status == 429 and retry_after_iso:
        policy.set_retry_after(domain, retry_after_iso)
        policy.set_mode(domain, "throttle", reason="http_429")
"""