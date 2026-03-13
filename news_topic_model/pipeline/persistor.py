from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
from dataclasses import dataclass, asdict
from typing import Optional, Tuple

import pandas as pd

from news_topic_model.adapters.gdelt import DiscoveredItem
from news_topic_model.storage.seen_store import SeenStore
from news_topic_model.utils.iohelpers import ensure_dir, writer_jsonl, write_parquet
from news_topic_model.utils.urlnorm import get_domain

from news_topic_model.contract.policy_event import (
    PolicyEvent,
    PolicyEventType,
    RecordOutcome,
    SetMode,
    SetRetryAfter,
    UpdateRss,
)

from news_topic_model.config import persistor_logger as logger

FLUSH_EVERY = 1000  # flush JSONL every N records


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


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
        return json.dumps(asdict(self), ensure_ascii=False)


async def persistor(
    args,
    html_queue: asyncio.Queue[Optional[Tuple[str, DiscoveredItem, str, str]]],
    policy_queue: asyncio.Queue[Optional[PolicyEvent]],
) -> None:
    """Consume extracted articles and persist to SQLite + JSONL + Parquet.
    Consumes policy events and applies them to the domain policy via SeenStore.

    - Upserts into normalized SQLite (content, url_seen) via SeenStore
    - Writes one JSONL line per accepted article under data/bronze_jsonl/<date>/part-*.jsonl
    - Writes Parquet files under data/silver_parquet/ingest_date=YYYY-MM-DD
    - Flushes periodically to reduce syscall overhead while bounding loss on crash
    - Shuts down after receiving one sentinel per fetcher
    - This coroutine performs the actual file write (atomic) using a path provided by write_parquet
    """
    base_out = args.out
    ensure_dir(base_out)

    seen = SeenStore(f"{base_out}/indexes/seen.sqlite")
    jf = None  # will open on first write
    jpath = ""

    parquet_batch = []

    count = 0
    remaining_sentinels = max(1, getattr(args, "number_of_fetchers", 1))

    policy = seen.domain_policy()
    remaining_policy_sentinels = max(1, getattr(args, "number_of_fetchers", 1))

    # Prepare concurrent queue readers
    pending_html = asyncio.create_task(html_queue.get())
    pending_policy = asyncio.create_task(policy_queue.get())

    try:
        while True:
            tasks = set()
            if pending_html is not None:
                tasks.add(pending_html)
            if pending_policy is not None:
                tasks.add(pending_policy)
            if not tasks:
                # Nothing more to read
                break

            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            if pending_html in done:
                msg = pending_html.result()
                if msg is None:
                    html_queue.task_done()
                    remaining_sentinels -= 1
                    if remaining_sentinels == 0:
                        # stop listening to html_queue
                        pending_html = None
                    else:
                        pending_html = asyncio.create_task(html_queue.get())
                else:
                    canonical_url, item, text, content_hash = msg
                    logger.debug("Processing article from %s", canonical_url)
                    dom = item.source_domain or get_domain(canonical_url)
                    title = item.title or canonical_url

                    art = Article(
                        doc_id=content_hash,
                        canonical_url=canonical_url,
                        url_original=item.url,
                        source_domain=dom,
                        title=title,
                        text=text,
                        published_at=item.published_at,
                        crawl_ts=now_utc_iso(),
                        lang="en",
                        content_sha1=content_hash,
                        discovered_via=item.discovered_via,
                        extraction_method="trafilatura",
                    )

                    # Upsert into normalized store
                    seen.upsert(
                        canonical_url=canonical_url,
                        content_sha1=content_hash,
                        source_domain=dom,
                        published_at=item.published_at,
                    )

                    if jf is None:
                        jf, jpath = writer_jsonl(base_out)
                    jf.write(art.to_json() + "\n")
                    parquet_batch.append(asdict(art))
                    count += 1

                    if (count % FLUSH_EVERY) == 0:
                        if jf is not None:
                            jf.flush()
                        if parquet_batch:
                            parquet_path = write_parquet(base_out)
                            df = pd.DataFrame(parquet_batch)
                            tmp_path = f"{parquet_path}.tmp"
                            df.to_parquet(tmp_path, index=False, engine="pyarrow", compression="snappy")
                            os.replace(tmp_path, parquet_path)
                            logger.info("Flushed %d records to parquet %s", len(parquet_batch), parquet_path)
                            parquet_batch.clear()

                    html_queue.task_done()
                    pending_html = asyncio.create_task(html_queue.get())

            if pending_policy in done:
                evt = pending_policy.result()
                if evt is None:
                    policy_queue.task_done()
                    remaining_policy_sentinels -= 1
                    if remaining_policy_sentinels == 0:
                        pending_policy = None
                    else:
                        pending_policy = asyncio.create_task(policy_queue.get())
                else:
                    # Apply policy event to SeenStore's domain policy
                    try:
                        if isinstance(evt, RecordOutcome):
                            policy.record_outcome(evt.domain, status=evt.status, extract_ok=evt.extract_ok)
                        elif isinstance(evt, SetMode):
                            policy.set_mode(evt.domain, evt.mode, reason=evt.reason)
                        elif isinstance(evt, SetRetryAfter):
                            policy.set_retry_after(evt.domain, evt.until_iso)
                        elif isinstance(evt, UpdateRss):
                            policy.update_rss(evt.domain, evt.rss_url)
                        else:
                            logger.warning("persistor: unknown PolicyEvent type: %r", evt)
                    except Exception:
                        logger.exception("persistor: failed applying PolicyEvent for %s", getattr(evt, 'domain', 'unknown'))
                    finally:
                        policy_queue.task_done()
                        pending_policy = asyncio.create_task(policy_queue.get())

            # Exit condition: both queues have received all sentinels
            if (pending_html is None) and (pending_policy is None):
                break
    finally:
        if jf is not None:
            jf.close()
            # If nothing was written (defensive; should not happen with lazy open), remove the empty file
            if count == 0 and jpath:
                try:
                    os.remove(jpath)
                except FileNotFoundError:
                    pass
        # Write any remaining parquet batch
        if parquet_batch:
            parquet_path = write_parquet(base_out)
            df = pd.DataFrame(parquet_batch)
            tmp_path = f"{parquet_path}.tmp"
            df.to_parquet(tmp_path, index=False, engine="pyarrow", compression="snappy")
            os.replace(tmp_path, parquet_path)
        seen.close()
        if count > 0 and jpath:
            logger.info(f"Wrote {count} records to {jpath}")
        else:
            logger.info("No records written; skipped JSONL and Parquet outputs for this run")