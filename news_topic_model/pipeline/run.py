"""
Pipeline Orchestrator (TaskGroup)
=================================

Coordinates the three async stages of the ingestion pipeline:
  1) producer  → discovers canonical URLs and enqueues them to `url_queue`.
  2) fetcher(s) → consume URLs, fetch/extract content, and enqueue results to `html_queue`.
  3) persistor → consumes normalized items and policy events; writes to storage and domain policy.

Design & Guarantees
-------------------
- **Fail-fast**: Uses `asyncio.TaskGroup` so if any task raises, siblings are
  cancelled and the exception is re-raised here.
- **No deadlock on bounded queues**: Fetchers and persistor are started **before**
  the producer, so the producer never blocks indefinitely on `put()` when the
  queue is full.
- **Deterministic shutdown**: After the producer completes, we push exactly N URL
  sentinels (N = number of fetchers). Each fetcher forwards one HTML sentinel in
  the happy path so the persistor can exit. TaskGroup cancellation will interrupt
  any blocking `get()` calls on error.

Notes
-----
- Domain policy decisions and browser-like HTTP headers are applied inside the
  fetcher; the orchestrator only supervises lifecycle and queue draining.
- Policy queue visibility is included in queue monitoring to track backpressure.
"""

import asyncio
import contextlib
import logging
import os
import time
from typing import Optional, Tuple, Any, Awaitable

from news_topic_model.pipeline.producer import producer
from news_topic_model.pipeline.fetcher import fetcher
from news_topic_model.pipeline.persistor import persistor
from news_topic_model.config import MAX_QUEUE_SIZE, pipeline_logger as logger


# --- Logging bootstrap (file-only, no console) ---

def _ensure_pipeline_file_logger(args) -> None:
    """Ensure the pipeline logger writes to a rotating file under <base_out>/logs.

    If no handlers are attached to the pipeline logger, attach a RotatingFileHandler.
    This avoids silent logging when config wasn't imported/initialized early.
    """
    base_out = getattr(args, "base_out", "data")
    logs_dir = os.path.join(base_out, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    lg = logger  # imported from config as pipeline_logger
    if not getattr(lg, "handlers", None):
        # Fallback: attach a rotating file handler
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(
            os.path.join(logs_dir, "pipeline.log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        fmt = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        lg.setLevel(logging.DEBUG)
        lg.addHandler(fh)
        lg.propagate = False


# --- Supervisor helpers ---

async def supervise(name: str, coro: Awaitable[Any]):
    """Run a coroutine and log start/finish/crash to the pipeline logger.

    This ensures fetcher/persistor/producer lifecycle events appear in pipeline logs.
    """
    logger.info("%s: start", name)
    try:
        result = await coro
        logger.info("%s: done", name)
        return result
    except asyncio.CancelledError:
        logger.warning("%s: cancelled", name)
        raise
    except Exception:
        logger.exception("%s: crashed", name)
        raise

async def monitor_queues(url_queue: asyncio.Queue, html_queue: asyncio.Queue, policy_queue: asyncio.Queue, interval: float = 5.0) -> None:
    """Periodically log queue lengths so backpressure is visible in pipeline logs."""
    try:
        while True:
            logger.debug("queues: url=%d html=%d policy=%d", url_queue.qsize(), html_queue.qsize(), policy_queue.qsize())
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        # normal on shutdown
        raise

async def _run_fetcher_and_forward_sentinel(name: str,
                                            args,
                                            url_queue: asyncio.Queue,
                                            html_queue: asyncio.Queue,
                                            policy_queue: asyncio.Queue) -> None:
    """Run a single fetcher worker and, on exit, forward one HTML sentinel.

    This guarantees the persistor sees exactly N HTML sentinels (N = number of
    fetchers) when all fetchers terminate, enabling deterministic shutdown.
    """
    try:
        await fetcher(args, url_queue, html_queue, policy_queue)
    finally:
        # Forward one HTML sentinel so persistor can finish when all fetchers exit
        try:
            await html_queue.put(None)
            logger.info("%s: forwarded HTML sentinel to persistor", name)
        except Exception:
            logger.exception("%s: failed to forward HTML sentinel", name)
        # Forward one Policy sentinel so persistor can finish policy side too
        try:
            await policy_queue.put(None)
            logger.info("%s: forwarded POLICY sentinel to persistor", name)
        except Exception:
            logger.exception("%s: failed to forward POLICY sentinel", name)

async def _push_url_sentinels_when_done(prod_task: asyncio.Task,
                                        url_queue: asyncio.Queue,
                                        num_fetchers: int) -> None:
    """Wait for the producer to complete, then push N URL sentinels.

    Parameters
    ----------
    prod_task : asyncio.Task
        The task running the producer coroutine.
    url_queue : asyncio.Queue
        Queue into which URL items (and sentinels) are placed.
    num_fetchers : int
        Number of fetcher workers; one sentinel is pushed per fetcher.
    """
    await prod_task
    logger.info("run: producer finished; sending %d URL sentinels", num_fetchers)
    for i in range(num_fetchers):
        await url_queue.put(None)
        logger.info("run: URL sentinel %d/%d enqueued", i + 1, num_fetchers)
    logger.info("run: all URL sentinels enqueued")

async def run_queue(args) -> None:
    """
    Wire up the producer → fetcher(s) → persistor pipeline and run it.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI args. Expected to contain at least `number_of_fetchers`.

    Behavior
    --------
    - Creates two bounded queues (`url_queue`, `html_queue`).
    - Starts **fetchers and persistor first** inside a TaskGroup so they can
      immediately drain/enqueue while the producer runs.
    - Starts the producer as a TaskGroup task as well.
    - A helper task waits for the producer to complete and then pushes N URL
      sentinels (N = number of fetchers) so each fetcher can terminate once the
      queue drains.
    - TaskGroup provides fail-fast semantics: any task error cancels siblings and
      re-raises the exception here.

    Shutdown
    --------
    - Best-effort queue drain via `queue.join()` in `finally` to avoid leaving
      unfinished tasks.
    """
    _ensure_pipeline_file_logger(args)
    logger.info("run: logger initialized (base_out=%s)", getattr(args, "base_out", "data"))

    url_queue: asyncio.Queue[Optional[Tuple[str, object]]] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    html_queue: asyncio.Queue[Optional[Tuple[str, object, str, str]]] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    policy_queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    num_fetchers = max(1, getattr(args, "number_of_fetchers", 2))
    logger.info("run: starting with %d fetchers, max_queue_size=%d", num_fetchers, MAX_QUEUE_SIZE)

    try:
        # Start queue monitor OUTSIDE the TaskGroup; we'll cancel it on shutdown
        monitor_task = asyncio.create_task(monitor_queues(url_queue, html_queue, policy_queue), name="queue-monitor")
        async with asyncio.TaskGroup() as tg:
            # Start fetchers & persistor first so bounded queues don't deadlock producer.
            for i in range(num_fetchers):
                tg.create_task(
                    supervise(
                        f"fetcher-{i}",
                        _run_fetcher_and_forward_sentinel(f"fetcher-{i}", args, url_queue, html_queue, policy_queue),
                    ),
                    name=f"fetcher-{i}",
                )
            tg.create_task(supervise("persistor", persistor(args, html_queue, policy_queue)), name="persistor")

            # Run producer concurrently inside the group
            prod_task = tg.create_task(supervise("producer", producer(args, url_queue)), name="producer")

            # When producer completes, push URL sentinels for each fetcher
            tg.create_task(_push_url_sentinels_when_done(prod_task, url_queue, num_fetchers),
                           name="sentinels")
            # TaskGroup waits until all tasks complete or one raises.
    finally:
        # Best-effort: ensure queues drain; suppress if cancellation is in-flight
        with contextlib.suppress(Exception):
            await url_queue.join()
        with contextlib.suppress(Exception):
            await html_queue.join()
        with contextlib.suppress(Exception):
            await policy_queue.join()
        logger.info("run: queues drained (url=%d html=%d policy=%d)", url_queue.qsize(), html_queue.qsize(), policy_queue.qsize())
        # Stop the monitor task since it runs forever
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task
        logger.info("run: pipeline complete")