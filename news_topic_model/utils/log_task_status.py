"""Utilities to log asyncio Task completion state in a consistent way.

Usage:
    from news_topic_model.utils.log_task_status import log_task_status, attach_task_logging

    t = tg.create_task(coro(), name="fetcher-1")
    attach_task_logging(t)  # logs when the task completes
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

try:
    # Prefer the pipeline logger if available
    from news_topic_model.config import pipeline_logger as _default_logger
except Exception:  # pragma: no cover - fallback if config isn't importable here
    _default_logger = logging.getLogger("news_topic_model.pipeline")


def _exc_info_tuple(e: BaseException):
    """Return a (type, value, traceback) tuple suitable for logging.exc_info."""
    return (type(e), e, e.__traceback__)


def _brief(obj, limit: int = 200) -> str:
    s = repr(obj)
    return s if len(s) <= limit else s[:limit] + "…"


def log_task_status(task: "asyncio.Task", logger: Optional[logging.Logger] = None) -> None:
    """Log the final state of an asyncio Task.

    This should be called from a Task's done-callback or by a supervising coroutine
    after awaiting the task. It will not raise; it only logs.

    Args:
        task: The asyncio Task whose status should be logged. Must be done.
        logger: Optional logger to use; defaults to the pipeline logger.
    """
    lg = logger or _default_logger

    # Defensive: if the task isn't done yet, do nothing to avoid InvalidStateError
    if not task.done():
        lg.debug("%s: status check called before completion", task.get_name())
        return

    name = task.get_name() or "unnamed-task"
    lg.debug("%s: checking completion status", name)

    if task.cancelled():
        lg.warning("%s: was cancelled", name)
        return

    exc = task.exception()
    if exc is not None:
        # Log with traceback (provide explicit exc_info tuple)
        lg.error("%s: failed with %r", name, exc, exc_info=_exc_info_tuple(exc))
        return

    try:
        result = task.result()
    except Exception as e:  # Unlikely if exception() was None, but be safe
        lg.error("%s: result retrieval failed with %r", name, e, exc_info=_exc_info_tuple(e))
        return

    lg.info("%s: finished with result=%s", name, _brief(result))


def attach_task_logging(task: "asyncio.Task", logger: Optional[logging.Logger] = None) -> None:
    """Attach a done-callback that logs the task's completion status.

    Args:
        task: The task to monitor.
        logger: Optional logger override. If provided, it will be captured in the closure.
    """
    if logger is None:
        task.add_done_callback(log_task_status)
    else:
        # Bind the logger in a tiny wrapper
        task.add_done_callback(lambda t: log_task_status(t, logger=logger))