"""
Asyncio transport adapters
==========================

Thin wrappers around ``asyncio.Queue`` that implement the generic
``Sender[T]`` / ``Receiver[T]`` protocols defined in
``news_topic_model.transport.base``.

These are intended for local/dev and batch pipelines. They optionally
support a *sentinel* object to signal end-of-stream so that receivers can
terminate cleanly (useful when orchestrating finite jobs). In long-lived
service mode, omit the sentinel and the receiver will run indefinitely.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Generic, Optional, Tuple, TypeVar

from news_topic_model.transport.base import Sender, Receiver

T = TypeVar("T")

__all__ = [
    "AsyncioSender",
    "AsyncioReceiver",
]


class AsyncioSender(Sender[T], Generic[T]):
    """A ``Sender[T]`` backed by an ``asyncio.Queue``.

    Parameters
    ----------
    queue : asyncio.Queue
        The underlying queue used to buffer messages.
    sentinel : object | None, optional
        If provided, ``close()`` will enqueue this sentinel to indicate
        end-of-stream to receivers. The sentinel is compared by identity
        (``is``), so it should be a unique object.
    """

    def __init__(self, queue: asyncio.Queue, *, sentinel: object | None = None) -> None:
        self._q = queue
        self._sentinel = sentinel
        self._closed = False

    async def send(self, msg: T) -> None:
        """Enqueue a message, awaiting if the queue is full (backpressure)."""
        if self._closed:
            raise RuntimeError("AsyncioSender is closed")
        await self._q.put(msg)

    async def close(self) -> None:
        """Optionally signal end-of-stream with the configured sentinel.

        If no sentinel was configured, this is a no-op. Multiple calls are safe.
        """
        if self._closed:
            return
        self._closed = True
        if self._sentinel is not None:
            await self._q.put(self._sentinel)


class AsyncioReceiver(Receiver[T], Generic[T]):
    """A ``Receiver[T]`` backed by an ``asyncio.Queue``.

    Parameters
    ----------
    queue : asyncio.Queue
        The underlying queue to consume from.
    sentinel : object | None, optional
        If provided, the receiver will stop iteration when it dequeues the
        sentinel (compared by identity). The sentinel item will be marked
        ``task_done()`` and not yielded.
    """

    def __init__(self, queue: asyncio.Queue, *, sentinel: object | None = None) -> None:
        self._q = queue
        self._sentinel = sentinel

    def __aiter__(self) -> AsyncIterator[T]:
        async def _gen() -> AsyncIterator[T]:
            while True:
                item = await self._q.get()
                try:
                    if self._sentinel is not None and item is self._sentinel:
                        # Swallow sentinel and terminate the stream
                        return
                    # Normal item
                    yield item  # type: ignore[misc]
                finally:
                    # Always account for the dequeued item (including sentinel)
                    self._q.task_done()
        return _gen()

    # Optional convenience to receive exactly one message
    async def receive(self) -> T:
        item = await self._q.get()
        try:
            if self._sentinel is not None and item is self._sentinel:
                raise StopAsyncIteration
            return item  # type: ignore[misc]
        finally:
            self._q.task_done()
