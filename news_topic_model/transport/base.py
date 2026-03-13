"""
Transport interfaces (sender/receiver)
=====================================

These minimal protocols decouple pipeline stages (producer/fetcher/persistor)
from the underlying messaging mechanism (asyncio.Queue, Kafka, gRPC, etc.).

Stages should depend only on these interfaces, not on a specific transport.

Design goals
------------
- Tiny, explicit surface: `send()` for producers; async iteration for consumers.
- Plays well with both in-process queues and networked systems.
- Easy to fake in tests.

Example
-------
from typing import AsyncIterator
from news_topic_model.transport.base import Sender, Receiver
from news_topic_model.contract import UrlDiscovered, ArticleFetched

async def producer(settings, out: Sender[UrlDiscovered]):
    await out.send(UrlDiscovered(url="https://..."))

async def fetcher(settings, inp: Receiver[UrlDiscovered], out: Sender[ArticleFetched]):
    async for item in inp:
        ...
        await out.send(ArticleFetched(url=item.url, ...))

"""

from __future__ import annotations

from typing import Protocol, TypeVar, Generic, AsyncIterator, AsyncIterable, runtime_checkable

T = TypeVar("T")


@runtime_checkable
class Sender(Protocol, Generic[T]):
    """A write-only endpoint for messages of type ``T``.

    Implementations may buffer, batch, or block to apply backpressure.
    ``send`` should raise an exception if the message could not be enqueued
    (e.g., transport is closed).
    """

    async def send(self, msg: T) -> None:  # pragma: no cover - interface only
        ...


@runtime_checkable
class Receiver(Protocol, Generic[T]):
    """A read-only endpoint for messages of type ``T``.

    Supports ``async for`` iteration. Implementations should deliver messages in
    the transport's natural order and raise ``StopAsyncIteration`` on end of
    stream (e.g., for batch/queue mode). Long-lived transports (Kafka/gRPC)
    typically do *not* terminate by themselves; they rely on service shutdown.
    """

    def __aiter__(self) -> AsyncIterator[T]:  # pragma: no cover - interface only
        ...

    # Optional convenience for pull-style consumers; not required by stages.
    async def receive(self) -> T:  # pragma: no cover - interface only
        """Receive a single message. Implementations may override for efficiency."""
        async for item in self:
            return item
        raise StopAsyncIteration
