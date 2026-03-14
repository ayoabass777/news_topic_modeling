from src.pipeline.producer import producer
from src.pipeline.fetcher import fetcher
from src.pipeline.persistor import persistor
import asyncio
from typing import Awaitable
import contextlib

from src.utils.logger import get_logger

MAX_QUEUE_SIZE = 100
FETCHER_WORKERS = 3

logger = get_logger("pipeline.run")


class NamedQueue(asyncio.Queue):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


#task to supervise primitive task and log exceptions
async def supervise(name: str, coro: Awaitable[None] )->None:
    """Run a coroutine and log start/finish/crash to pipeline logger."""
    logger.info("run: starting %s task", name)

    try:
        await coro 
        logger.info("run: %s task done", name)
    except asyncio.CancelledError:
        logger.info("run: %s task cancelled", name)
        raise
    except Exception as e:
        logger.exception("run: %s task crashed", name)
        raise

async def monitor_queues(url_queue: NamedQueue, content_queue: NamedQueue,  interval: float = 5.0) -> None:
    #add name of queue later if possible
    """Periodically log queue lengths so backpressure is visible in pipeline logs."""
    try:
        while True:
            await asyncio.sleep(interval)
            logger.debug(
                "run: queue sizes %s=%d %s=%d",
                url_queue.name,
                url_queue.qsize(),
                content_queue.name,
                content_queue.qsize(),
            )
    except asyncio.CancelledError:
        logger.info("monitor_queues: cancelled")
        raise
    except Exception as e:
        logger.exception("monitor_queues: exception %s", e)
        raise
    



async def run_queue()->None:
    """Create a task group that bundles and runs the producer, fetcher(s), and persistor task."""

    url_queue: NamedQueue = NamedQueue(name="url_queue", maxsize=MAX_QUEUE_SIZE)
    content_queue: NamedQueue = NamedQueue(name="content_queue", maxsize=MAX_QUEUE_SIZE)

    async def producer_run_and_forward_sentinel():
        try:
            await producer(url_queue)
        finally:
            try:
                # Signal fetcher to finish when producer is done
                await url_queue.join()
                for _ in range(FETCHER_WORKERS):
                    await url_queue.put(None)
                logger.info(
                    "run: producer forwarded %d URL sentinels to fetchers",
                    FETCHER_WORKERS,
                )
            except Exception as e:
                logger.exception("run: producer failed to forward URL sentinel: %s", e)

    async def fetchers_run_and_forward_sentinel():
        try:
            async with asyncio.TaskGroup() as ftg:
                for idx in range(FETCHER_WORKERS):
                    ftg.create_task(
                        fetcher(url_queue, content_queue),
                        name=f"fetcher-{idx}",
                    )
        finally:
            try:
                await content_queue.join()
                await content_queue.put(None)
                logger.info("run: fetchers forwarded content sentinel to persistor")
            except Exception as e:
                logger.exception("run: fetchers failed to forward content sentinel: %s", e)

    try:
        logger.info("run: starting pipeline")
        # Start a background task to monitor queue sizes
        monitor_task = asyncio.create_task(
            monitor_queues(url_queue, content_queue, 5),
            name="monitor_queues",
        )

        async with asyncio.TaskGroup() as tg:
            #Run each stage under supervise; if one crashes, the whole pipeline cancels
            tg.create_task(supervise("persistor", persistor(content_queue)), name="supervise_persistor")
        
            tg.create_task(supervise("fetchers", fetchers_run_and_forward_sentinel()), name="supervise_fetchers")

            tg.create_task(supervise("producer", producer_run_and_forward_sentinel()), name="supervise_producer")

    finally:
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task
            
        logger.info(
            "run: final queue sizes %s=%d %s=%d",
            url_queue.name,
            url_queue.qsize(),
            content_queue.name,
            content_queue.qsize(),
        )



if __name__ == "__main__":
    asyncio.run(run_queue())
