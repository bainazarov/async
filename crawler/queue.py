import asyncio
import itertools
import logging

logger = logging.getLogger(__name__)


class CrawlerQueue:

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[tuple[int, int, str]] = asyncio.PriorityQueue()
        self._counter = itertools.count()
        self._known: set[str] = set()
        self._pending: set[str] = set()
        self._depths: dict[str, int] = {}
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        if url in self._known:
            return False
        self._known.add(url)
        self._pending.add(url)
        self._depths[url] = depth
        self._queue.put_nowait((-priority, next(self._counter), url))
        logger.debug("В очередь: %s (глубина=%d, приоритет=%d)", url, depth, priority)
        return True

    async def get_next(self) -> str | None:
        try:
            *_, url = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        self._pending.discard(url)
        return url

    async def wait_next(self) -> str:
        *_, url = await self._queue.get()
        self._pending.discard(url)
        return url

    def task_done(self) -> None:
        self._queue.task_done()

    async def wait_all(self) -> None:
        await self._queue.join()

    def mark_processed(self, url: str) -> None:
        self._processed.add(url)

    def mark_failed(self, url: str, error: str) -> None:
        self._failed[url] = error

    def get_depth(self, url: str) -> int:
        return self._depths.get(url, 0)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def get_stats(self) -> dict:
        return {
            "total": len(self._known),
            "pending": len(self._pending),
            "processed": len(self._processed),
            "failed": len(self._failed),
        }
