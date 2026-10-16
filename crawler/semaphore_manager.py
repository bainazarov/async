import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class SemaphoreManager:

    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 2) -> None:
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains: dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()
        self.active_total = 0
        self.active_by_domain: dict[str, int] = {}

    async def _domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        async with self._lock:
            sem = self._domains.get(domain)
            if sem is None:
                sem = self._domains[domain] = asyncio.Semaphore(self.max_per_domain)
            return sem

    @asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[None]:
        domain = urlparse(url).netloc
        domain_sem = await self._domain_semaphore(domain)

        await self._global.acquire()
        await domain_sem.acquire()
        self.active_total += 1
        self.active_by_domain[domain] = self.active_by_domain.get(domain, 0) + 1
        logger.debug("Захват слота: %s (активно запросов: %d)", url, self.active_total)
        try:
            yield
        finally:
            self.active_by_domain[domain] -= 1
            self.active_total -= 1
            domain_sem.release()
            self._global.release()

    def get_stats(self) -> dict:
        return {
            "active_total": self.active_total,
            "active_by_domain": dict(self.active_by_domain),
            "domains": len(self._domains),
        }
