import asyncio
import logging
import random

logger = logging.getLogger(__name__)


class RateLimiter:

    _GLOBAL_KEY = "__global__"

    def __init__(
        self,
        requests_per_second: float | None = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        base_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ):
        self.per_domain = per_domain
        rps = requests_per_second or 0.0
        self._base_interval = max(1.0 / rps, min_delay) if rps > 0 else min_delay
        self.jitter = jitter
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self._domain_intervals: dict[str, float] = {}
        self._next_at: dict[str, float] = {}
        self._backoff_until: dict[str, float] = {}
        self._failures: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self.total_requests = 0
        self.total_waited = 0
        self.total_wait = 0.0

    def _key(self, domain: str | None) -> str:
        return domain if (self.per_domain and domain) else self._GLOBAL_KEY

    def _interval(self, key: str) -> float:
        base = self._domain_intervals.get(key, self._base_interval)
        return base + (random.uniform(0, self.jitter) if self.jitter else 0.0)

    async def acquire(self, domain: str | None = None) -> float:
        loop = asyncio.get_running_loop()
        key = self._key(domain)
        async with self._lock:
            now = loop.time()
            ready_at = max(now, self._next_at.get(key, 0.0), self._backoff_until.get(key, 0.0))
            self._next_at[key] = ready_at + self._interval(key)
            wait = ready_at - now
            self.total_requests += 1
            self.total_wait += wait
            if wait > 0:
                self.total_waited += 1
        if wait > 0:
            logger.debug("Rate limiter: ожидание %.2fс (%s)", wait, key)
            await asyncio.sleep(wait)
        return wait

    def penalize(self, domain: str | None = None) -> None:
        key = self._key(domain)
        self._failures[key] = self._failures.get(key, 0) + 1
        delay = min(self.base_backoff * 2 ** (self._failures[key] - 1), self.max_backoff)
        self._backoff_until[key] = asyncio.get_running_loop().time() + delay
        logger.debug("Rate limiter: штраф %.2fс для %s", delay, key)

    def reset_backoff(self, domain: str | None = None) -> None:
        key = self._key(domain)
        self._failures.pop(key, None)
        self._backoff_until.pop(key, None)

    def set_domain_min_interval(self, domain: str, seconds: float) -> None:
        if seconds > self._domain_intervals.get(domain, self._base_interval):
            self._domain_intervals[domain] = seconds
            logger.debug("Rate limiter: мин. интервал для %s = %.2fс", domain, seconds)

    def get_stats(self) -> dict:
        return {
            "requests": self.total_requests,
            "waits": self.total_waited,
            "total_wait": round(self.total_wait, 3),
            "avg_wait": round(self.total_wait / self.total_requests, 4)
                        if self.total_requests else 0.0,
        }
