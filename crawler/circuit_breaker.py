from __future__ import annotations

import asyncio
import logging
import time

from .errors import CircuitOpenError

logger = logging.getLogger(__name__)


class CircuitBreaker:

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def is_open(self, domain: str) -> bool:
        async with self._lock:
            opened_at = self._opened_at.get(domain)
            if opened_at is None:
                return False
            if time.monotonic() - opened_at >= self.recovery_timeout:
                del self._opened_at[domain]
                self._failures[domain] = 0
                logger.info("Circuit breaker для %s полуоткрыт — пробная попытка", domain)
                return False
            return True

    async def raise_if_open(self, domain: str) -> None:
        if await self.is_open(domain):
            raise CircuitOpenError(f"circuit breaker открыт для {domain}", url=domain)

    async def record_success(self, domain: str) -> None:
        async with self._lock:
            if self._failures.get(domain):
                self._failures[domain] = 0
                logger.info("Circuit breaker для %s сброшен после успеха", domain)

    async def record_failure(self, domain: str) -> None:
        async with self._lock:
            self._failures[domain] = self._failures.get(domain, 0) + 1
            if (self._failures[domain] >= self.failure_threshold
                    and domain not in self._opened_at):
                self._opened_at[domain] = time.monotonic()
                logger.warning(
                    "Circuit breaker ОТКРЫТ для %s на %.0fс (%d ошибок подряд)",
                    domain, self.recovery_timeout, self._failures[domain])

    async def reset(self) -> None:
        async with self._lock:
            self._failures.clear()
            self._opened_at.clear()

    def get_stats(self) -> dict:
        return {
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "open_circuits": sorted(self._opened_at),
            "consecutive_failures": dict(self._failures),
        }
