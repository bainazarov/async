from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass, field

from .errors import NetworkError, TransientError

logger = logging.getLogger(__name__)


@dataclass
class RetryStats:
    total_attempts: int = 0
    total_errors: int = 0
    errors_by_type: Counter = field(default_factory=Counter)
    retries_scheduled: int = 0
    successful_retries: int = 0
    retry_durations: list[float] = field(default_factory=list)

    @property
    def avg_retry_time(self) -> float:
        if not self.retry_durations:
            return 0.0
        return sum(self.retry_durations) / len(self.retry_durations)

    def reset(self) -> None:
        self.total_attempts = 0
        self.total_errors = 0
        self.errors_by_type.clear()
        self.retries_scheduled = 0
        self.successful_retries = 0
        self.retry_durations.clear()

    def to_dict(self) -> dict:
        return {
            "total_attempts": self.total_attempts,
            "total_errors": self.total_errors,
            "errors_by_type": dict(self.errors_by_type),
            "retries_scheduled": self.retries_scheduled,
            "successful_retries": self.successful_retries,
            "avg_retry_time": round(self.avg_retry_time, 3),
        }


class RetryStrategy:

    def __init__(
            self,
            max_retries: int = 3,
            backoff_factor: float = 2.0,
            retry_on: list[type[BaseException]] | None = None,
            base_delay: float = 1.0,
            max_delay: float = 60.0,
            jitter: float = 0.0,
            retry_limits: dict[type[BaseException], int] | None = None,
            backoff_factors: dict[type[BaseException], float] | None = None,
            attempt_kwarg: str | None = None,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on: tuple = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.retry_limits = dict(retry_limits or {})
        self.backoff_factors = dict(backoff_factors or {})
        self.attempt_kwarg = attempt_kwarg
        self.stats = RetryStats()

    def is_retryable(self, exc: BaseException) -> bool:
        return isinstance(exc, self.retry_on)

    def _match(self, mapping: dict, exc: BaseException):
        for klass, value in mapping.items():
            if isinstance(exc, klass):
                return value
        return None

    def limit_for(self, exc: BaseException) -> int:
        return self._match(self.retry_limits, exc) or self.max_retries

    def delay_for(self, exc: BaseException, retry_number: int) -> float:
        factor = self._match(self.backoff_factors, exc) or self.backoff_factor
        delay = self.base_delay * (factor ** retry_number)
        suggested = getattr(exc, "suggested_delay", None)
        if suggested:
            delay = max(delay, suggested)
        delay = min(delay, self.max_delay)
        if self.jitter:
            delay += random.uniform(0.0, self.jitter * delay)
        return delay

    async def execute_with_retry(self, coro, *args, **kwargs):
        retries_used: dict[type, int] = {}
        attempt = 0
        started_at = time.perf_counter()

        while True:
            self.stats.total_attempts += 1
            try:
                if self.attempt_kwarg:
                    kwargs[self.attempt_kwarg] = attempt
                result = await coro(*args, **kwargs)
            except Exception as exc:
                self.stats.total_errors += 1
                self.stats.errors_by_type[type(exc).__name__] += 1

                if not self.is_retryable(exc):
                    logger.error("Ошибка не подлежит повтору: %s: %s",
                                 type(exc).__name__, exc)
                    raise

                kind = type(exc)
                used = retries_used.get(kind, 0)
                if used >= self.limit_for(exc):
                    logger.error("Лимит повторов (%d) исчерпан для %s: %s",
                                 used, type(exc).__name__, exc)
                    raise

                delay = self.delay_for(exc, used)
                retries_used[kind] = used + 1
                attempt += 1
                self.stats.retries_scheduled += 1
                logger.warning(
                    "Попытка %d не удалась — %s: %s. "
                    "Повтор %d/%d через %.2fс",
                    attempt, type(exc).__name__, exc, used + 1,
                    self.limit_for(exc), delay)
                await asyncio.sleep(delay)
            else:
                if attempt > 0:
                    self.stats.successful_retries += 1
                    self.stats.retry_durations.append(time.perf_counter() - started_at)
                    logger.info("Успех после %d повторов (%.2fс)", attempt,
                                self.stats.retry_durations[-1])
                return result
