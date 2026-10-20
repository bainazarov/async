import aiohttp
import asyncio
import logging
import random
import re
import time
from aiohttp import ClientSession
from collections import deque
from urllib.parse import ParseResult, urlparse, urlunparse

from .html_parser import HTMLParser
from .queue import CrawlerQueue
from .rate_limiter import RateLimiter
from .robots_parser import RobotsParser
from .semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)

_BINARY_EXTENSIONS = (
    ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".ico", ".svg",
    ".css", ".js", ".mjs", ".mp3", ".wav", ".mp4", ".avi", ".mov",
)

DEFAULT_USER_AGENT = "AsyncCrawler/1.0"

class AsyncCrawler:
    def __init__(
            self,
            max_concurrent: int = 10,
            max_depth: int = 2,
            same_domain_only: bool = False,
            exclude_patterns: list[str] | None = None,
            include_patterns: list[str] | None = None,
            max_per_domain: int = 4,
            requests_per_second: float | None = None,
            per_domain_limit: bool = True,
            min_delay: float = 0.0,
            jitter: float = 0.0,
            respect_robots: bool = True,
            user_agent: str = DEFAULT_USER_AGENT,
            user_agents: list[str] | None = None,
            max_retries: int = 2,
            retry_base_delay: float = 1.0,
    ):
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        self.max_per_domain = max_per_domain

        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.user_agents = user_agents
        self.max_retries = max_retries

        self._sem_manager = SemaphoreManager(max_concurrent, max_per_domain)
        self._limiter = RateLimiter(
            requests_per_second,
            per_domain=per_domain_limit,
            min_delay=min_delay,
            jitter=jitter,
            base_backoff=retry_base_delay,
        )
        self._robots = RobotsParser()
        self._session: aiohttp.ClientSession | None = None
        self._parser = HTMLParser()

        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.robots_blocked: set[str] = set()
        self._start_domains: set[str] = set()
        self._pages_fetched = 0
        self._pages_lock = asyncio.Lock()
        self._request_times: deque[float] = deque()

    def _get_session(self) -> ClientSession | None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(connect=5, total=30)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.user_agent},
            )
        return self._session

    def _pick_ua(self) -> str:
        if self.user_agents:
            return random.choice(self.user_agents)
        return self.user_agent

    def _record_request(self) -> None:
        self._request_times.append(time.perf_counter())

    async def _robots_allows(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        await self._robots.fetch_robots(url)
        if not self._robots.can_fetch(url, self._pick_ua()):
            logger.warning("robots.txt запрещает: %s", url)
            self.robots_blocked.add(url)
            return False
        delay = self._robots.get_crawl_delay(self._pick_ua(), url=url)
        if delay:
            self._limiter.set_domain_min_interval(urlparse(url).netloc, delay)
        return True

    async def fetch_url(self, url: str) -> str:
        logger.info("Начало загрузки: %s", url)
        session = self._get_session()
        domain = urlparse(url).netloc

        if not await self._robots_allows(url):
            return ""

        for attempt in range(self.max_retries + 1):
            await self._limiter.acquire(domain)
            self._record_request()
            headers = {"User-Agent": self._pick_ua()} if self.user_agents else None
            try:
                async with self._sem_manager.acquire(url):
                    async with session.get(url, headers=headers) as response:
                        response.raise_for_status()
                        html = await response.text()
                self._limiter.reset_backoff(domain)
                logger.info("Успешно загружено: %s [%d] (%d байт)",
                            url, response.status, len(html))
                return html

            except aiohttp.ClientResponseError as e:
                if e.status in (408, 429) or e.status >= 500:
                    if attempt < self.max_retries:
                        logger.warning("HTTP %s, повтор %d/%d: %s",
                                       e.status, attempt + 1, self.max_retries, url)
                        self._limiter.penalize(domain)
                        continue
                logger.warning("HTTP ошибка %s: %s — %s", e.status, url, e.message)
                return ""

            except asyncio.TimeoutError:
                if attempt < self.max_retries:
                    logger.warning("Таймаут, повтор %d/%d: %s",
                                   attempt + 1, self.max_retries, url)
                    self._limiter.penalize(domain)
                    continue
                logger.warning("Таймаут после %d попыток: %s", self.max_retries + 1, url)
                return ""

            except aiohttp.ClientError as e:
                if attempt < self.max_retries:
                    logger.warning("Сетевая ошибка, повтор %d/%d: %s — %s",
                                   attempt + 1, self.max_retries, url, e)
                    self._limiter.penalize(domain)
                    continue
                logger.warning("Сетевая ошибка %s: %s", url, e)
                return ""

        return ""

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        return await self._parser.parse_html(html, url)

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return dict(zip(urls, results))

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            *,
            max_depth: int | None = None,
            same_domain_only: bool | None = None,
            exclude_patterns: list[str] | None = None,
            include_patterns: list[str] | None = None,
            progress_interval: float | None = 1.0,
    ) -> dict[str, dict]:

        depth_limit = self.max_depth if max_depth is None else max_depth
        same_domain = self.same_domain_only if same_domain_only is None else same_domain_only
        excludes = ([re.compile(p) for p in exclude_patterns]
                    if exclude_patterns is not None else self.exclude_patterns)
        includes = ([re.compile(p) for p in include_patterns]
                    if include_patterns is not None else self.include_patterns)

        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()
        self.robots_blocked.clear()
        self._pages_fetched = 0
        self._request_times.clear()
        self._start_domains = {self._host(u) for u in start_urls}

        queue = CrawlerQueue()
        for url in start_urls:
            normalized = self._normalize_url(url)
            if normalized:
                queue.add_url(normalized, depth=0)
            else:
                logger.warning("Некорректный стартовый URL: %s", url)

        if max_pages < 1 or queue.pending_count == 0:
            return self.processed_urls

        start_time = time.perf_counter()
        stop_report = asyncio.Event()
        workers = [
            asyncio.create_task(
                self._worker(queue, max_pages, depth_limit, same_domain, includes, excludes)
            )
            for _ in range(min(self.max_concurrent, max_pages))
        ]
        reporter = None
        if progress_interval:
            reporter = asyncio.create_task(
                self._report_progress(queue, max_pages, progress_interval, start_time, stop_report)
            )

        try:
            await queue.wait_all()
        finally:
            stop_report.set()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            if reporter:
                await reporter

        elapsed = time.perf_counter() - start_time
        done = len(self.processed_urls)
        rate = done / elapsed if elapsed > 0 else 0
        print(f"\nГотово: обработано {done} страниц, "
              f"ошибок {len(self.failed_urls)}, за {elapsed:.1f}с ({rate:.1f} стр/с)")
        return self.processed_urls

    async def _worker(
            self,
            queue: CrawlerQueue,
            max_pages: int,
            max_depth: int,
            same_domain: bool,
            includes: list[re.Pattern],
            excludes: list[re.Pattern],
    ) -> None:
        while True:
            url = await queue.wait_next()
            try:
                if not await self._robots_allows(url):
                    self.failed_urls[url] = "blocked by robots.txt"
                    queue.mark_failed(url, "blocked by robots.txt")
                    continue

                depth = queue.get_depth(url)

                async with self._pages_lock:
                    if self._pages_fetched >= max_pages:
                        continue
                    self.visited_urls.add(url)
                    self._pages_fetched += 1

                result = await self.fetch_and_parse(url)

                if "error" in result:
                    self.failed_urls[url] = result["error"]
                    queue.mark_failed(url, result["error"])
                    continue

                self.processed_urls[url] = result
                queue.mark_processed(url)

                if depth >= max_depth:
                    continue

                for link in result["links"]:
                    normalized = self._normalize_url(link)
                    if (normalized is None
                            or not self._should_follow(normalized, same_domain, includes, excludes)):
                        continue
                    queue.add_url(normalized, depth=depth + 1)
            finally:
                queue.task_done()

    async def _report_progress(
            self,
            queue: CrawlerQueue,
            max_pages: int,
            interval: float,
            start_time: float,
            stop: asyncio.Event,
    ) -> None:
        while True:
            done = len(self.visited_urls)
            errors = len(self.failed_urls)
            elapsed = time.perf_counter() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            print(f"  обработано {done}/{max_pages} | "
                  f"в очереди {queue.pending_count} | "
                  f"ошибок {errors} | robots: {len(self.robots_blocked)} | "
                  f"{rate:.1f} стр/с", flush=True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                continue

    def get_rate_stats(self) -> dict:
        times = list(self._request_times)
        n = len(times)
        elapsed = times[-1] - times[0] if n > 1 else 0.0
        overall_rps = (n - 1) / elapsed if elapsed > 0 else 0.0
        window = 10.0
        recent = sum(1 for t in times if t >= time.perf_counter() - window)
        return {
            "requests": n,
            "avg_delay": round(elapsed / (n - 1), 3) if n > 1 else 0.0,
            "requests_per_sec": round(overall_rps, 2),
            "recent_per_sec": round(recent / window, 2),
            "blocked_by_robots": len(self.robots_blocked),
            "limiter": self._limiter.get_stats(),
        }

    def _should_follow(
            self,
            url: str,
            same_domain: bool,
            includes: list[re.Pattern],
            excludes: list[re.Pattern],
    ) -> bool:
        if same_domain and self._host(url) not in self._start_domains:
            return False
        if includes and not any(p.search(url) for p in includes):
            return False
        if any(p.search(url) for p in excludes):
            return False
        if urlparse(url).path.lower().endswith(_BINARY_EXTENSIONS):
            return False
        return True

    @staticmethod
    def _host(url: str) -> str | bytes:
        return urlparse(url).netloc.lower().removeprefix("www.")

    @staticmethod
    def _normalize_url(url: str) -> str | None:
        try:
            parsed: ParseResult[str] = urlparse(url.strip())
        except ValueError:
            return None
        if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc:
            return None
        path = parsed.path or "/"
        return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "",
                           parsed.query, ""))

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия закрыта")
        await self._robots.close()
