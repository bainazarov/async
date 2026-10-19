import asyncio
import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
from aiohttp import ClientSession

logger = logging.getLogger(__name__)


class RobotsParser:

    def __init__(self, timeout: float = 10.0):
        self._timeout = timeout
        self._session: aiohttp.ClientSession | None = None
        self._rules: dict[str, RobotFileParser] = {}
        self._summaries: dict[str, dict] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    @staticmethod
    def _domain_key(url: str) -> str:
        parsed = urlparse(url if "//" in url else f"https://{url}")
        return f"{parsed.scheme}://{parsed.netloc}"

    async def _get_session(self) -> ClientSession | None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout)
            )
        return self._session

    async def _fetch_text(self, robots_url: str) -> tuple[str, int]:
        session = await self._get_session()
        async with session.get(robots_url) as response:
            return await response.text(), response.status

    async def fetch_robots(self, base_url: str) -> dict:
        key = self._domain_key(base_url)
        async with self._locks_guard:
            lock = self._locks.setdefault(key, asyncio.Lock())

        async with lock:
            if key in self._summaries:
                return self._summaries[key]

            parser = RobotFileParser()
            text, status = "", None
            try:
                text, status = await self._fetch_text(f"{key}/robots.txt")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning("robots.txt недоступен для %s: %s — разрешаем обход", key, e)

            if status is None or status == 404:
                parser.parse([])
            elif status in (401, 403) or status >= 500:
                parser.parse(["User-agent: *", "Disallow: /"])
            else:
                parser.parse(text.splitlines())

            delay = parser.crawl_delay("*")
            summary = {
                "domain": key,
                "status": status,
                "crawl_delay": float(delay) if delay else None,
                "allows_all": status is None or status == 404,
            }
            self._rules[key] = parser
            self._summaries[key] = summary
            logger.info("robots.txt %s: статус=%s, crawl-delay=%s",
                        key, status, summary["crawl_delay"])
            return summary

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parser = self._rules.get(self._domain_key(url))
        if parser is None:
            logger.debug("robots для %s не загружен — разрешаем", self._domain_key(url))
            return True
        return parser.can_fetch(user_agent, url)

    def get_crawl_delay(self, user_agent: str = "*", url: str | None = None) -> float:
        if url is None:
            return 0.0
        parser = self._rules.get(self._domain_key(url))
        if parser is None:
            return 0.0
        delay = parser.crawl_delay(user_agent)
        return float(delay) if delay else 0.0

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия robots закрыта")
