from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET

import aiohttp

logger = logging.getLogger(__name__)


class SitemapParser:

    def __init__(self, max_depth: int = 3, timeout: float = 10.0):
        self.max_depth = max_depth
        self._timeout = timeout
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout),
                headers={"User-Agent": "AsyncCrawler/1.0"},
            )
        return self._session

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    async def fetch_sitemap(self, sitemap_url: str, _depth: int = 0) -> list[str]:
        if _depth > self.max_depth:
            logger.warning("Превышена глубина sitemap: %s", sitemap_url)
            return []

        session = await self._get_session()
        try:
            async with session.get(sitemap_url) as response:
                response.raise_for_status()
                text = await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Не удалось загрузить sitemap %s: %s", sitemap_url, e)
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            logger.warning("Битый XML в sitemap %s: %s", sitemap_url, e)
            return []

        urls: list[str] = []
        if self._local_name(root.tag) == "sitemapindex":
            for loc in self._child_texts(root, "sitemap", "loc"):
                logger.info("Индексный sitemap %s → %s", sitemap_url, loc)
                urls.extend(await self.fetch_sitemap(loc, _depth + 1))
        else:
            urls = self._child_texts(root, "url", "loc")
            logger.info("Sitemap %s: %d URL", sitemap_url, len(urls))
        return urls

    def _child_texts(self, root: ET.Element, child_tag: str,
                     field_tag: str) -> list[str]:
        result = []
        for child in root:
            if self._local_name(child.tag) != child_tag:
                continue
            for field in child:
                if self._local_name(field.tag) == field_tag and field.text:
                    result.append(field.text.strip())
        return result

    async def urls_from_robots(self, site_url: str) -> list[str]:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(site_url)
        robots_url = urlunparse((parsed.scheme, parsed.netloc, "/robots.txt",
                                 "", "", ""))
        session = await self._get_session()
        try:
            async with session.get(robots_url) as response:
                if response.status != 200:
                    return []
                text = await response.text()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return []
        sitemaps = []
        for line in text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
        return sitemaps

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
