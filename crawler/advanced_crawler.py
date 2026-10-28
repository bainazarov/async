from __future__ import annotations

import asyncio
import logging

from .async_crawler import AsyncCrawler
from .config import load_config
from .monitoring import ProgressMonitor
from .sitemap import SitemapParser
from .stats import CrawlerStats
from .storage import (CSVStorage, DataStorage, JSONStorage, MultiStorage,
                      SQLiteStorage)

logger = logging.getLogger(__name__)

_CRAWLER_KEYS = (
    "max_concurrent", "max_depth", "same_domain_only", "respect_robots",
    "requests_per_second", "max_per_domain", "min_delay", "max_retries",
    "retry_base_delay", "connect_timeout", "read_timeout", "total_timeout",
)


class AdvancedCrawler:

    def __init__(self, config: dict | None = None, **overrides):
        self.config: dict = {**(config or {}), **overrides}
        self.stats = CrawlerStats()
        self.crawler = self._build_crawler()
        self._sitemaps = SitemapParser()
        self._monitor = ProgressMonitor(
            interval=self.config.get("progress_interval", 1.0))

    @classmethod
    def from_config(cls, path: str) -> "AdvancedCrawler":
        return cls(load_config(path))


    def _build_crawler(self) -> AsyncCrawler:
        kwargs = {key: self.config[key]
                  for key in _CRAWLER_KEYS if key in self.config}
        return AsyncCrawler(storage=self._build_storage(),
                            stats=self.stats, **kwargs)

    def _build_storage(self) -> DataStorage | None:
        settings = self.config.get("storage") or {}
        storages: list[DataStorage] = []
        if settings.get("json"):
            storages.append(JSONStorage(settings["json"]))
        if settings.get("csv"):
            storages.append(CSVStorage(settings["csv"]))
        if settings.get("sqlite"):
            storages.append(SQLiteStorage(settings["sqlite"]))
        if not storages:
            return None
        return storages[0] if len(storages) == 1 else MultiStorage(*storages)


    async def _collect_urls(self, urls: list[str] | None) -> list[str]:
        result = list(urls or self.config.get("start_urls") or [])

        sitemap_urls = list(self.config.get("sitemaps") or [])
        if self.config.get("use_sitemaps"):
            for url in result:
                sitemap_urls.extend(await self._sitemaps.urls_from_robots(url))
        for sitemap in sitemap_urls:
            result.extend(await self._sitemaps.fetch_sitemap(sitemap))

        return list(dict.fromkeys(result))

    async def crawl(self, urls: list[str] | None = None,
                    max_pages: int | None = None,
                    max_depth: int | None = None) -> dict:
        start_urls = await self._collect_urls(urls)
        if not start_urls:
            raise ValueError("Нет стартовых URL: задайте urls или start_urls в конфиге")

        page_limit = max_pages or self.config.get("max_pages", 100)
        depth_limit = max_depth if max_depth is not None \
            else self.config.get("max_depth")

        logger.info("Старт краулинга: %d URL, лимит %d страниц",
                    len(start_urls), page_limit)
        self.stats.start()
        stop = asyncio.Event()
        monitor = asyncio.create_task(
            self._monitor.run(stop, self.stats, page_limit))
        try:
            result = await self.crawler.crawl(
                start_urls=start_urls,
                max_pages=page_limit,
                max_depth=depth_limit,
                same_domain_only=self.config.get("same_domain_only"),
                include_patterns=self.config.get("include_patterns"),
                exclude_patterns=self.config.get("exclude_patterns"),
                progress_interval=None,
            )
        finally:
            self.stats.stop()
            stop.set()
            await monitor
        logger.info("Краулинг завершён: %s", self.stats.get_stats())
        return result


    def get_stats(self) -> dict:
        stats = self.stats.get_stats()
        stats["attempts"] = self.crawler.retry_strategy.stats.to_dict()
        if self.crawler.storage is not None:
            stats["storage"] = self.crawler.storage.get_stats()
        return stats

    async def export_to_json(self, filename: str) -> str:
        return await self.stats.export_to_json(filename)

    async def export_to_html_report(self, filename: str) -> str:
        return await self.stats.export_to_html_report(filename)

    async def close(self) -> None:
        await self.crawler.close()
        await self._sitemaps.close()
