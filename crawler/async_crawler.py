import asyncio
import logging
import aiohttp
from aiohttp import ClientSession

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None

    def _get_session(self) -> ClientSession | None:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(connect=5, total=30)
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
        return self._session

    async def fetch_url(self, url: str) -> str:
        logger.info("Начало загрузки: %s", url)
        session = self._get_session()

        async with self._semaphore:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    html = await response.text()
                    logger.info("Успешно загружено: %s [%d] (%d байт)",
                                url, response.status, len(html))
                    return html

            except aiohttp.ClientResponseError as e:
                logger.warning("HTTP ошибка %s: %s — %s", e.status, url, e.message)
            except asyncio.TimeoutError:
                logger.warning("Таймаут: %s", url)
            except aiohttp.ClientError as e:
                logger.warning("Сетевая ошибка %s: %s", url, e)

        return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return dict(zip(urls, results))

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия закрыта")
