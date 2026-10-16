import asyncio
import json
import logging
import os

import aiofiles

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# quotes.toscrape.com — песочница, созданная специально для тренировки краулеров
START_URLS = ["https://quotes.toscrape.com/"]
OUTPUT_FILE = "data/results.json"


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=10, max_depth=2, max_per_domain=4)
    try:
        print("\n" + "=" * 55)
        print(f"  КРАУЛИНГ: {', '.join(START_URLS)}")
        print("=" * 55)

        results = await crawler.crawl(
            start_urls=START_URLS,
            max_pages=25,
            same_domain_only=True,
        )

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(results, ensure_ascii=False, indent=2))

        print(f"\nСохранено {len(results)} страниц в {OUTPUT_FILE}")
        for url, page in list(results.items())[:5]:
            print(f"  - {page['title'][:45]!r} ({url}) — ссылок: {page['links_count']}")
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
