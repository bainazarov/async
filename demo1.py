import asyncio
import logging
import time

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

URLS = [
    "https://example.com",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/200",
    "https://httpbin.org/status/404",  # HTTP ошибка
    "https://httpbin.org/status/500",  # HTTP ошибка
    "https://httpbin.org/get",
    "https://this-domain-does-not-exist-xyz.com",  # сетевая ошибка
]


async def run_parallel(urls: list[str]) -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=5)
    start = time.perf_counter()
    results = await crawler.fetch_urls(urls)
    elapsed = time.perf_counter() - start
    await crawler.close()
    return results, elapsed


async def run_sequential(urls: list[str]) -> float:
    crawler = AsyncCrawler(max_concurrent=1)
    start = time.perf_counter()
    for url in urls:
        await crawler.fetch_url(url)
    elapsed = time.perf_counter() - start
    await crawler.close()
    return elapsed


async def main() -> None:
    print("\n" + "=" * 55)
    print("  ПАРАЛЛЕЛЬНАЯ загрузка")
    print("=" * 55)
    results, parallel_time = await run_parallel(URLS)

    success = sum(1 for html in results.values() if html)
    failed = len(results) - success
    print(f"\nИтог: загружено {success}/{len(results)}, "
          f"ошибок {failed}, время {parallel_time:.2f}с")

    print("\n" + "=" * 55)
    print("  ПОСЛЕДОВАТЕЛЬНАЯ загрузка")
    print("=" * 55)
    seq_time = await run_sequential(URLS)
    print(f"\nВремя: {seq_time:.2f}с")

    print("\n" + "=" * 55)
    speedup = seq_time / parallel_time if parallel_time > 0 else 0
    print(f"  Ускорение: {speedup:.1f}x  "
          f"({seq_time:.2f}с → {parallel_time:.2f}с)")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
