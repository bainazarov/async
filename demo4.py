import asyncio
import logging

from crawler import AsyncCrawler, RobotsParser

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


async def demo_robots() -> None:
    print("\n" + "=" * 60)
    print("  1. РАЗБОР robots.txt — httpbin.org")
    print("=" * 60)

    robots = RobotsParser()
    try:
        info = await robots.fetch_robots("https://httpbin.org/")
        print(f"  Статус ответа:   {info['status']}")
        print(f"  Crawl-delay:     {info['crawl_delay'] or 'не задан'}")
        print(f"  Разрешает всё:   {info['allows_all']}")
        print()
        print("  Проверка can_fetch для AsyncCrawler/1.0:")
        for path in ("/", "/get", "/deny", "/redirect/1"):
            allowed = robots.can_fetch(f"https://httpbin.org{path}", "AsyncCrawler/1.0")
            print(f"    {'✓ разрешено' if allowed else '✗ ЗАПРЕЩЕНО'}  {path}")
    finally:
        await robots.close()


async def demo_crawl() -> None:
    print("\n" + "=" * 60)
    print("  2. КРАУЛИНГ С ПРАВИЛАМИ ВЕЖЛИВОСТИ — httpbin.org")
    print("=" * 60)
    print("  Лимит: 2 запроса/сек, мин. задержка 0.5с, robots.txt пропускается\n")

    crawler = AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,
        min_delay=0.5,
        max_per_domain=2,
        respect_robots=True,
        user_agent="AsyncCrawler/1.0",
        max_depth=1,
    )
    try:
        await crawler.crawl(
            start_urls=[
                "https://httpbin.org/",
                "https://httpbin.org/ip",
                "https://httpbin.org/headers",
                "https://httpbin.org/user-agent",
                "https://httpbin.org/get",
                "https://httpbin.org/uuid",
                "https://httpbin.org/deny",
            ],
            max_pages=10,
            same_domain_only=True,
        )

        stats = crawler.get_rate_stats()
        print("\n  Статистика по скорости:")
        print(f"    Запросов (с повторами):          {stats['requests']}")
        print(f"    Средняя задержка между запросами: {stats['avg_delay']}с")
        print(f"    Средняя скорость:                 {stats['requests_per_sec']} зап/с")
        print(f"    Заблокировано robots.txt:         {stats['blocked_by_robots']}")
        for url in sorted(crawler.robots_blocked):
            print(f"      ✗ {url}")
        print(f"    Ожиданий у лимитера: {stats['limiter']['waits']} "
              f"(суммарно {stats['limiter']['total_wait']}с)")
    finally:
        await crawler.close()


async def main() -> None:
    await demo_robots()
    await demo_crawl()


if __name__ == "__main__":
    asyncio.run(main())
