"""Бенчмарк: асинхронный краулер против синхронного.

Поднимает локальный сервер с N страниц и гоняет оба варианта:
  - sync  — последовательные запросы через urllib (один поток);
  - async — AsyncCrawler с конкурентными воркерами.

Замеряет время и пиковое потребление памяти (tracemalloc).

Запуск:  python benchmark.py            # размеры 100 и 500
         python benchmark.py --sizes 100 500 1000 --concurrent 20
"""

import argparse
import asyncio
import time
import tracemalloc
import urllib.request

from aiohttp import web

from crawler import AsyncCrawler

PAGE = "<html><body><h1>страница</h1><p>benchmark</p></body></html>"


async def handle(request: web.Request) -> web.Response:
    delay = request.app.get("delay", 0.0)
    if delay:
        await asyncio.sleep(delay)  # имитация сетевой задержки
    return web.Response(text=PAGE, content_type="text/html")


def sync_crawl(base: str, count: int) -> tuple[float, int]:
    # свой opener без прокси: urllib иначе может ходить в интернет
    # через системный прокси Windows и зависать на каждом запросе
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    tracemalloc.start()
    start = time.perf_counter()
    for i in range(count):
        try:
            with opener.open(f"{base}/page{i}", timeout=10) as r:
                r.read()
        except OSError:
            pass
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak


async def async_crawl(base: str, count: int,
                      concurrent: int) -> tuple[float, int]:
    crawler = AsyncCrawler(max_concurrent=concurrent, max_depth=1,
                           respect_robots=False, use_circuit_breaker=False)
    tracemalloc.start()
    start = time.perf_counter()
    await crawler.crawl([f"{base}/page{i}" for i in range(count)],
                        max_pages=count, progress_interval=None)
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    await crawler.close()
    return elapsed, peak


def fmt_mb(num_bytes: int) -> str:
    return f"{num_bytes / 1024 / 1024:.2f} МБ"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", type=int, nargs="+",
                        default=[100, 500])
    parser.add_argument("--concurrent", type=int, default=20)
    parser.add_argument("--delay", type=float, default=0.02,
                        help="симулированная задержка ответа сервера, сек "
                             "(покажет выигрыш async на медленной сети)")
    args = parser.parse_args()

    app = web.Application()
    app["delay"] = args.delay
    app.router.add_get("/page{index:\\d+}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    print(f"Локальный сервер: {base}")

    print(f"\n{'Страниц':>8} | {'sync, сек':>10} | {'async, сек':>10} "
          f"| {'ускорение':>10} | {'пик памяти sync':>16} | {'пик памяти async':>17}")
    print("-" * 84)
    for size in args.sizes:
        # sync-загрузка идёт в отдельном потоке: иначе её блокирующий
        # сокет весь просидит, не дав event loop'у обслужить сервер
        sync_time, sync_peak = await asyncio.to_thread(sync_crawl, base, size)
        async_time, peak = await async_crawl(base, size, args.concurrent)
        speedup = sync_time / async_time if async_time > 0 else 0.0
        print(f"{size:>8} | {sync_time:>10.2f} | {async_time:>10.2f} "
              f"| {speedup:>9.1f}x | {fmt_mb(sync_peak):>16} "
              f"| {fmt_mb(peak):>17}")

    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
