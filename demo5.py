import asyncio
import logging
from aiohttp import web

from crawler import (AsyncCrawler, RetryStrategy, TransientError)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("demo5")

PAGE = "<html><body><h1>{title}</h1>{links}</body></html>"
LINKS_OK = "<a href='/page2'>page2</a> <a href='/page3'>page3</a>"

_flaky_hits = 0


async def handle_ok(request: web.Request) -> web.Response:
    return web.Response(text=PAGE.format(title="всё хорошо", links=LINKS_OK),
                        content_type="text/html")


async def handle_flaky(request: web.Request) -> web.Response:
    global _flaky_hits
    _flaky_hits += 1
    if _flaky_hits <= 2:
        return web.Response(status=503, text="Service Unavailable")
    return web.Response(text=PAGE.format(title="flaky: теперь отвечаю!",
                                         links=""), content_type="text/html")


async def handle_slow(request: web.Request) -> web.Response:
    await asyncio.sleep(2.5)
    return web.Response(text=PAGE.format(title="медленный, но живой",
                                         links=""), content_type="text/html")


async def handle_status(request: web.Request) -> web.Response:
    status = int(request.match_info["status"])
    headers = {"Retry-After": "1"} if status == 429 else None
    return web.Response(status=status, text=f"status {status}", headers=headers)


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_ok)
    app.router.add_get("/page2", handle_ok)
    app.router.add_get("/page3", handle_ok)
    app.router.add_get("/flaky", handle_flaky)
    app.router.add_get("/slow", handle_slow)
    app.router.add_get("/status/{status:\\d+}", handle_status)
    return app


async def start_server() -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(make_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    logger.info("Локальный сервер ошибок: порт %d", port)
    return runner, port


async def demo_retry_strategy() -> None:
    print("\n" + "=" * 60)
    print("  1. RetryStrategy — повторы с экспоненциальным backoff")
    print("=" * 60)

    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
        base_delay=0.5,
        retry_on=[TransientError],
    )

    calls = {"n": 0}

    async def unstable_server() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError(f"сервер временно недоступен (вызов {calls['n']})")
        return "данные получены"

    print("  Вызываем корутину, которая падает 2 раза, потом работает...")
    result = await strategy.execute_with_retry(unstable_server)
    print(f"  Результат: {result}")

    stats = strategy.stats.to_dict()
    print(f"  Всего попыток:          {stats['total_attempts']}")
    print(f"  Повторов:               {stats['retries_scheduled']}")
    print(f"  Успешных повторов:      {stats['successful_retries']}")
    print(f"  Ошибки по типам:        {stats['errors_by_type']}")
    print(f"  Среднее время повтора:  {stats['avg_retry_time']}с")


def print_error_stats(stats: dict) -> None:
    print(f"  Всего попыток:           {stats['total_attempts']}")
    print(f"  Ошибок всего:            {stats['total_errors']}")
    print(f"  Повторов запланировано:  {stats['retries_scheduled']}")
    print(f"  Успешных повторов:       {stats['successful_retries']}")
    print(f"  Среднее время повтора:   {stats['avg_retry_time']}с")
    print("  Ошибки по типам:")
    for name, count in stats["errors_by_type"].items():
        print(f"    {name}: {count}")
    print("  Постоянные ошибки (повторы не помогли бы):")
    for url, err in stats["permanent_errors"].items():
        print(f"    {url} — {err}")
    circuit = stats["circuit_breaker"]
    if circuit:
        opened = circuit["open_circuits"]
        print(f"  Circuit breaker: открыт для {opened or 'никого'}")


async def demo_crawler(port: int) -> None:
    print("\n" + "=" * 60)
    print("  2. Краулер против сервера с ошибками")
    print("=" * 60)

    sick = f"http://127.0.0.1:{port}"
    healthy = f"http://localhost:{port}"

    crawler = AsyncCrawler(
        max_concurrent=10,
        max_depth=1,
        max_retries=2,
        retry_base_delay=0.5,
        read_timeout=10.0,
        total_timeout=2.0,
        timeout_growth=1.5,
        circuit_failure_threshold=3,
        circuit_recovery_timeout=60.0,
        limiter_max_backoff=1.0,
    )

    try:
        await crawler.crawl(
            start_urls=[
                f"{sick}/status/500",
                f"{sick}/flaky",
                f"{sick}/slow",
                f"{sick}/status/404",
                f"{sick}/status/403",
                f"{sick}/status/429",
                f"{healthy}/",
                f"{healthy}/page2",
            ],
            max_pages=8,
            progress_interval=None,
        )

        print("\n  Статистика ошибок краулера:")
        print_error_stats(crawler.get_error_stats())

        print("\n  Circuit breaker: пробуем ещё один URL больного хоста...")
        html = await crawler.fetch_url(f"{sick}/flaky")
        if html:
            print("  Сервер ответил (circuit закрыт)")
        else:
            print("  Запрос отклонён мгновенно — circuit breaker открыт, "
                  "сервер не долбим")

        report_path = await crawler.save_error_report("data/error_report.json")
        print(f"\n  Отчёт об ошибках сохранён: {report_path}")
    finally:
        await crawler.close()


async def main() -> None:
    runner, port = await start_server()
    try:
        await demo_retry_strategy()
        await demo_crawler(port)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
