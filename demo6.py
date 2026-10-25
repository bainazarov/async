import asyncio
import csv
import json
import logging

from aiohttp import web

from crawler import (AsyncCrawler, CSVStorage, JSONStorage, MultiStorage,
                     SQLiteStorage)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

PAGES = {
    "/": ("Главная", ["/about", "/products"]),
    "/about": ("О нас", ["/contacts"]),
    "/products": ("Продукты", ["/contacts"]),
    "/contacts": ("Контакты", []),
}


async def handle_page(request: web.Request) -> web.Response:
    title, links = PAGES.get(request.path, ("404", []))
    anchors = " ".join(f"<a href='{href}'>{href}</a>" for href in links)
    html = (f"<html><head><title>{title}</title></head>"
            f"<body><h1>{title}</h1><p>Текст страницы «{title}».</p>{anchors}"
            f"</body></html>")
    return web.Response(text=html, content_type="text/html")


async def start_server() -> tuple[web.AppRunner, int]:
    app = web.Application()
    for path in PAGES:
        app.router.add_get(path, handle_page)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, runner.addresses[0][1]


async def main() -> None:
    runner, port = await start_server()
    base = f"http://127.0.0.1:{port}"
    print(f"\nЛокальный сайт: {base} ({len(PAGES)} страниц)")

    json_storage = JSONStorage("data/pages.json")
    csv_storage = CSVStorage("data/pages.csv")
    db_storage = SQLiteStorage("data/pages.db")
    storage = MultiStorage(json_storage, csv_storage, db_storage)

    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=2,
        storage=storage,
    )

    print("\nКраулим и сохраняем в JSON + CSV + SQLite...")
    try:
        await crawler.crawl(
            start_urls=[base],
            max_pages=10,
            same_domain_only=True,
            progress_interval=None,
        )
    finally:
        await crawler.close()

    print("\nСтатистика хранилищ:")
    for name, stats in storage.get_stats().items():
        print(f"  {name}: сохранено {stats['saved']}, "
              f"ошибок записи {stats['errors']}")

    print("\nЧитаем данные обратно:")

    with open("data/pages.json", encoding="utf-8") as f:
        pages = json.load(f)
    print(f"  JSON: {len(pages)} записей; первая — {pages[0]['title']!r}, "
          f"ссылок: {len(pages[0]['links'])}, статус {pages[0]['status_code']}")

    with open("data/pages.csv", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    print(f"  CSV:  {len(rows)} строк; колонки: {', '.join(rows[0].keys())}")

    records = await db_storage.read_all()
    print(f"  SQLite: {len(records)} записей")
    for rec in records[:3]:
        print(f"    [{rec['status_code']}] {rec['url']} — {rec['title']!r}")
    await db_storage.close()

    print("\nФайлы: data/pages.json, data/pages.csv, data/pages.db")

    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
