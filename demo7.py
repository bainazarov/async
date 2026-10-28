
import asyncio
import os
import sys

import yaml
from aiohttp import web

from crawler import AdvancedCrawler
from crawler.logging_setup import setup_logging

setup_logging(level="WARNING")

PAGES = {
    "/": "Главная",
    "/about": "О нас",
    "/blog": "Блог",
    "/docs": "Документация",
}
LINKS = {
    "/": ["/about", "/blog"],
    "/about": ["/docs"],
    "/blog": ["/docs"],
    "/docs": [],
}


async def handle_page(request: web.Request) -> web.Response:
    anchors = " ".join(f"<a href='{href}'>{href}</a>"
                       for href in LINKS.get(request.path, []))
    title = PAGES.get(request.path, "?")
    return web.Response(
        text=f"<html><head><title>{title}</title></head>"
             f"<body><h1>{title}</h1>{anchors}</body></html>",
        content_type="text/html")


async def handle_sitemap(request: web.Request) -> web.Response:
    base = f"http://{request.host}"
    urls = "".join(f"<url><loc>{base}{path}</loc></url>" for path in PAGES)
    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
           f"{urls}</urlset>")
    return web.Response(text=xml, content_type="application/xml")


async def handle_sitemap_index(request: web.Request) -> web.Response:
    base = f"http://{request.host}"
    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
           f"<sitemap><loc>{base}/sitemap.xml</loc></sitemap>"
           "</sitemapindex>")
    return web.Response(text=xml, content_type="application/xml")


async def demo_advanced_crawler(base: str) -> None:
    print("\n" + "=" * 60)
    print("  1. AdvancedCrawler + YAML-конфиг + sitemap")
    print("=" * 60)

    config = {
        "start_urls": [base],
        "sitemaps": [f"{base}/sitemap-index.xml"],
        "max_pages": 10,
        "max_depth": 2,
        "max_concurrent": 5,
        "respect_robots": True,
        "storage": {"json": "data/demo7_pages.json",
                    "sqlite": "data/demo7_pages.db"},
        "progress_interval": 0.5,
    }
    cfg_path = "data/config.demo.yaml"
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, allow_unicode=True)
    print(f"  Конфиг записан: {cfg_path}")

    crawler = AdvancedCrawler.from_config(cfg_path)
    try:
        print("  Краулим (следите за прогресс-баром):")
        await crawler.crawl()
    finally:
        await crawler.close()

    stats = crawler.get_stats()
    print(f"\n  Обработано: {stats['total_pages']} "
          f"(успешно {stats['successful']}, ошибок {stats['failed']})")
    print(f"  Скорость: {stats['pages_per_second']} стр/с, "
          f"время: {stats['elapsed_seconds']}с")
    print(f"  Статус-коды: {stats['status_codes']}")
    print(f"  Топ доменов: {stats['top_domains']}")

    await crawler.export_to_json("data/demo7_stats.json")
    await crawler.export_to_html_report("data/demo7_report.html")
    print("\n  Экспорт: data/demo7_stats.json, data/demo7_report.html")


async def demo_cli(cfg_path: str) -> None:
    print("\n" + "=" * 60)
    print("  2. Тот же прогон через CLI")
    print("=" * 60)
    cmd = [sys.executable, "-m", "crawler.cli",
           "--config", cfg_path,
           "--max-pages", "2",
           "--output", "data/cli_pages.json",
           "--stats-output", "data/cli_stats.json"]
    print(f"  $ {' '.join(cmd[1:])}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=os.getcwd())
    output, _ = await proc.communicate()
    print("  " + output.decode(encoding="utf-8",
                               errors="replace").strip().replace("\n", "\n  "))


async def main() -> None:
    app = web.Application()
    for path in PAGES:
        app.router.add_get(path, handle_page)
    app.router.add_get("/sitemap.xml", handle_sitemap)
    app.router.add_get("/sitemap-index.xml", handle_sitemap_index)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]

    try:
        await demo_advanced_crawler(f"http://127.0.0.1:{port}")
        await demo_cli("data/config.demo.yaml")
        print("\nГотово. Откройте data/demo7_report.html в браузере.")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
