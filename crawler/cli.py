"""Командная строка краулера.

Примеры:
    python -m crawler.cli --urls https://example.com --max-pages 100
    python -m crawler.cli --config config.yaml --output results.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os

from .advanced_crawler import AdvancedCrawler
from .config import load_config
from .logging_setup import setup_logging

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler",
        description="Асинхронный веб-краулер (итоговый проект, день 7)")
    parser.add_argument("--urls", nargs="+", help="стартовые URL")
    parser.add_argument("--max-pages", type=int, help="лимит страниц")
    parser.add_argument("--max-depth", type=int, help="лимит глубины")
    parser.add_argument("--output", help="JSON-файл с результатами (страницами)")
    parser.add_argument("--stats-output", help="JSON-файл со статистикой")
    parser.add_argument("--report", help="HTML-файл с отчётом")
    parser.add_argument("--config", help="YAML/JSON конфигурация")
    parser.add_argument("--respect-robots", action="store_true", default=None,
                        help="соблюдать robots.txt")
    parser.add_argument("--no-robots", dest="respect_robots",
                        action="store_false", help="игнорировать robots.txt")
    parser.add_argument("--rate-limit", type=float,
                        help="запросов в секунду")
    parser.add_argument("--sitemap", action="append", default=None,
                        help="URL sitemap.xml (можно несколько раз)")
    parser.add_argument("--log-file", help="писать логи в файл")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


async def run(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else {}

    if args.urls:
        config["start_urls"] = args.urls
    if args.max_pages is not None:
        config["max_pages"] = args.max_pages
    if args.max_depth is not None:
        config["max_depth"] = args.max_depth
    if args.respect_robots is not None:
        config["respect_robots"] = args.respect_robots
    if args.rate_limit is not None:
        config["requests_per_second"] = args.rate_limit
    if args.sitemap:
        config["sitemaps"] = args.sitemap

    crawler = AdvancedCrawler(config)
    try:
        results = await crawler.crawl()
    finally:
        await crawler.close()

    stats = crawler.get_stats()
    print(f"\nОбработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}, ошибок: {stats['failed']}, "
          f"скорость: {stats['pages_per_second']} стр/с")

    if args.output:
        directory = os.path.dirname(args.output)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Результаты сохранены: {args.output}")
    if args.stats_output:
        await crawler.export_to_json(args.stats_output)
        print(f"Статистика сохранена: {args.stats_output}")
    if args.report:
        await crawler.export_to_html_report(args.report)
        print(f"Отчёт сохранён: {args.report}")
    return 0 if stats["successful"] else 1


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(log_file=args.log_file, level=args.log_level)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
