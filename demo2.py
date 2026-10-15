import asyncio
import logging

from crawler import AsyncCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

URLS = [
    "https://example.com",
    "https://python.org",
    "https://this-domain-does-not-exist-xyz.com",
]


def print_result(result: dict) -> None:
    sep = "─" * 55
    print(f"\n{sep}")
    print(f"  URL:         {result['url']}")

    if "error" in result:
        print(f"  Ошибка:      {result['error']}")
        return

    print(f"  Заголовок:   {result['title'] or '—'}")
    print(f"  Описание:    {result['metadata']['description'][:80] or '—'}")
    print(f"  Символов:    {result['text_length']}")
    print(f"  Ссылок:      {result['links_count']}")
    print(f"  Изображений: {result['images_count']}")

    if result["headings"]:
        print("  Заголовки:")
        for h in result["headings"][:5]:
            print(f"    [{h['level']}] {h['text'][:60]}")

    if result["links"]:
        print("  Первые ссылки:")
        for link in result["links"][:5]:
            print(f"    {link}")

    if result["lists"]:
        print(f"  Списков на странице: {len(result['lists'])}")


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=5)

    print("\n" + "=" * 55)
    print("  ПАРАЛЛЕЛЬНЫЙ парсинг страниц")
    print("=" * 55)

    tasks = [crawler.fetch_and_parse(url) for url in URLS]
    results = await asyncio.gather(*tasks)

    for result in results:
        print_result(result)

    print(f"\n{'=' * 55}")
    success = sum(1 for r in results if "error" not in r)
    print(f"  Итог: обработано {success}/{len(results)} страниц")
    print("=" * 55)

    await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
