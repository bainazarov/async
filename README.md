# AsyncCrawler — асинхронный веб-краулер

Итоговый проект недельного курса «Параллельность и асинхронность» на Python.
Краулер построен на `asyncio` + `aiohttp` и собирает страницы параллельно
с соблюдением правил вежливости, повторами при сбоях, сохранением результатов
и подробной статистикой.

## Возможности

- Параллельная загрузка с ограничением конкурентности (семафоры, воркеры)
- Очередь URL с приоритетами и обходом в ширину (глубина ограничивается)
- Соблюдение robots.txt (включая Crawl-delay) и лимитов запросов (rate limiter)
- Классификация ошибок и автоматические повторы с экспоненциальным backoff
  (таймауты растут с каждой попыткой, `Retry-After` у 429 учитывается)
- Circuit breaker: домен с потоком ошибок временно блокируется
- Сохранение в JSON / CSV / SQLite одновременно (асинхронно, батчами)
- sitemap.xml: обычные и индексные (рекурсивно), поиск в robots.txt
- Статистика прогона + экспорт в JSON и HTML-отчёт
- YAML/JSON-конфигурация, CLI, логирование в файл с ротацией, прогресс-бар

## Установка

```bash
pip install -r requirements.txt
```

## Быстрый старт (Python API)

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    crawler = AdvancedCrawler.from_config("config.example.yaml")
    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}, ошибок: {stats['failed']}")

    await crawler.export_to_json("data/stats.json")
    await crawler.export_to_html_report("data/report.html")
    await crawler.close()

asyncio.run(main())
```

Минимальный вариант без конфига:

```python
from crawler import AsyncCrawler

crawler = AsyncCrawler(max_concurrent=10, max_depth=2)
await crawler.crawl(start_urls=["https://example.com"], max_pages=50)
await crawler.close()
```

## Командная строка

```bash
# через конфиг
python -m crawler.cli --config config.example.yaml

# напрямую аргументами
python -m crawler.cli --urls https://example.com --max-pages 100 \
    --max-depth 2 --rate-limit 2 --respect-robots \
    --output results.json --report report.html

# с логом в файл
python -m crawler.cli --config config.yaml --log-file logs/crawler.log
```

Основные параметры: `--urls`, `--max-pages`, `--max-depth`, `--output`
(страницы в JSON), `--stats-output` (статистика в JSON), `--report`
(HTML-отчёт), `--config`, `--respect-robots` / `--no-robots`,
`--rate-limit`, `--sitemap`, `--log-file`, `--log-level`.

## Конфигурация

Форматы YAML и JSON. Пример — в `config.example.yaml`:

```yaml
start_urls:
  - https://example.com
max_pages: 100
max_depth: 2
max_concurrent: 10
respect_robots: true
requests_per_second: 2.0
sitemaps:
  - https://example.com/sitemap.xml
include_patterns: ["/article/"]
exclude_patterns: ["\\.pdf$"]
storage:
  json: data/pages.json
  sqlite: data/pages.db
```

| Ключ | Назначение |
|---|---|
| `start_urls` | стартовые URL |
| `max_pages`, `max_depth` | лимиты объёма и глубины |
| `max_concurrent` | число параллельных воркеров |
| `respect_robots` | соблюдать robots.txt |
| `requests_per_second`, `min_delay` | лимитер скорости |
| `max_retries`, `retry_base_delay` | повторы (дни 5) |
| `sitemaps` | URL sitemap (можно индексные) |
| `use_sitemaps` | искать sitemap в robots.txt |
| `include_patterns` / `exclude_patterns` | фильтры ссылок (regex) |
| `storage` | куда сохранять: `json`, `csv`, `sqlite` |

## Демонстрации и бенчмарк

```bash
python demo7.py        # sitemap + конфиг + статистика + CLI (офлайн, локальный сервер)
python benchmark.py    # синхронный vs асинхронный краулер, время и память
```

`benchmark.py` поднимает локальный сервер и сравнивает последовательную
загрузку (urllib) с асинхронным краулером; опция `--delay` имитирует
сетевую задержку, `--sizes` — объёмы (например `100 500 1000`).

## Структура проекта

```
crawler/
  async_crawler.py    ядро: воркеры, очередь, загрузка, интеграция
  errors.py           классификация ошибок (Transient/Permanent/Network/Parse)
  retry.py            RetryStrategy: повторы + экспоненциальный backoff
  circuit_breaker.py  блокировка «болеющих» доменов
  rate_limiter.py     лимит запросов и штрафы доменам
  robots_parser.py    разбор robots.txt
  sitemap.py          sitemap.xml (в т.ч. индексные)
  html_parser.py      извлечение текста, ссылок, метаданных
  queue.py            очередь URL
  semaphore_manager.py ограничение конкурентности
  storage.py          JSON/CSV/SQLite хранилища
  stats.py            статистика и HTML-отчёт
  monitoring.py       прогресс-бар со скоростью и ETA
  config.py           YAML/JSON конфигурация
  logging_setup.py    логирование (консоль + файл с ротацией)
  advanced_crawler.py финальная интеграция всех компонентов
  cli.py              командная строка
```

## Замечания

- Все операции ввода-вывода асинхронные (`aiohttp`, `aiofiles`, `aiosqlite`),
  event loop никогда не блокируется сетью или диском.
- Ошибки сохранения не прерывают краулинг; ошибки сети повторяются
  по классификации (404 не повторяется, 503 — да).
