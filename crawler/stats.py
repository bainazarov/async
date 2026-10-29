from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter
from urllib.parse import urlparse

import aiofiles

logger = logging.getLogger(__name__)


class CrawlerStats:

    def __init__(self, top_domains: int = 10):
        self._top = top_domains
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.successful = 0
        self.failed = 0
        self.by_status: Counter = Counter()
        self.by_domain: Counter = Counter()

    def start(self) -> None:
        self.started_at = time.perf_counter()

    def stop(self) -> None:
        self.finished_at = time.perf_counter()

    def record_page(self, url: str, status_code: int) -> None:
        self.successful += 1
        self.by_status[status_code] += 1
        self.by_domain[urlparse(url).netloc] += 1

    def record_failure(self, url: str, status_code: int | None = None) -> None:
        self.failed += 1
        if status_code:
            self.by_status[status_code] += 1
        self.by_domain[urlparse(url).netloc] += 1

    @property
    def elapsed(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at if self.finished_at is not None \
            else time.perf_counter()
        return end - self.started_at

    def get_stats(self) -> dict:
        return {
            "total_pages": self.successful + self.failed,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_seconds": round(self.elapsed, 2),
            "pages_per_second": round(self.successful / self.elapsed, 2)
            if self.elapsed > 0 else 0.0,
            "status_codes": {str(code): count
                             for code, count in sorted(self.by_status.items())},
            "top_domains": [
                {"domain": domain, "pages": count}
                for domain, count in self.by_domain.most_common(self._top)
            ],
        }

    async def export_to_json(self, filename: str) -> str:
        directory = os.path.dirname(filename)
        if directory:
            os.makedirs(directory, exist_ok=True)
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(self.get_stats(), ensure_ascii=False,
                                     indent=2))
        return filename

    async def export_to_html_report(self, filename: str) -> str:
        stats = self.get_stats()
        directory = os.path.dirname(filename)
        if directory:
            os.makedirs(directory, exist_ok=True)

        def bars(rows: list[tuple[str, int]]) -> str:
            if not rows:
                return "<p>нет данных</p>"
            maximum = max(count for _, count in rows)
            items = []
            for label, count in rows:
                width = max(2, round(count / maximum * 100))
                items.append(
                    f"<tr><td>{label}</td><td>{count}</td>"
                    f"<td><div class='bar' style='width:{width}%'></div></td></tr>")
            return ("<table><tr><th>Ключ</th><th>Кол-во</th>"
                    "<th></th></tr>" + "".join(items) + "</table>")

        status_rows = [(code, count)
                       for code, count in stats["status_codes"].items()]
        domain_rows = [(d["domain"], d["pages"])
                       for d in stats["top_domains"]]

        html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<title>Отчёт краулера</title>
<style>
 body {{ font-family: sans-serif; margin: 2em; color: #222; }}
 .cards {{ display: flex; gap: 1em; flex-wrap: wrap; }}
 .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 1em 1.5em; }}
 .card b {{ font-size: 1.6em; }}
 table {{ border-collapse: collapse; margin-top: 1em; min-width: 30em; }}
 td, th {{ border-bottom: 1px solid #eee; padding: 4px 12px; text-align: left; }}
 .bar {{ background: #4a90d9; height: 14px; border-radius: 3px; }}
</style></head><body>
<h1>Отчёт краулера</h1>
<div class="cards">
 <div class="card">Страниц обработано<br><b>{stats['total_pages']}</b></div>
 <div class="card">Успешно<br><b>{stats['successful']}</b></div>
 <div class="card">Ошибок<br><b>{stats['failed']}</b></div>
 <div class="card">Время<br><b>{stats['elapsed_seconds']}с</b></div>
 <div class="card">Скорость<br><b>{stats['pages_per_second']} стр/с</b></div>
</div>
<h2>Статус-коды</h2>
{bars(status_rows)}
<h2>Топ доменов</h2>
{bars(domain_rows)}
</body></html>"""
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(html)
        return filename
