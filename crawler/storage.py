from __future__ import annotations

import abc
import asyncio
import csv
import io
import json
import logging
from datetime import datetime, timezone

import aiofiles
import aiosqlite

logger = logging.getLogger(__name__)

FLUSH_EVERY = 50
WRITE_RETRIES = 3


def normalize_record(data: dict) -> dict:
    crawled_at = data.get("crawled_at")
    if crawled_at is None:
        crawled_at = datetime.now(timezone.utc).isoformat()
    elif isinstance(crawled_at, datetime):
        crawled_at = crawled_at.isoformat()

    status_code = data.get("status_code") or 0
    return {
        "url": str(data.get("url", "")),
        "title": str(data.get("title", "")),
        "text": str(data.get("text", "")),
        "links": list(data.get("links", [])),
        "metadata": dict(data.get("metadata", {})),
        "crawled_at": str(crawled_at),
        "status_code": int(status_code),
        "content_type": str(data.get("content_type", "")),
    }


class DataStorage(abc.ABC):
    def __init__(self, flush_every: int = FLUSH_EVERY):
        self._flush_every = flush_every
        self._buffer: list[dict] = []
        self.saved_count = 0
        self.error_count = 0

    async def save(self, data: dict) -> None:
        self._buffer.append(normalize_record(data))
        if len(self._buffer) >= self._flush_every:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        batch, self._buffer = self._buffer, []
        for attempt in range(1, WRITE_RETRIES + 1):
            try:
                await self._write_batch(batch)
            except Exception as e:
                logger.warning("Ошибка записи (попытка %d/%d): %s",
                               attempt, WRITE_RETRIES, e)
                if attempt < WRITE_RETRIES:
                    await asyncio.sleep(0.1 * attempt)
            else:
                self.saved_count += len(batch)
                logger.debug("Сохранено записей: %d", self.saved_count)
                return
        self.error_count += len(batch)
        logger.error("Батч из %d записей потерян: запись не удалась",
                     len(batch))

    async def close(self) -> None:
        await self.flush()
        await self._close_impl()

    @abc.abstractmethod
    async def _write_batch(self, batch: list[dict]) -> None:
        """Записать список записей одной операцией."""

    async def _close_impl(self) -> None:
        """Дополнительное закрытие ресурсов (файлов, соединений)."""

    def get_stats(self) -> dict:
        return {"saved": self.saved_count,
                "errors": self.error_count,
                "buffered": len(self._buffer)}


class JSONStorage(DataStorage):

    def __init__(self, path: str, flush_every: int = FLUSH_EVERY,
                 encoding: str = "utf-8", indent: int = 2):
        super().__init__(flush_every)
        self._path = path
        self._encoding = encoding
        self._indent = indent
        self._file = None
        self._first_batch = True

    async def _ensure_open(self) -> None:
        if self._file is None:
            self._file = await aiofiles.open(self._path, "w",
                                             encoding=self._encoding)
            await self._file.write("[\n")

    async def _write_batch(self, batch: list[dict]) -> None:
        await self._ensure_open()
        parts = [json.dumps(record, ensure_ascii=False, indent=self._indent)
                 for record in batch]
        separator = "" if self._first_batch else ",\n"
        self._first_batch = False
        await self._file.write(separator + ",\n".join(parts))

    async def _close_impl(self) -> None:
        if self._file is not None:
            await self._file.write("\n]\n")
            await self._file.close()
            self._file = None


class CSVStorage(DataStorage):

    def __init__(self, path: str, flush_every: int = FLUSH_EVERY,
                 encoding: str = "utf-8"):
        super().__init__(flush_every)
        self._path = path
        self._encoding = encoding
        self._file = None
        self._header_written = False
        self._columns: list[str] = []

    async def _ensure_open(self) -> None:
        if self._file is None:
            self._file = await aiofiles.open(self._path, "w",
                                             encoding=self._encoding,
                                             newline="")

    @staticmethod
    def _flatten(record: dict) -> dict:
        row = dict(record)
        row["links"] = json.dumps(row["links"], ensure_ascii=False)
        row["metadata"] = json.dumps(row["metadata"], ensure_ascii=False)
        return row

    async def _write_batch(self, batch: list[dict]) -> None:
        await self._ensure_open()
        rows = [self._flatten(record) for record in batch]
        buf = io.StringIO()
        writer = csv.writer(buf)
        if not self._header_written:
            self._columns = list(rows[0].keys())
            writer.writerow(self._columns)
            self._header_written = True
        for row in rows:
            writer.writerow([row.get(col, "") for col in self._columns])
        await self._file.write(buf.getvalue())

    async def _close_impl(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


class SQLiteStorage(DataStorage):

    def __init__(self, path: str, flush_every: int = FLUSH_EVERY,
                 table: str = "pages"):
        super().__init__(flush_every)
        self._path = path
        self._table = table
        self._db: aiosqlite.Connection | None = None

    async def init_db(self) -> None:
        if self._db is None:
            self._db = await aiosqlite.connect(self._path)
        await self._db.execute(
            f"""CREATE TABLE IF NOT EXISTS {self._table} (
                    url          TEXT PRIMARY KEY,
                    title        TEXT,
                    text         TEXT,
                    links        TEXT,
                    metadata     TEXT,
                    crawled_at   TEXT,
                    status_code  INTEGER,
                    content_type TEXT
                )""")
        await self._db.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self._table}_crawled_at "
            f"ON {self._table} (crawled_at)")
        await self._db.commit()

    async def _write_batch(self, batch: list[dict]) -> None:
        await self.init_db()
        rows = [(
            r["url"], r["title"], r["text"],
            json.dumps(r["links"], ensure_ascii=False),
            json.dumps(r["metadata"], ensure_ascii=False),
            r["crawled_at"], r["status_code"], r["content_type"],
        ) for r in batch]
        await self._db.executemany(
            f"INSERT OR REPLACE INTO {self._table} "
            f"(url, title, text, links, metadata, crawled_at, status_code, "
            f"content_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        await self._db.commit()

    async def _close_impl(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def read_all(self) -> list[dict]:
        await self.init_db()
        cursor = await self._db.execute(
            f"SELECT url, title, text, links, metadata, crawled_at, "
            f"status_code, content_type FROM {self._table} "
            f"ORDER BY crawled_at, rowid")
        result = []
        async for row in cursor:
            result.append({
                "url": row[0],
                "title": row[1],
                "text": row[2],
                "links": json.loads(row[3] or "[]"),
                "metadata": json.loads(row[4] or "{}"),
                "crawled_at": row[5],
                "status_code": row[6],
                "content_type": row[7],
            })
        return result


class MultiStorage(DataStorage):
    def __init__(self, *storages: DataStorage):
        super().__init__(flush_every=1)
        self._storages = storages

    async def save(self, data: dict) -> None:
        for storage in self._storages:
            await storage.save(data)

    async def flush(self) -> None:
        for storage in self._storages:
            await storage.flush()

    async def _write_batch(self, batch: list[dict]) -> None:  # не используется
        raise NotImplementedError

    async def _close_impl(self) -> None:
        for storage in self._storages:
            await storage.close()

    def get_stats(self) -> dict:
        return {f"{type(s).__name__}": s.get_stats() for s in self._storages}
