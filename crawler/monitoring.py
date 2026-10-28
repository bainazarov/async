from __future__ import annotations

import asyncio
import time


class ProgressMonitor:

    def __init__(self, interval: float = 1.0, width: int = 30):
        self._interval = interval
        self._width = width

    async def run(self, stop: asyncio.Event, stats, max_pages: int) -> None:
        started_at = time.perf_counter()
        while True:
            done = stats.successful + stats.failed
            elapsed = time.perf_counter() - started_at
            percent = min(100.0, done / max_pages * 100) if max_pages else 0.0
            filled = round(self._width * percent / 100)
            bar = "█" * filled + "░" * (self._width - filled)
            rate = done / elapsed if elapsed > 0 else 0.0
            remaining = max_pages - done
            eta = remaining / rate if rate > 0 else 0.0
            tasks = len(asyncio.all_tasks()) - 1  # без самого монитора

            print(f"\r  {bar} {percent:5.1f}% | {done}/{max_pages} | "
                  f"{rate:.1f} стр/с | прошло {elapsed:.0f}с | "
                  f"осталось ~{eta:.0f}с | задач: {tasks}   ",
                  end="", flush=True)

            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
                break
            except asyncio.TimeoutError:
                continue
        print()
