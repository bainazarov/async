from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler

_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

# 5 МБ на файл, храним 3 старые копии
_MAX_BYTES = 5 * 1024 * 1024
_BACKUP_COUNT = 3


def setup_logging(log_file: str | None = None,
                  level: str = "INFO") -> logging.Logger:

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    root.addHandler(console)

    if log_file:
        directory = os.path.dirname(log_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT,
            encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
        root.addHandler(file_handler)

    return root
