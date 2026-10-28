from __future__ import annotations

import json
import logging

import yaml

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        text = f.read()

    if path.lower().endswith((".yaml", ".yml")):
        config = yaml.safe_load(text)
    elif path.lower().endswith(".json"):
        config = json.loads(text)
    else:
        # Расширение не подсказывает формат — пробуем оба.
        try:
            config = yaml.safe_load(text)
        except yaml.YAMLError:
            config = json.loads(text)

    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ValueError(f"Конфиг {path} должен содержать словарь на верхнем уровне")
    logger.info("Конфигурация загружена: %s", path)
    return config
