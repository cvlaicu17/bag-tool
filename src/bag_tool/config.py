"""Persistent configuration for bag-tool stored in ~/.config/bag-tool/config.json."""

from __future__ import annotations
import json
from pathlib import Path

_CONFIG_DIR  = Path.home() / ".config" / "bag-tool"
_CONFIG_FILE = _CONFIG_DIR / "config.json"


def _load() -> dict:
    if _CONFIG_FILE.exists():
        try:
            return json.loads(_CONFIG_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save(data: dict) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps(data, indent=2))


def get_vio_topic() -> str | None:
    """Return the stored default VIO topic, or None if not set."""
    return _load().get("vio_topic")


def set_vio_topic(topic: str) -> None:
    """Persist topic as the new default VIO topic."""
    data = _load()
    data["vio_topic"] = topic
    _save(data)
