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


def get_vib_ref() -> str | None:
    """Return the stored default vibration reference bag path, or None if not set."""
    return _load().get("vib_ref")


def set_vib_ref(path: str) -> None:
    """Persist path as the default vibration reference bag."""
    data = _load()
    data["vib_ref"] = path
    _save(data)


def get_vib_targets() -> str | None:
    """Return the stored default vibration-verification targets JSON path, or None."""
    return _load().get("vib_targets")


def set_vib_targets(path: str) -> None:
    """Persist path as the default vibration-verification targets JSON."""
    data = _load()
    data["vib_targets"] = path
    _save(data)


def get_vib_state() -> str | None:
    """Return the stored default vib-fit-state coefficients JSON path, or None."""
    return _load().get("vib_state")


def set_vib_state(path: str) -> None:
    """Persist path as the default vib-fit-state coefficients JSON."""
    data = _load()
    data["vib_state"] = path
    _save(data)


def get_vib_harmonic_targets() -> str | None:
    """Return the stored default harmonic-indexed vib targets JSON path, or None."""
    return _load().get("vib_harmonic_targets")


def set_vib_harmonic_targets(path: str) -> None:
    """Persist path as the default harmonic-indexed vib targets JSON."""
    data = _load()
    data["vib_harmonic_targets"] = path
    _save(data)
