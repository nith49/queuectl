"""Configuration management with persistent storage."""

from __future__ import annotations

import json
import os
from typing import Any

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".queuectl", "config.json")

DEFAULTS = {
    "max-retries": 3,
    "backoff-base": 2,
    "worker-poll-interval": 1,
    "job-timeout": 0,  # 0 means no timeout
}


def _ensure_dir():
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)


def load_config() -> dict:
    _ensure_dir()
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            stored = json.load(f)
        # Merge with defaults for any missing keys
        merged = {**DEFAULTS, **stored}
        return merged
    return dict(DEFAULTS)


def save_config(config: dict):
    _ensure_dir()
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def get_config(key: str) -> Any:
    config = load_config()
    if key not in config:
        raise KeyError(f"Unknown config key: {key}. Available: {', '.join(DEFAULTS.keys())}")
    return config[key]


def set_config(key: str, value: str) -> Any:
    if key not in DEFAULTS:
        raise KeyError(f"Unknown config key: {key}. Available: {', '.join(DEFAULTS.keys())}")
    config = load_config()
    # Cast to the same type as the default
    default_type = type(DEFAULTS[key])
    config[key] = default_type(value)
    save_config(config)
    return config[key]


def reset_config():
    save_config(dict(DEFAULTS))
