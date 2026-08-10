"""Loads and validates the YAML configuration, resolving ${ENV_VAR}
placeholders from environment variables (via a local .env file).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


class ConfigError(Exception):
    """Raised when the configuration file is missing or invalid."""


def _resolve_env_vars(value: Any) -> Any:
    """Recursively substitute ${VAR_NAME} placeholders with environment values."""
    if isinstance(value, str):
        def replace(match: re.Match) -> str:
            var_name = match.group(1)
            resolved = os.environ.get(var_name)
            if resolved is None:
                raise ConfigError(
                    f"Environment variable '{var_name}' referenced in config.yaml "
                    "is not set. Check your .env file."
                )
            return resolved

        return _ENV_VAR_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    return value


def load_config(config_path: str = "config.yaml", env_path: str = ".env") -> dict:
    """Load config.yaml, substitute environment variables, and validate structure."""
    if Path(env_path).exists():
        load_dotenv(env_path)

    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    if not raw_config:
        raise ConfigError("Configuration file is empty.")

    config = _resolve_env_vars(raw_config)
    _validate_config(config)
    return config


def _validate_config(config: dict) -> None:
    for section in ("oracle", "sqlserver", "tables"):
        if section not in config:
            raise ConfigError(f"Missing required '{section}' section in config.yaml")

    for key in ("user", "password", "dsn"):
        if not config["oracle"].get(key):
            raise ConfigError(f"Missing 'oracle.{key}' in config.yaml")

    for key in ("server", "database", "user", "password"):
        if not config["sqlserver"].get(key):
            raise ConfigError(f"Missing 'sqlserver.{key}' in config.yaml")

    if not isinstance(config["tables"], list) or not config["tables"]:
        raise ConfigError("'tables' must be a non-empty list in config.yaml")

    for entry in config["tables"]:
        if not entry.get("source") or not entry.get("target"):
            raise ConfigError(f"Table entry missing 'source' or 'target': {entry}")
