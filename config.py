"""
Configuration module.

Loads settings from environment variables / .env file.
All pipeline components import from here instead of reading env vars directly.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load .env from project root (if present)
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


def _int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def _str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _list(key: str, default: list[str]) -> list[str]:
    raw = os.environ.get(key)
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, "").lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no"):
        return False
    return default


# ---------------------------------------------------------------------------
# Source settings
# ---------------------------------------------------------------------------

class HackerNewsConfig:
    limit: int = _int("HACKERNEWS_LIMIT", 100)
    base_url: str = "https://hacker-news.firebaseio.com/v0"
    feed: str = _str("HACKERNEWS_FEED", "topstories")  # topstories | newstories | beststories


class GitHubConfig:
    language: str = _str("GITHUB_LANGUAGE", "python")
    timeframe: str = _str("GITHUB_TIMEFRAME", "weekly")   # daily | weekly | monthly
    limit: int = _int("GITHUB_LIMIT", 50)
    # GitHub REST search API (no auth for low-volume usage)
    base_url: str = "https://api.github.com"
    token: Optional[str] = os.environ.get("GITHUB_TOKEN")  # optional, raises rate limit if set


class WeatherConfig:
    cities: list[str] = _list("WEATHER_CITIES", ["London", "New York", "Tokyo", "Sydney"])
    days_forecast: int = _int("WEATHER_FORECAST_DAYS", 3)
    days_history: int = _int("WEATHER_HISTORY_DAYS", 0)
    base_url: str = "https://api.open-meteo.com/v1"
    geocoding_url: str = "https://geocoding-api.open-meteo.com/v1"


# ---------------------------------------------------------------------------
# Processing settings
# ---------------------------------------------------------------------------

class ProcessingConfig:
    dedup_similarity_threshold: float = _float("DEDUP_SIMILARITY_THRESHOLD", 0.85)
    missing_value_strategy: str = _str("MISSING_VALUE_STRATEGY", "flag")  # drop | fill | flag
    fill_value_str: str = _str("FILL_VALUE_STR", "")
    fill_value_num: float = _float("FILL_VALUE_NUM", 0.0)


# ---------------------------------------------------------------------------
# Export settings
# ---------------------------------------------------------------------------

class ExportConfig:
    export_dir: Path = Path(_str("EXPORT_DIR", "./output"))
    sqlite_db_path: Path = Path(_str("SQLITE_DB_PATH", "./output/pipeline.db"))
    csv_delimiter: str = _str("CSV_DELIMITER", ",")
    csv_append: bool = _bool("CSV_APPEND", False)
    # Google Sheets (optional)
    sheets_credentials_path: Optional[str] = os.environ.get("GOOGLE_CREDENTIALS_PATH")
    sheets_spreadsheet_id: Optional[str] = os.environ.get("GOOGLE_SPREADSHEET_ID")


# ---------------------------------------------------------------------------
# General
# ---------------------------------------------------------------------------

class GeneralConfig:
    log_level: str = _str("LOG_LEVEL", "INFO")
    request_delay: float = _float("REQUEST_DELAY", 0.5)
    max_retries: int = _int("MAX_RETRIES", 3)
    dry_run: bool = _bool("DRY_RUN", False)


# Singleton instances used by the rest of the codebase
hackernews = HackerNewsConfig()
github = GitHubConfig()
weather = WeatherConfig()
processing = ProcessingConfig()
export = ExportConfig()
general = GeneralConfig()


def configure_logging(level: str | None = None) -> None:
    """Set up root logger with a sensible format."""
    lvl = getattr(logging, (level or general.log_level).upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
