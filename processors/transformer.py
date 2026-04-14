"""
Data transformation processor.

Adds computed columns, normalizes dates, extracts domains from URLs,
and validates schema completeness.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from models import BaseRecord, GitHubRepoRecord, HackerNewsRecord, WeatherRecord

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def extract_domain(url: str | None) -> str | None:
    """Extract the registered domain from a URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        host = parsed.netloc or parsed.path
        # Strip www. prefix
        host = re.sub(r"^www\.", "", host)
        return host or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def word_count(text: str | None) -> int:
    """Count words in a string."""
    if not text:
        return 0
    return len(text.split())


def simple_category(text: str | None, title: str | None = None) -> str:
    """
    Assign a rough content category based on keywords.
    This is a placeholder for a real classifier.
    """
    combined = " ".join(filter(None, [text, title])).lower()
    if not combined:
        return "uncategorized"

    keyword_map = [
        ("ai ml nlp llm model neural gpt bert pytorch tensorflow", "ai-ml"),
        ("security vulnerability exploit cve hack breach", "security"),
        ("cloud kubernetes docker aws gcp azure devops", "cloud-devops"),
        ("startup funding raise series seed venture capital vc", "startup"),
        ("web frontend react vue angular nextjs", "web-frontend"),
        ("rust golang python rust zig", "systems-lang"),
        ("database postgres mysql sqlite redis mongo", "database"),
        ("open source release library framework tool", "open-source"),
        ("tutorial guide howto beginner", "tutorial"),
    ]
    for keywords, category in keyword_map:
        for kw in keywords.split():
            if kw in combined:
                return category
    return "general-tech"


# ---------------------------------------------------------------------------
# Per-model transformers
# ---------------------------------------------------------------------------

def _set_extra(record: Any, key: str, value: Any) -> None:
    """Safely set an extra field on a Pydantic model that uses extra='allow'."""
    # model_extra may be None until the first extra field is set
    if record.model_extra is None:
        # Pydantic v2: __pydantic_extra__ is the backing dict
        object.__setattr__(record, "__pydantic_extra__", {key: value})
    else:
        record.model_extra[key] = value


def _transform_hackernews(record: HackerNewsRecord) -> HackerNewsRecord:
    record.domain = extract_domain(record.url)
    record.word_count = word_count(record.title)
    # Add computed fields as model_extra (Pydantic extra="allow")
    _set_extra(record, "category", simple_category(record.title))
    _set_extra(record, "score_tier", (
        "viral" if record.score >= 500
        else "hot" if record.score >= 100
        else "rising" if record.score >= 20
        else "new"
    ))
    return record


def _transform_github(record: GitHubRepoRecord) -> GitHubRepoRecord:
    if record.forks > 0:
        record.stars_per_fork_ratio = round(record.stars / record.forks, 2)
    _set_extra(record, "category", simple_category(record.description, record.repo_name))
    _set_extra(record, "size_tier", (
        "mega" if record.stars >= 10_000
        else "large" if record.stars >= 1_000
        else "medium" if record.stars >= 100
        else "small"
    ))
    return record


def _transform_weather(record: WeatherRecord) -> WeatherRecord:
    # Add human-readable weather description
    from sources.weather import WMO_CODES  # avoid circular at module level
    if record.weather_code is not None:
        _set_extra(record, "weather_description", WMO_CODES.get(record.weather_code, "Unknown"))
    # Compute feels-like delta
    if record.temperature_c is not None and record.apparent_temperature_c is not None:
        delta = round(record.apparent_temperature_c - record.temperature_c, 1)
        _set_extra(record, "feels_like_delta_c", delta)
    return record


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def transform_record(record: BaseRecord) -> BaseRecord:
    """Apply source-specific transformations to a single record."""
    if isinstance(record, HackerNewsRecord):
        return _transform_hackernews(record)
    if isinstance(record, GitHubRepoRecord):
        return _transform_github(record)
    if isinstance(record, WeatherRecord):
        return _transform_weather(record)
    # Unknown type — pass through
    return record


def transform_records(records: list[BaseRecord]) -> list[BaseRecord]:
    """Transform a list of records. Returns a new list."""
    transformed: list[BaseRecord] = []
    errors = 0

    for rec in records:
        try:
            transformed.append(transform_record(rec))
        except Exception as exc:
            log.warning("Transform failed for record %s: %s", getattr(rec, "record_hash", "?"), exc)
            transformed.append(rec)  # keep original on failure
            errors += 1

    log.info("Transformer: %d records processed, %d errors", len(records), errors)
    return transformed


# ---------------------------------------------------------------------------
# Schema validation report
# ---------------------------------------------------------------------------

def validate_schema(records: list[BaseRecord]) -> dict[str, Any]:
    """
    Report completeness of each field across all records.

    Returns a dict mapping field_name → {total, present, missing, pct_complete}.
    """
    if not records:
        return {}

    field_stats: dict[str, dict[str, int]] = {}

    for rec in records:
        for key, value in rec.model_dump().items():
            stats = field_stats.setdefault(key, {"total": 0, "present": 0, "missing": 0})
            stats["total"] += 1
            if value is not None and value != "":
                stats["present"] += 1
            else:
                stats["missing"] += 1

    result: dict[str, Any] = {}
    for field_name, stats in field_stats.items():
        pct = round(100 * stats["present"] / stats["total"], 1) if stats["total"] else 0.0
        result[field_name] = {**stats, "pct_complete": pct}

    return result
