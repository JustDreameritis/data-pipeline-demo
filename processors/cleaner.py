"""
Data cleaning processor.

Handles:
- Unicode normalization
- Whitespace stripping
- Missing value handling (drop | fill | flag)
- Type coercion (dates, numbers)
- Basic text normalization
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime
from typing import Any

import config as cfg
from models import BaseRecord

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------

def normalize_unicode(text: str) -> str:
    """Normalize unicode to NFC form and strip control characters."""
    normalized = unicodedata.normalize("NFC", text)
    # Remove non-printable control characters except common whitespace
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", normalized)


def strip_whitespace(text: str) -> str:
    """Strip leading/trailing whitespace and collapse internal runs."""
    return re.sub(r"\s+", " ", text.strip())


def clean_text(text: str) -> str:
    """Apply unicode normalization + whitespace stripping."""
    return strip_whitespace(normalize_unicode(text))


def coerce_int(value: Any) -> int | None:
    """Attempt to coerce a value to int. Returns None on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    """Attempt to coerce a value to float. Returns None on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_datetime(value: Any) -> datetime | None:
    """Attempt to parse a value as datetime. Supports ISO strings and unix timestamps."""
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Missing value strategies
# ---------------------------------------------------------------------------

def _handle_missing_str(
    key: str, strategy: str, fill: str
) -> str | None:
    if strategy == "drop":
        return None  # signal to caller to drop the record
    if strategy == "fill":
        return fill
    # strategy == "flag"
    return f"__MISSING_{key.upper()}__"


def _handle_missing_num(
    key: str, strategy: str, fill: float
) -> float | None:
    if strategy == "drop":
        return None
    if strategy == "fill":
        return fill
    return -9999.0  # sentinel for flagged numeric missing


# ---------------------------------------------------------------------------
# Main cleaner
# ---------------------------------------------------------------------------

class Cleaner:
    """
    Stateless data cleaner applied to each record in the pipeline.

    Cleaning is non-destructive: it returns a new dict rather than
    mutating the original record.
    """

    def __init__(
        self,
        missing_strategy: str | None = None,
        fill_str: str | None = None,
        fill_num: float | None = None,
    ) -> None:
        self.strategy = missing_strategy or cfg.processing.missing_value_strategy
        self.fill_str = fill_str if fill_str is not None else cfg.processing.fill_value_str
        self.fill_num = fill_num if fill_num is not None else cfg.processing.fill_value_num

    def clean_record(self, record: BaseRecord) -> BaseRecord | None:
        """
        Clean a single record in-place (modifies model fields).

        Returns the cleaned record, or None if the record should be dropped.
        """
        # Inspect the model's fields to know which are Optional (allow None)
        optional_fields = self._get_optional_fields(type(record))
        data = record.model_dump()
        cleaned = self._clean_dict(data, optional_fields)
        if cleaned is None:
            return None
        # Rebuild with cleaned data; use model_validate to preserve type
        return type(record).model_validate(cleaned)

    @staticmethod
    def _get_optional_fields(model_cls: type) -> set[str]:
        """Return the set of field names that allow None in the model."""
        import typing
        optional: set[str] = set()
        for name, field_info in model_cls.model_fields.items():
            annotation = field_info.annotation
            # Check if annotation is Optional (i.e., Union[X, None])
            origin = getattr(annotation, "__origin__", None)
            if origin is typing.Union:
                if type(None) in annotation.__args__:
                    optional.add(name)
            elif annotation is type(None):
                optional.add(name)
        return optional

    def _clean_dict(
        self, data: dict[str, Any], optional_fields: set[str] | None = None
    ) -> dict[str, Any] | None:
        """Clean a flat dict. Returns None if the record should be dropped."""
        opt = optional_fields or set()
        result: dict[str, Any] = {}
        for key, value in data.items():
            if value is None:
                if key in opt:
                    # Optional field — None is valid, leave it alone
                    result[key] = None
                elif self.strategy == "drop":
                    log.debug("Dropping record due to missing required field '%s'", key)
                    return None
                else:
                    # For non-optional fields with strategy=fill/flag,
                    # use the string flag in the extra/metadata only
                    result[key] = None  # keep None; flag is informational
                continue
            cleaned_value = self._clean_value(key, value, opt)
            result[key] = cleaned_value
        return result

    def _clean_value(self, key: str, value: Any, optional_fields: set[str] | None = None) -> Any:
        if isinstance(value, str):
            cleaned = clean_text(value)
            if not cleaned:
                opt = optional_fields or set()
                if key in opt:
                    return None
                if self.strategy == "fill":
                    return self.fill_str
                if self.strategy == "flag":
                    return f"__MISSING_{key.upper()}__"
                return None  # drop strategy: return None, handled by caller
            return cleaned

        if isinstance(value, list):
            return [self._clean_value(f"{key}[{i}]", v, optional_fields) for i, v in enumerate(value)]

        if isinstance(value, dict):
            return {k: self._clean_value(k, v, optional_fields) for k, v in value.items()}

        # Numbers, booleans, datetimes — pass through unchanged
        return value


def clean_records(
    records: list[BaseRecord],
    missing_strategy: str | None = None,
) -> tuple[list[BaseRecord], int]:
    """
    Clean a list of records.

    Returns:
        (cleaned_records, drop_count) tuple.
    """
    cleaner = Cleaner(missing_strategy=missing_strategy)
    cleaned: list[BaseRecord] = []
    dropped = 0

    for rec in records:
        result = cleaner.clean_record(rec)
        if result is None:
            dropped += 1
        else:
            cleaned.append(result)

    log.info("Cleaner: %d records in, %d cleaned, %d dropped", len(records), len(cleaned), dropped)
    return cleaned, dropped
