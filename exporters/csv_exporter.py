"""
CSV exporter.

Writes pipeline records to a CSV file with proper quoting, configurable
delimiter, and append/overwrite mode.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

import config as cfg
from models import BaseRecord
from utils import ensure_dir

log = logging.getLogger(__name__)


def _flatten(record: BaseRecord) -> dict[str, Any]:
    """Flatten a record to a simple dict, serialising complex types to strings."""
    flat: dict[str, Any] = {}
    for key, value in record.to_flat_dict().items():
        if isinstance(value, list):
            flat[key] = "|".join(str(v) for v in value)
        elif isinstance(value, dict):
            flat[key] = str(value)
        else:
            flat[key] = value
    return flat


class CSVExporter:
    """
    Exports records to CSV.

    - First write: creates/overwrites the file and writes a header row.
    - Append mode: appends rows to an existing file without re-writing the header.
    """

    def __init__(
        self,
        path: Path | str | None = None,
        delimiter: str | None = None,
        append: bool | None = None,
    ) -> None:
        self.delimiter = delimiter or cfg.export.csv_delimiter
        self.append = append if append is not None else cfg.export.csv_append
        # Default path derived from export_dir; callers may override
        self._path: Path | None = Path(path) if path else None

    def _resolve_path(self, source: str) -> Path:
        if self._path:
            return self._path
        export_dir = ensure_dir(cfg.export.export_dir)
        return export_dir / f"{source}.csv"

    def export(self, records: list[BaseRecord], source: str = "records") -> Path:
        """
        Write records to CSV.

        Args:
            records: Records to export.
            source: Used to derive a filename if no explicit path was given.

        Returns:
            Path to the written file.
        """
        if not records:
            log.warning("CSV exporter: no records to write for source '%s'", source)
            return self._resolve_path(source)

        output_path = self._resolve_path(source)
        ensure_dir(output_path.parent)

        rows = [_flatten(r) for r in records]
        fieldnames = list(rows[0].keys())

        # Check if we can append (file exists and has content)
        write_header = True
        mode = "w"
        if self.append and output_path.exists() and output_path.stat().st_size > 0:
            mode = "a"
            write_header = False

        with open(output_path, mode=mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=fieldnames,
                delimiter=self.delimiter,
                quoting=csv.QUOTE_MINIMAL,
                extrasaction="ignore",
            )
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

        log.info(
            "CSV: wrote %d rows to %s (mode=%s, delimiter='%s')",
            len(rows), output_path, mode, self.delimiter,
        )
        return output_path


def export_csv(
    records: list[BaseRecord],
    source: str = "records",
    path: Path | str | None = None,
    delimiter: str | None = None,
    append: bool = False,
) -> Path:
    """Convenience function for one-shot CSV export."""
    exporter = CSVExporter(path=path, delimiter=delimiter, append=append)
    return exporter.export(records, source=source)
