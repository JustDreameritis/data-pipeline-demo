"""
SQLite exporter.

- Auto-creates the table based on record fields (first batch defines schema)
- Upserts (INSERT OR REPLACE) on conflict
- Adds indexes on common query columns (source, fetched_at, primary key)
- Uses a simple connection wrapper (no heavy ORM dependency)
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import config as cfg
from models import BaseRecord
from utils import ensure_dir

log = logging.getLogger(__name__)

# Columns to index automatically if present in the schema
_AUTO_INDEX_COLUMNS = {"source", "fetched_at", "hn_id", "repo_name", "city", "observation_time"}

# Map Python types → SQLite affinities
_TYPE_MAP: dict[type, str] = {
    int: "INTEGER",
    float: "REAL",
    bool: "INTEGER",  # SQLite has no boolean
    str: "TEXT",
    type(None): "TEXT",
}


def _py_to_sqlite_type(value: Any) -> str:
    return _TYPE_MAP.get(type(value), "TEXT")


def _infer_schema(rows: list[dict[str, Any]]) -> dict[str, str]:
    """
    Infer SQLite column types from the first non-None value seen per column.
    Returns an ordered dict of {column_name: sqlite_type}.
    """
    schema: dict[str, str] = {}
    for row in rows:
        for col, val in row.items():
            if col not in schema:
                schema[col] = _py_to_sqlite_type(val)
            elif schema[col] == "TEXT" and val is not None:
                # Upgrade TEXT to more specific type if we have a real value
                inferred = _py_to_sqlite_type(val)
                if inferred != "TEXT":
                    schema[col] = inferred
    return schema


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert Python objects to SQLite-compatible scalars."""
    result: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (list, dict)):
            result[k] = str(v)
        elif isinstance(v, bool):
            result[k] = int(v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Connection pool (simple thread-local singleton per path)
# ---------------------------------------------------------------------------

class _ConnectionPool:
    """Minimal connection pool: one connection per db_path."""

    _connections: dict[str, sqlite3.Connection] = {}

    @classmethod
    def get(cls, db_path: Path) -> sqlite3.Connection:
        key = str(db_path.resolve())
        if key not in cls._connections:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            cls._connections[key] = conn
        return cls._connections[key]

    @classmethod
    def close(cls, db_path: Path) -> None:
        key = str(db_path.resolve())
        if key in cls._connections:
            cls._connections[key].close()
            del cls._connections[key]


@contextmanager
def get_connection(db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    conn = _ConnectionPool.get(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# SQLite exporter
# ---------------------------------------------------------------------------

class SQLiteExporter:
    """Exports records to a SQLite database, upserting on conflict."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path) if db_path else cfg.export.sqlite_db_path

    def export(self, records: list[BaseRecord], table_name: str | None = None) -> int:
        """
        Upsert records into the database.

        Args:
            records: Records to write.
            table_name: SQLite table name. Defaults to the source name.

        Returns:
            Number of rows written.
        """
        if not records:
            log.warning("SQLite exporter: no records to write")
            return 0

        ensure_dir(self.db_path.parent)
        tbl = table_name or records[0].source.replace("-", "_").replace(" ", "_")

        rows = [_serialize_row(r.to_flat_dict()) for r in records]
        schema = _infer_schema(rows)
        columns = list(schema.keys())

        with get_connection(self.db_path) as conn:
            self._ensure_table(conn, tbl, schema)
            self._ensure_indexes(conn, tbl, columns)
            written = self._upsert_rows(conn, tbl, columns, rows)

        log.info("SQLite: upserted %d rows into table '%s' at %s", written, tbl, self.db_path)
        return written

    def _ensure_table(
        self, conn: sqlite3.Connection, table: str, schema: dict[str, str]
    ) -> None:
        """Create the table if it doesn't exist; add missing columns if it does."""
        col_defs = ", ".join(f'"{col}" {dtype}' for col, dtype in schema.items())
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

        # Add any new columns that don't exist yet (ALTER TABLE ADD COLUMN)
        existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
        for col, dtype in schema.items():
            if col not in existing:
                log.debug("Adding column '%s' (%s) to table '%s'", col, dtype, table)
                conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {dtype}')

    def _ensure_indexes(
        self, conn: sqlite3.Connection, table: str, columns: list[str]
    ) -> None:
        """Create indexes on commonly-queried columns."""
        for col in columns:
            if col in _AUTO_INDEX_COLUMNS:
                idx_name = f"idx_{table}_{col}"
                conn.execute(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{col}")'
                )

    def _upsert_rows(
        self,
        conn: sqlite3.Connection,
        table: str,
        columns: list[str],
        rows: list[dict[str, Any]],
    ) -> int:
        """INSERT OR REPLACE all rows. Returns count written."""
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        sql = f'INSERT OR REPLACE INTO "{table}" ({col_list}) VALUES ({placeholders})'

        data = [[row.get(c) for c in columns] for row in rows]
        conn.executemany(sql, data)
        return len(data)

    def query(self, table: str, limit: int = 10) -> list[dict[str, Any]]:
        """Simple query helper for verification."""
        with get_connection(self.db_path) as conn:
            cursor = conn.execute(f'SELECT * FROM "{table}" LIMIT {limit}')
            return [dict(row) for row in cursor.fetchall()]


def export_sqlite(
    records: list[BaseRecord],
    table_name: str | None = None,
    db_path: Path | str | None = None,
) -> int:
    """Convenience function for one-shot SQLite export."""
    exporter = SQLiteExporter(db_path=db_path)
    return exporter.export(records, table_name=table_name)
