#!/usr/bin/env python3
"""
Data Pipeline Toolkit — main orchestrator.

Usage examples:
    python pipeline.py --source hackernews --export csv
    python pipeline.py --source hackernews --export csv,sqlite --limit 50
    python pipeline.py --source github_trending --export sqlite --language rust
    python pipeline.py --source weather --export csv,sqlite
    python pipeline.py --source hackernews --export csv --dry-run
    python pipeline.py --source hackernews,github_trending --export csv,sqlite
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Bootstrap config and logging before other imports
import config as cfg
cfg.configure_logging()

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich import print as rprint

from models import BaseRecord
from processors.cleaner import clean_records
from processors.deduplicator import deduplicate_records
from processors.transformer import transform_records, validate_schema

log = logging.getLogger(__name__)
console = Console()

SUPPORTED_SOURCES = ("hackernews", "github_trending", "weather")
SUPPORTED_EXPORTERS = ("csv", "sqlite", "sheets")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="Modular data pipeline: fetch → validate → clean → deduplicate → transform → export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python pipeline.py --source hackernews --export csv
  python pipeline.py --source hackernews --export csv,sqlite --limit 50
  python pipeline.py --source github_trending --language rust --export csv
  python pipeline.py --source weather --cities "London,Tokyo" --export csv,sqlite
  python pipeline.py --source hackernews,github_trending --export csv --dry-run
        """,
    )

    # Sources
    parser.add_argument(
        "--source", "-s",
        required=True,
        help=f"Data source(s), comma-separated. Options: {', '.join(SUPPORTED_SOURCES)}",
    )

    # Exporters
    parser.add_argument(
        "--export", "-e",
        default="csv",
        help=f"Export format(s), comma-separated. Options: {', '.join(SUPPORTED_EXPORTERS)}. Default: csv",
    )

    # Source-specific options
    parser.add_argument("--limit", "-n", type=int, default=None,
                        help="Max records to fetch per source (overrides .env)")
    parser.add_argument("--feed", default=None,
                        help="HackerNews feed: top, new, best (default: top)")
    parser.add_argument("--language", default=None,
                        help="GitHub: programming language filter (default: python)")
    parser.add_argument("--timeframe", default=None,
                        help="GitHub: daily, weekly, monthly (default: weekly)")
    parser.add_argument("--cities", default=None,
                        help='Weather: comma-separated city names (e.g. "London,Tokyo")')

    # Processing options
    parser.add_argument("--missing", default=None,
                        choices=["drop", "fill", "flag"],
                        help="Missing value strategy (default: flag)")
    parser.add_argument("--dedup-threshold", type=float, default=None,
                        help="Fuzzy dedup similarity threshold 0-1 (default: 0.85)")
    parser.add_argument("--skip-dedup", action="store_true",
                        help="Disable deduplication step")
    parser.add_argument("--skip-transform", action="store_true",
                        help="Disable transformation step")

    # Export options
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (overrides .env EXPORT_DIR)")
    parser.add_argument("--db-path", default=None,
                        help="SQLite database path (overrides .env)")

    # General
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and process but do not write output files")
    parser.add_argument("--log-level", default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level (default: INFO)")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Suppress progress display (useful in scripts)")

    return parser


# ---------------------------------------------------------------------------
# Source dispatch
# ---------------------------------------------------------------------------

def fetch_source(source: str, args: argparse.Namespace) -> list[BaseRecord]:
    """Fetch records from the named source, returning a list."""
    cities = [c.strip() for c in args.cities.split(",")] if args.cities else None

    if source == "hackernews":
        from sources.hackernews import fetch
        return list(fetch(limit=args.limit, feed=args.feed))

    if source == "github_trending":
        from sources.github_trending import fetch
        return list(fetch(language=args.language, timeframe=args.timeframe, limit=args.limit))

    if source == "weather":
        from sources.weather import fetch
        return list(fetch(cities=cities))

    raise ValueError(f"Unknown source: {source!r}. Supported: {', '.join(SUPPORTED_SOURCES)}")


# ---------------------------------------------------------------------------
# Exporter dispatch
# ---------------------------------------------------------------------------

def run_exporter(
    exporter_name: str,
    records: list[BaseRecord],
    source: str,
    args: argparse.Namespace,
) -> str:
    """Run a named exporter and return a human-readable result string."""
    if exporter_name == "csv":
        from exporters.csv_exporter import export_csv
        path = export_csv(records, source=source)
        return str(path)

    if exporter_name == "sqlite":
        from exporters.sqlite_exporter import export_sqlite
        db_path = Path(args.db_path) if args.db_path else None
        count = export_sqlite(records, table_name=source, db_path=db_path)
        db = str(db_path or cfg.export.sqlite_db_path)
        return f"{count} rows → {db} (table: {source})"

    if exporter_name == "sheets":
        from exporters.sheets_exporter import export_sheets
        ok = export_sheets(records, sheet_name=source)
        return "✓ exported" if ok else "⚠ skipped (not configured)"

    raise ValueError(f"Unknown exporter: {exporter_name!r}")


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> int:
    """
    Execute the full pipeline and return an exit code (0 = success).
    """
    # Override config from CLI args
    if args.log_level:
        cfg.configure_logging(args.log_level)
    if args.output_dir:
        cfg.export.export_dir = Path(args.output_dir)

    sources = [s.strip() for s in args.source.split(",")]
    exporters = [e.strip() for e in args.export.split(",")]

    # Validate inputs early
    for s in sources:
        if s not in SUPPORTED_SOURCES:
            console.print(f"[red]Unknown source: {s!r}. Supported: {', '.join(SUPPORTED_SOURCES)}[/red]")
            return 1
    for e in exporters:
        if e not in SUPPORTED_EXPORTERS:
            console.print(f"[red]Unknown exporter: {e!r}. Supported: {', '.join(SUPPORTED_EXPORTERS)}[/red]")
            return 1

    if not args.quiet:
        console.print(Panel.fit(
            f"[bold cyan]Data Pipeline Toolkit[/bold cyan]\n"
            f"Sources: [green]{', '.join(sources)}[/green]  |  "
            f"Exporters: [yellow]{', '.join(exporters)}[/yellow]"
            + ("  |  [red]DRY RUN[/red]" if args.dry_run else ""),
            border_style="blue",
        ))

    all_records: list[BaseRecord] = []
    stage_stats: dict[str, Any] = {}
    start_time = datetime.utcnow()

    # ------------------------------------------------------------------ #
    # Stage 1: Fetch
    # ------------------------------------------------------------------ #
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        disable=args.quiet,
    ) as progress:
        for source in sources:
            task = progress.add_task(f"[cyan]Fetching {source}…", total=None)
            try:
                records = fetch_source(source, args)
                all_records.extend(records)
                stage_stats.setdefault("fetched", {})[source] = len(records)
                progress.update(task, description=f"[green]✓ {source}: {len(records)} records")
            except Exception as exc:
                progress.update(task, description=f"[red]✗ {source}: {exc}")
                log.error("Fetch failed for source '%s': %s", source, exc)

    if not all_records:
        console.print("[red]No records fetched. Check network and source configuration.[/red]")
        return 1

    if not args.quiet:
        console.print(f"  Total fetched: [bold]{len(all_records)}[/bold] records from {len(sources)} source(s)")

    # ------------------------------------------------------------------ #
    # Stage 2: Validate (schema check — informational only)
    # ------------------------------------------------------------------ #
    schema_report = validate_schema(all_records)
    incomplete_fields = {
        k: v for k, v in schema_report.items()
        if v["pct_complete"] < 50 and k not in ("record_hash",)
    }
    if incomplete_fields and not args.quiet:
        console.print(f"  [yellow]⚠ {len(incomplete_fields)} fields < 50% complete:[/yellow] "
                      + ", ".join(incomplete_fields.keys()))

    # ------------------------------------------------------------------ #
    # Stage 3: Clean
    # ------------------------------------------------------------------ #
    with _stage_spinner("Cleaning…", args.quiet):
        cleaned, dropped = clean_records(all_records, missing_strategy=args.missing)
    stage_stats["cleaned"] = len(cleaned)
    stage_stats["dropped"] = dropped
    if not args.quiet:
        console.print(f"  Cleaned: [bold]{len(cleaned)}[/bold] records ({dropped} dropped)")

    # ------------------------------------------------------------------ #
    # Stage 4: Deduplicate
    # ------------------------------------------------------------------ #
    if not args.skip_dedup:
        with _stage_spinner("Deduplicating…", args.quiet):
            deduped, report = deduplicate_records(
                cleaned, similarity_threshold=args.dedup_threshold
            )
        stage_stats["deduped"] = len(deduped)
        stage_stats["exact_dups"] = report.exact_duplicates
        stage_stats["fuzzy_dups"] = report.fuzzy_duplicates
        if not args.quiet:
            console.print(
                f"  Deduplicated: [bold]{len(deduped)}[/bold] records "
                f"({report.exact_duplicates} exact + {report.fuzzy_duplicates} fuzzy removed)"
            )
    else:
        deduped = cleaned

    # ------------------------------------------------------------------ #
    # Stage 5: Transform
    # ------------------------------------------------------------------ #
    if not args.skip_transform:
        with _stage_spinner("Transforming…", args.quiet):
            transformed = transform_records(deduped)
        if not args.quiet:
            console.print(f"  Transformed: [bold]{len(transformed)}[/bold] records")
    else:
        transformed = deduped

    # ------------------------------------------------------------------ #
    # Stage 6: Export
    # ------------------------------------------------------------------ #
    if args.dry_run:
        if not args.quiet:
            console.print("[yellow]  DRY RUN — skipping export[/yellow]")
    else:
        # Group by source for per-source export (cleaner filenames/tables)
        source_groups: dict[str, list[BaseRecord]] = {}
        for rec in transformed:
            source_groups.setdefault(rec.source, []).append(rec)

        export_results: list[tuple[str, str, str]] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            disable=args.quiet,
        ) as progress:
            total_exports = len(source_groups) * len(exporters)
            task = progress.add_task("[yellow]Exporting…", total=total_exports)

            for src_name, src_records in source_groups.items():
                for exp_name in exporters:
                    progress.update(task, description=f"[yellow]→ {exp_name}: {src_name}")
                    try:
                        result = run_exporter(exp_name, src_records, src_name, args)
                        export_results.append((exp_name, src_name, result))
                    except Exception as exc:
                        log.error("Export '%s' failed for source '%s': %s", exp_name, src_name, exc)
                        export_results.append((exp_name, src_name, f"ERROR: {exc}"))
                    progress.advance(task)

        # Summary table
        if not args.quiet:
            tbl = Table(title="Export Results", show_header=True, header_style="bold magenta")
            tbl.add_column("Exporter", style="cyan")
            tbl.add_column("Source", style="green")
            tbl.add_column("Result")
            for exp_name, src_name, result in export_results:
                tbl.add_row(exp_name, src_name, result)
            console.print(tbl)

    # ------------------------------------------------------------------ #
    # Final summary
    # ------------------------------------------------------------------ #
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    if not args.quiet:
        console.print(
            f"\n[bold green]Pipeline complete[/bold green] in {elapsed:.1f}s — "
            f"{len(transformed)} records ready."
        )

    return 0


# ---------------------------------------------------------------------------
# Spinner context helper
# ---------------------------------------------------------------------------

from contextlib import contextmanager
import time as _time

@contextmanager
def _stage_spinner(description: str, quiet: bool):
    if not quiet:
        console.print(f"  [dim]{description}[/dim]", end="")
    yield
    if not quiet:
        print()  # newline after inline print


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
