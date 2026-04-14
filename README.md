# Data Pipeline Toolkit

A modular, production-ready data pipeline for fetching, cleaning, deduplicating, and exporting data from public APIs — with zero mandatory API keys.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Features

- **Multi-source fetching** — HackerNews, GitHub Trending, Open-Meteo weather (more easy to add)
- **Smart deduplication** — exact (hash-based) + fuzzy (configurable similarity threshold)
- **Data cleaning** — unicode normalization, missing-value strategies (drop / fill / flag), type coercion
- **Enrichment** — computed columns, domain extraction, category tagging, score tiers
- **Multi-format export** — CSV, SQLite (upsert + indexed), Google Sheets (optional)
- **Retry & rate limiting** — decorators with exponential backoff, polite API delays
- **Dry-run mode** — validate the full pipeline without writing files
- **Configurable** — `.env` file or CLI flags, no hardcoded values
- **Extensible** — drop in a new source or exporter in ~50 lines

---

## Architecture

```
Sources              Processing               Export
┌──────────────┐   ┌─────────────────────┐   ┌──────────────┐
│ HackerNews   │──▶│ 1. Validate schema  │──▶│ CSV          │
│ GitHub       │──▶│ 2. Clean            │──▶│ SQLite       │
│ Weather      │──▶│ 3. Deduplicate      │──▶│ Google Sheets│
│ [Custom]     │──▶│ 4. Transform/enrich │──▶│ [Custom]     │
└──────────────┘   └─────────────────────┘   └──────────────┘
        ▲                                              │
        └──────────── pipeline.py (CLI) ───────────────┘
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/JustDreameritis/data-pipeline-demo.git
cd data-pipeline-demo

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. (Optional) Configure
cp .env.example .env
# Edit .env to taste — defaults work out of the box

# 5. Run
python pipeline.py --source hackernews --export csv
```

Output lands in `./output/hackernews.csv`.

---

## Usage Examples

```bash
# Fetch top 50 HackerNews stories → CSV
python pipeline.py --source hackernews --export csv --limit 50

# Fetch top 100 stories → CSV + SQLite
python pipeline.py --source hackernews --export csv,sqlite --limit 100

# Trending Rust repos → CSV
python pipeline.py --source github_trending --language rust --export csv

# Weekly Python repos → SQLite
python pipeline.py --source github_trending --language python --timeframe weekly --export sqlite

# Weather for specific cities → CSV + SQLite
python pipeline.py --source weather --cities "London,Tokyo,Berlin" --export csv,sqlite

# Run all sources at once
python pipeline.py --source hackernews,github_trending,weather --export csv,sqlite

# Dry run (no files written)
python pipeline.py --source hackernews --export csv --dry-run

# Use 'new' HN feed, flag missing values
python pipeline.py --source hackernews --feed new --missing flag --export csv

# Strict dedup (99% similarity required to merge)
python pipeline.py --source hackernews --dedup-threshold 0.99 --export csv

# Quiet mode (no progress bars — good for cron)
python pipeline.py --source hackernews --export csv --quiet
```

---

## Configuration Reference

All settings can be placed in a `.env` file (copy `.env.example` to start).
CLI flags override `.env` values at runtime.

| Variable | Default | Description |
|---|---|---|
| `HACKERNEWS_LIMIT` | `100` | Max HN stories to fetch |
| `HACKERNEWS_FEED` | `topstories` | `topstories` / `newstories` / `beststories` |
| `GITHUB_LANGUAGE` | `python` | Repo language filter (`any` for all) |
| `GITHUB_TIMEFRAME` | `weekly` | `daily` / `weekly` / `monthly` |
| `GITHUB_LIMIT` | `50` | Max repos to fetch |
| `GITHUB_TOKEN` | _(none)_ | Optional token to raise API rate limit |
| `WEATHER_CITIES` | `London,New York,Tokyo,Sydney` | Comma-separated city names |
| `WEATHER_FORECAST_DAYS` | `3` | Days of forecast data |
| `WEATHER_HISTORY_DAYS` | `0` | Days of historical data |
| `MISSING_VALUE_STRATEGY` | `flag` | `drop` / `fill` / `flag` |
| `DEDUP_SIMILARITY_THRESHOLD` | `0.85` | Fuzzy match threshold (0–1) |
| `EXPORT_DIR` | `./output` | Directory for CSV and DB files |
| `SQLITE_DB_PATH` | `./output/pipeline.db` | SQLite database path |
| `CSV_DELIMITER` | `,` | CSV column separator |
| `CSV_APPEND` | `false` | Append to existing CSV instead of overwrite |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `REQUEST_DELAY` | `0.5` | Seconds between API requests |
| `MAX_RETRIES` | `3` | Retry count for failed requests |

---

## Project Structure

```
data-pipeline-demo/
├── pipeline.py              # CLI orchestrator (entry point)
├── models.py                # Pydantic data models
├── config.py                # Configuration loader
├── utils.py                 # Shared utilities (rate limit, retry, logging)
│
├── sources/
│   ├── hackernews.py        # HackerNews Firebase API
│   ├── github_trending.py   # GitHub Search API
│   └── weather.py           # Open-Meteo (no key required)
│
├── processors/
│   ├── cleaner.py           # Unicode, whitespace, missing values
│   ├── deduplicator.py      # Hash-based + fuzzy dedup
│   └── transformer.py       # Computed columns, enrichment
│
├── exporters/
│   ├── csv_exporter.py      # CSV with append support
│   ├── sqlite_exporter.py   # SQLite with upsert + indexes
│   └── sheets_exporter.py   # Google Sheets (optional, stub)
│
├── docs/
│   └── SOW-template.md      # Statement of Work template
│
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Adding a Custom Source

1. Create `sources/my_source.py`
2. Implement a `fetch()` generator that yields `BaseRecord` subclasses
3. Define a Pydantic model in `models.py` (or reuse `BaseRecord` with extra fields)
4. Register the source name in `pipeline.py`'s `SUPPORTED_SOURCES` and `fetch_source()`

```python
# sources/my_source.py
from models import BaseRecord

class MyRecord(BaseRecord):
    source: str = "my_source"
    title: str
    value: float

def fetch(limit: int = 50):
    for item in my_api.get_items(limit=limit):
        yield MyRecord(title=item["title"], value=item["val"])
```

---

## Adding a Custom Exporter

1. Create `exporters/my_exporter.py`
2. Implement an `export(records, ...)` function
3. Register the name in `pipeline.py`'s `SUPPORTED_EXPORTERS` and `run_exporter()`

```python
# exporters/my_exporter.py
from models import BaseRecord

def export(records: list[BaseRecord], **kwargs) -> str:
    for rec in records:
        my_api.push(rec.to_flat_dict())
    return f"pushed {len(records)} records"
```

---

## Google Sheets Setup

See the detailed instructions in [`exporters/sheets_exporter.py`](exporters/sheets_exporter.py).

Summary:
1. Enable Sheets API + Drive API in Google Cloud Console
2. Create a Service Account and download the JSON key
3. Share your spreadsheet with the service account email
4. Set `GOOGLE_CREDENTIALS_PATH` and `GOOGLE_SPREADSHEET_ID` in `.env`
5. `pip install google-auth google-auth-httplib2 google-api-python-client`

---

## Tech Stack

| Library | Purpose |
|---|---|
| [httpx](https://www.python-httpx.org/) | Async-capable HTTP client |
| [Pydantic v2](https://docs.pydantic.dev/) | Data validation and serialization |
| [Rich](https://rich.readthedocs.io/) | Terminal progress bars and tables |
| [python-dotenv](https://pypi.org/project/python-dotenv/) | `.env` file loading |
| [RapidFuzz](https://github.com/maxbachmann/RapidFuzz) | Fast fuzzy string matching |

All APIs used are free and require no authentication:
- [HackerNews Firebase API](https://github.com/HackerNews/API)
- [GitHub Search API](https://docs.github.com/en/rest/search) (public, rate-limited)
- [Open-Meteo](https://open-meteo.com/) (no key, generous free tier)

---

## License

MIT — free to use, modify, and distribute.
