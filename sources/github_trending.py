"""
GitHub trending repositories fetcher.

Uses the public GitHub Search API (no authentication required for low-volume use).
Optional GITHUB_TOKEN env var raises the rate limit from 10 to 30 req/min.

Docs: https://docs.github.com/en/rest/search/search
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Generator

import httpx

import config as cfg
from models import GitHubRepoRecord
from utils import rate_limit, retry

log = logging.getLogger(__name__)

BASE_URL = cfg.github.base_url

_TIMEFRAME_DAYS: dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_headers() -> dict[str, str]:
    headers: dict[str, str] = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "data-pipeline-demo/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if cfg.github.token:
        headers["Authorization"] = f"Bearer {cfg.github.token}"
    return headers


@retry()
@rate_limit(seconds=2.0)   # generous delay — unauthenticated limit is 10 req/min
def _search_repos(client: httpx.Client, query: str, per_page: int, page: int) -> dict[str, Any]:
    url = f"{BASE_URL}/search/repositories"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": min(per_page, 100),
        "page": page,
    }
    log.debug("GET %s params=%s", url, params)
    resp = client.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _build_query(language: str, timeframe: str) -> str:
    """Build a GitHub search query for trending repos in the given timeframe."""
    days = _TIMEFRAME_DAYS.get(timeframe, 7)
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    parts = [f"created:>={since}", "stars:>10"]
    if language and language.lower() not in ("any", "all", ""):
        parts.append(f"language:{language}")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch(
    language: str | None = None,
    timeframe: str | None = None,
    limit: int | None = None,
) -> Generator[GitHubRepoRecord, None, None]:
    """
    Fetch trending GitHub repositories.

    Args:
        language: Programming language filter (e.g. 'python', 'javascript').
        timeframe: 'daily', 'weekly', or 'monthly'.
        limit: Maximum number of repos to return.

    Yields:
        GitHubRepoRecord instances.
    """
    lang = language or cfg.github.language
    tf = timeframe or cfg.github.timeframe
    max_items = limit if limit is not None else cfg.github.limit

    query = _build_query(lang, tf)
    log.info("GitHub trending: query='%s', limit=%d", query, max_items)

    fetched = 0
    page = 1
    per_page = min(max_items, 100)

    with httpx.Client(headers=_build_headers()) as client:
        while fetched < max_items:
            try:
                data = _search_repos(client, query, per_page, page)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 403:
                    log.error("GitHub rate limit hit. Set GITHUB_TOKEN to increase the limit.")
                else:
                    log.error("GitHub API error: %s", exc)
                break

            items = data.get("items", [])
            if not items:
                log.debug("No more results at page %d", page)
                break

            for repo in items:
                if fetched >= max_items:
                    break
                try:
                    record = GitHubRepoRecord(
                        repo_name=repo["full_name"],
                        description=repo.get("description"),
                        stars=repo.get("stargazers_count", 0),
                        forks=repo.get("forks_count", 0),
                        language=repo.get("language"),
                        url=repo.get("html_url", ""),
                        topics=repo.get("topics", []),
                    )
                    yield record
                    fetched += 1
                except Exception as exc:
                    log.warning("Could not parse repo %s: %s", repo.get("full_name"), exc)

            page += 1
            if len(items) < per_page:
                break  # last page

    log.info("GitHub trending: fetched %d records", fetched)
