"""
HackerNews API fetcher.

Uses the public Firebase REST API — no authentication required.
Docs: https://github.com/HackerNews/API
"""

from __future__ import annotations

import logging
from typing import Any, Generator

import httpx

import config as cfg
from models import HackerNewsRecord
from utils import rate_limit, retry

log = logging.getLogger(__name__)

BASE_URL = cfg.hackernews.base_url
FEED_MAP = {
    "top": "topstories",
    "new": "newstories",
    "best": "beststories",
    "topstories": "topstories",
    "newstories": "newstories",
    "beststories": "beststories",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@retry()
@rate_limit()
def _get_json(client: httpx.Client, url: str) -> Any:
    """Make a GET request and return parsed JSON."""
    resp = client.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _fetch_story_ids(client: httpx.Client, feed: str) -> list[int]:
    """Return the list of story IDs for the given feed name."""
    endpoint = FEED_MAP.get(feed, "topstories")
    url = f"{BASE_URL}/{endpoint}.json"
    log.debug("Fetching story list from %s", url)
    ids = _get_json(client, url)
    if not isinstance(ids, list):
        raise ValueError(f"Expected a list of IDs, got {type(ids)}")
    return ids


def _fetch_item(client: httpx.Client, item_id: int) -> dict[str, Any] | None:
    """Fetch a single HN item by ID. Returns None if the item is deleted/missing."""
    url = f"{BASE_URL}/item/{item_id}.json"
    data = _get_json(client, url)
    if not isinstance(data, dict):
        return None
    return data


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch(
    limit: int | None = None,
    feed: str | None = None,
) -> Generator[HackerNewsRecord, None, None]:
    """
    Fetch stories from HackerNews.

    Args:
        limit: Maximum number of stories to return. Defaults to config value.
        feed: Feed to use ('top', 'new', 'best'). Defaults to config value.

    Yields:
        HackerNewsRecord instances, one per story.
    """
    max_items = limit if limit is not None else cfg.hackernews.limit
    feed_name = feed or cfg.hackernews.feed
    log.info("Fetching up to %d stories from HN feed '%s'", max_items, feed_name)

    fetched = 0
    skipped = 0

    with httpx.Client(headers={"User-Agent": "data-pipeline-demo/1.0"}) as client:
        story_ids = _fetch_story_ids(client, feed_name)
        log.debug("Feed returned %d story IDs", len(story_ids))

        for item_id in story_ids:
            if fetched >= max_items:
                break

            raw = _fetch_item(client, item_id)
            if raw is None:
                skipped += 1
                continue

            # We only want actual stories (not jobs, comments, polls, etc.)
            if raw.get("type") not in ("story", "job"):
                skipped += 1
                continue

            # Skip stories without a title (shouldn't happen, but be defensive)
            if not raw.get("title"):
                skipped += 1
                continue

            try:
                record = HackerNewsRecord(
                    hn_id=raw["id"],
                    title=raw.get("title", ""),
                    url=raw.get("url"),
                    score=raw.get("score", 0),
                    author=raw.get("by"),
                    descendants=raw.get("descendants", 0),
                    type=raw.get("type", "story"),
                    posted_at=raw.get("time"),
                )
                yield record
                fetched += 1
            except Exception as exc:
                log.warning("Could not parse HN item %d: %s", item_id, exc)
                skipped += 1

    log.info("HackerNews: fetched %d records, skipped %d", fetched, skipped)
