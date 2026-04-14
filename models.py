"""
Data models for the pipeline.

Pydantic v2 models for each source type with validation and serialization helpers.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class BaseRecord(BaseModel):
    """Common fields shared by every record that flows through the pipeline."""

    source: str = Field(..., description="Source identifier (e.g. 'hackernews')")
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    record_hash: Optional[str] = Field(None, description="SHA-256 of key fields, set by deduplicator")

    model_config = {"extra": "allow"}

    def compute_hash(self) -> str:
        """Return a stable SHA-256 fingerprint of the record's key fields."""
        # Subclasses can override _hash_fields() to control what's hashed.
        payload = json.dumps(self._hash_fields(), sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()

    def _hash_fields(self) -> dict[str, Any]:
        """Fields used for deduplication. Override in subclasses."""
        return self.model_dump(exclude={"fetched_at", "record_hash"})

    def to_flat_dict(self) -> dict[str, Any]:
        """Return a flat dictionary suitable for CSV / SQLite row.

        Includes both declared fields and any extra fields added by the transformer.
        """
        base = self.model_dump(mode="json")
        # Pydantic v2: extra fields are stored in __pydantic_extra__
        extras = self.__pydantic_extra__ or {}
        # Serialize any non-JSON-safe extras
        serialized_extras: dict[str, Any] = {}
        for k, v in extras.items():
            if isinstance(v, (str, int, float, bool, type(None))):
                serialized_extras[k] = v
            else:
                serialized_extras[k] = str(v)
        return {**base, **serialized_extras}


# ---------------------------------------------------------------------------
# HackerNews
# ---------------------------------------------------------------------------

class HackerNewsRecord(BaseRecord):
    """A single HackerNews story."""

    source: str = "hackernews"
    hn_id: int = Field(..., description="HN item ID")
    title: str
    url: Optional[str] = None
    score: int = 0
    author: Optional[str] = None
    comment_count: int = Field(0, alias="descendants")
    hn_type: str = Field("story", alias="type")
    posted_at: Optional[datetime] = None
    # Computed by transformer
    word_count: Optional[int] = None
    domain: Optional[str] = None

    model_config = {"populate_by_name": True, "extra": "ignore"}

    @field_validator("posted_at", mode="before")
    @classmethod
    def parse_posted_at(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.utcfromtimestamp(int(v))
        except (TypeError, ValueError):
            return None

    def _hash_fields(self) -> dict[str, Any]:
        return {"hn_id": self.hn_id, "source": self.source}


# ---------------------------------------------------------------------------
# GitHub Trending
# ---------------------------------------------------------------------------

class GitHubRepoRecord(BaseRecord):
    """A GitHub trending repository."""

    source: str = "github_trending"
    repo_name: str = Field(..., description="owner/repo")
    description: Optional[str] = None
    stars: int = 0
    forks: int = 0
    language: Optional[str] = None
    url: str = ""
    topics: list[str] = Field(default_factory=list)
    # Computed by transformer
    stars_per_fork_ratio: Optional[float] = None

    @field_validator("description", mode="before")
    @classmethod
    def truncate_description(cls, v: Any) -> Optional[str]:
        if v and len(v) > 512:
            return v[:509] + "..."
        return v

    def _hash_fields(self) -> dict[str, Any]:
        return {"repo_name": self.repo_name, "source": self.source}


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

class WeatherRecord(BaseRecord):
    """A weather observation or forecast row."""

    source: str = "weather"
    city: str
    latitude: float
    longitude: float
    observation_time: datetime
    temperature_c: Optional[float] = None
    apparent_temperature_c: Optional[float] = None
    precipitation_mm: Optional[float] = None
    wind_speed_kmh: Optional[float] = None
    wind_direction_deg: Optional[float] = None
    weather_code: Optional[int] = None
    is_forecast: bool = False

    def _hash_fields(self) -> dict[str, Any]:
        return {
            "city": self.city,
            "observation_time": self.observation_time.isoformat(),
            "source": self.source,
        }


# ---------------------------------------------------------------------------
# Union type used by the pipeline
# ---------------------------------------------------------------------------

PipelineRecord = HackerNewsRecord | GitHubRepoRecord | WeatherRecord
