"""
Deduplication processor.

Supports:
- Exact duplicate detection via SHA-256 hash of key fields
- Fuzzy duplicate detection via RapidFuzz string similarity
- Merge strategy: keep newest (by fetched_at) or most complete (fewest Nones)
- Summary report of duplicates found
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz

import config as cfg
from models import BaseRecord

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dedup report
# ---------------------------------------------------------------------------

@dataclass
class DedupReport:
    total_in: int = 0
    exact_duplicates: int = 0
    fuzzy_duplicates: int = 0
    total_out: int = 0
    groups: list[list[str]] = field(default_factory=list)  # groups of duplicate hashes

    def summary(self) -> str:
        return (
            f"Dedup: {self.total_in} in → "
            f"{self.exact_duplicates} exact + {self.fuzzy_duplicates} fuzzy removed → "
            f"{self.total_out} out"
        )


# ---------------------------------------------------------------------------
# Merge strategies
# ---------------------------------------------------------------------------

def _count_nones(record: BaseRecord) -> int:
    """Count None values in a record's dict (lower = more complete)."""
    return sum(1 for v in record.model_dump().values() if v is None)


def merge_records(group: list[BaseRecord]) -> BaseRecord:
    """
    Merge a group of near-duplicate records into one.

    Strategy: keep the most complete record (fewest None fields).
    Tie-break: keep the one with the latest fetched_at.
    """
    return min(group, key=lambda r: (_count_nones(r), -(r.fetched_at.timestamp() if r.fetched_at else 0)))


# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

def _text_fingerprint(record: BaseRecord) -> str:
    """
    Return a single string representing the record's main textual content,
    used for fuzzy comparison.
    """
    data = record.model_dump()
    # Prefer 'title' for HN, 'repo_name' for GitHub, 'city+observation_time' for weather
    candidates = [
        data.get("title"),
        data.get("repo_name"),
        data.get("description"),
    ]
    parts = [str(c) for c in candidates if c]
    return " ".join(parts)[:500]  # cap length for performance


# ---------------------------------------------------------------------------
# Deduplicator
# ---------------------------------------------------------------------------

class Deduplicator:
    """
    Two-stage deduplicator: exact (hash-based) then fuzzy (similarity-based).

    Fuzzy dedup is O(n²) on the post-exact set, acceptable for typical
    pipeline sizes (<100k records). For larger sets, use a blocking key.
    """

    def __init__(self, similarity_threshold: float | None = None) -> None:
        self.threshold = (
            similarity_threshold
            if similarity_threshold is not None
            else cfg.processing.dedup_similarity_threshold
        )

    def deduplicate(self, records: list[BaseRecord]) -> tuple[list[BaseRecord], DedupReport]:
        """
        Deduplicate a list of records.

        Returns:
            (unique_records, report) tuple.
        """
        report = DedupReport(total_in=len(records))

        # Stage 1: exact dedup via hash
        after_exact, exact_removed = self._exact_dedup(records)
        report.exact_duplicates = exact_removed

        # Stage 2: fuzzy dedup
        after_fuzzy, fuzzy_removed, groups = self._fuzzy_dedup(after_exact)
        report.fuzzy_duplicates = fuzzy_removed
        report.groups = groups
        report.total_out = len(after_fuzzy)

        log.info(report.summary())
        return after_fuzzy, report

    # ------------------------------------------------------------------
    # Stage 1: exact
    # ------------------------------------------------------------------

    def _exact_dedup(self, records: list[BaseRecord]) -> tuple[list[BaseRecord], int]:
        seen_hashes: dict[str, BaseRecord] = {}
        removed = 0

        for rec in records:
            h = rec.compute_hash()
            rec.record_hash = h
            if h in seen_hashes:
                removed += 1
                # Keep the more complete of the two
                existing = seen_hashes[h]
                if _count_nones(rec) < _count_nones(existing):
                    seen_hashes[h] = rec
            else:
                seen_hashes[h] = rec

        unique = list(seen_hashes.values())
        log.debug("Exact dedup: %d → %d (%d removed)", len(records), len(unique), removed)
        return unique, removed

    # ------------------------------------------------------------------
    # Stage 2: fuzzy
    # ------------------------------------------------------------------

    def _fuzzy_dedup(
        self, records: list[BaseRecord]
    ) -> tuple[list[BaseRecord], int, list[list[str]]]:
        """
        Group near-duplicates by fuzzy string similarity and keep one per group.

        Returns (unique_records, removed_count, duplicate_groups).
        """
        if self.threshold >= 1.0:
            # Threshold of 1.0 means "exact only" — skip fuzzy stage
            return records, 0, []

        n = len(records)
        fingerprints = [_text_fingerprint(r) for r in records]

        # Union-find for grouping
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: int, y: int) -> None:
            parent[find(x)] = find(y)

        # Only run fuzzy on records that have meaningful text fingerprints
        # Skip weather records (they're time-series, not text)
        eligible = [
            i for i, r in enumerate(records)
            if r.source not in ("weather",) and fingerprints[i]
        ]

        for i in range(len(eligible)):
            for j in range(i + 1, len(eligible)):
                ii, jj = eligible[i], eligible[j]
                score = fuzz.token_sort_ratio(fingerprints[ii], fingerprints[jj]) / 100.0
                if score >= self.threshold:
                    union(ii, jj)

        # Collect groups
        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            groups.setdefault(root, []).append(i)

        unique: list[BaseRecord] = []
        dup_groups: list[list[str]] = []
        removed = 0

        for root, indices in groups.items():
            group_records = [records[i] for i in indices]
            best = merge_records(group_records)
            unique.append(best)
            if len(indices) > 1:
                removed += len(indices) - 1
                dup_groups.append([records[i].record_hash or str(i) for i in indices])

        log.debug("Fuzzy dedup: %d → %d (%d removed, threshold=%.2f)", n, len(unique), removed, self.threshold)
        return unique, removed, dup_groups


def deduplicate_records(
    records: list[BaseRecord],
    similarity_threshold: float | None = None,
) -> tuple[list[BaseRecord], DedupReport]:
    """Convenience function — instantiate Deduplicator and run."""
    deduplicator = Deduplicator(similarity_threshold=similarity_threshold)
    return deduplicator.deduplicate(records)
