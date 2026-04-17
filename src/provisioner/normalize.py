"""String normalization for case-insensitive CSV and cache lookups (project, company, role names)."""

from __future__ import annotations


def normalize_key(value: str) -> str:
    """Collapse whitespace and apply casefold for stable, case-insensitive matching keys."""
    return " ".join(value.strip().split()).casefold()


def normalize_display(value: str) -> str:
    """Collapse repeated whitespace while preserving readable mixed-case text for display and emails."""
    return " ".join(value.strip().split())
