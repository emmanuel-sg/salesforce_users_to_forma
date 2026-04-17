"""Strict CSV validation for bulk-import files (header, columns, access level, roles)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from .normalize import normalize_display


EXPECTED_HEADER = (
    "first_name",
    "last_name",
    "email",
    "project_name",
    "roles",
    "company",
    "access_level",
)

_ACCESS_LEVEL_CANON = {"member": "Member", "administrator": "Administrator"}


def canonical_access_level(raw: str) -> str | None:
    """Return Member | Administrator, or None if invalid."""
    key = normalize_display(raw).casefold()
    return _ACCESS_LEVEL_CANON.get(key)


@dataclass(frozen=True)
class ValidatedRow:
    """One syntactically valid data row after header and access-level checks."""

    first_name: str
    last_name: str
    email: str
    project_name: str
    roles: list[str]
    company: str
    access_level: str  # canonical: Member | Administrator


@dataclass(frozen=True)
class ValidationSummary:
    """Running counts while scanning a CSV file."""

    processed: int = 0
    valid: int = 0
    skipped: int = 0
    failed: int = 0


def iter_csv_files(input_dir: Path) -> list[Path]:
    """Return sorted ``.csv`` files in a directory (empty list if missing)."""
    if not input_dir.exists():
        return []
    return sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])


def _parse_roles(raw: str) -> list[str]:
    """Split semicolon-separated role labels and trim/normalize each piece."""
    parts = [normalize_display(p) for p in raw.split(";")]
    return [p for p in parts if p]


def validate_csv_file(
    path: Path,
    *,
    on_valid_row,
    on_row_error,
    on_file_error,
) -> ValidationSummary:
    """Stream-parse ``path``; invoke callbacks per valid row, bad row, or file-level error."""
    summary = ValidationSummary()

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                on_file_error(path, "CSV is empty (missing header row)")
                return summary

            header_norm = tuple([h.strip() for h in header])
            if header_norm != EXPECTED_HEADER:
                on_file_error(
                    path,
                    f"Invalid header. Expected: {','.join(EXPECTED_HEADER)}",
                )
                return summary

            for row_idx, row in enumerate(reader, start=2):
                summary = ValidationSummary(
                    processed=summary.processed + 1,
                    valid=summary.valid,
                    skipped=summary.skipped,
                    failed=summary.failed,
                )

                if len(row) != len(EXPECTED_HEADER):
                    summary = ValidationSummary(
                        processed=summary.processed,
                        valid=summary.valid,
                        skipped=summary.skipped + 1,
                        failed=summary.failed,
                    )
                    on_row_error(
                        path,
                        row_idx,
                        None,
                        None,
                        f"Invalid column count: expected {len(EXPECTED_HEADER)} got {len(row)}",
                    )
                    continue

                first_name, last_name, email, project_name, roles_raw, company, access_level_raw = [
                    normalize_display(v) for v in row
                ]

                access_level = canonical_access_level(access_level_raw)
                if access_level is None:
                    summary = ValidationSummary(
                        processed=summary.processed,
                        valid=summary.valid,
                        skipped=summary.skipped + 1,
                        failed=summary.failed,
                    )
                    on_row_error(
                        path,
                        row_idx,
                        email or None,
                        project_name or None,
                        "Invalid access_level (allowed: Member, Administrator)",
                    )
                    continue

                roles = _parse_roles(roles_raw)

                validated = ValidatedRow(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    project_name=normalize_display(project_name),
                    roles=roles,
                    company=normalize_display(company),
                    access_level=access_level,
                )
                summary = ValidationSummary(
                    processed=summary.processed,
                    valid=summary.valid + 1,
                    skipped=summary.skipped,
                    failed=summary.failed,
                )
                on_valid_row(path, row_idx, validated)

    except Exception as e:  # noqa: BLE001
        summary = ValidationSummary(
            processed=summary.processed,
            valid=summary.valid,
            skipped=summary.skipped,
            failed=summary.failed + 1,
        )
        on_file_error(path, f"Failed to read CSV: {e}")

    return summary

