"""Write Phase 8 import dry-run / outcome reports as CSV or JSON."""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .import_plan import ImportDiffSummary, ImportRowPlan


def write_import_report(
    path: Path,
    *,
    hub_id: str,
    csv_path: str,
    diff_summary: ImportDiffSummary | None,
    validation_skips: Sequence[dict[str, Any]],
    row_plans: Sequence[ImportRowPlan],
    dry_run: bool,
    skip_diff: bool,
    fetch_dropped_users: int = 0,
    post_import: dict[str, Any] | None = None,
) -> None:
    """
    Write a human-friendly report of what the import did (or would do).

    This function is used by the CLI when you pass `--report ...` to `import-csv`.
    It produces **either JSON or CSV**, depending on the output filename:

    - `*.json`: writes one structured JSON object with run metadata plus full details
    - anything else (typically `*.csv`): writes a flat table that is easy to filter/sort in Excel

    The report combines three sources of information:
    - **validation_skips**: rows that were ignored because the CSV row was invalid or could not be mapped
      (e.g. project/company/role not found in SQLite)
    - **row_plans**: per-row planned actions (`add`, `update`, `skip`, `would_post`, ...)
    - **diff_summary / post_import**: optional high-level counts and batch stats

    Notes:
    - `dry_run=True` means no ACC API calls were made.
    - `skip_diff=True` means we did not compare desired state vs `project_user_cache`.
    - `fetch_dropped_users` counts CSV users that were dropped because list-users failed for a project
      (only relevant when diffing against the live API).
    """
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()

    validation_rows = [
        {
            "csv_row": x.get("csv_row"),
            "source_file": x.get("source_file", ""),
            "email": x.get("email", ""),
            "project_name": x.get("project_name", ""),
            "project_id": "",
            "action": "validation_skipped",
            "note": str(x.get("reason", "")),
        }
        for x in validation_skips
    ]
    plan_rows = [asdict(p) for p in row_plans]
    for pr in plan_rows:
        pr.setdefault("note", "")

    payload = {
        "hub_id": hub_id,
        "csv": csv_path,
        "dry_run": dry_run,
        "skip_diff": skip_diff,
        "fetch_dropped_users": fetch_dropped_users,
        "diff_summary": asdict(diff_summary) if diff_summary is not None else None,
        "validation_skips": list(validation_skips),
        "planned_rows": plan_rows,
        "post_import": post_import,
    }

    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return

    fieldnames = [
        "csv_row",
        "source_file",
        "email",
        "project_name",
        "project_id",
        "action",
        "note",
    ]
    all_rows = validation_rows + plan_rows
    all_rows.sort(key=lambda r: (int(r["csv_row"] or 0), str(r.get("action", ""))))

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in all_rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
