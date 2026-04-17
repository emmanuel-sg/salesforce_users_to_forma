"""Compare desired import users to ``project_user_cache`` (ADD / UPDATE / SKIP) and build row plans."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import sqlite3

from .db import get_cached_project_member
from .normalize import normalize_key


@dataclass(frozen=True)
class ImportDiffSummary:
    add: int
    update: int
    skip_same: int


@dataclass(frozen=True)
class ImportRowPlan:
    """Per CSV-derived user after diff (or dry-run skip-diff)."""

    csv_row: int
    source_file: str
    email: str
    project_name: str
    project_id: str
    action: str
    note: str = ""


def dedupe_users_per_project(
    payloads: dict[str, list[dict[str, Any]]],
    *,
    logger: logging.Logger,
) -> dict[str, list[dict[str, Any]]]:
    """
    Remove duplicates within each project before importing.

    The CSV can contain the same email more than once for the same project. This
    function keeps **only one** entry per (project_id, email), using a "last row
    wins" rule. A warning is logged when a duplicate is found.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    for pid, users in payloads.items():
        by_email: dict[str, dict[str, Any]] = {}
        for u in users:
            en = normalize_key(str(u.get("email", "")))
            if en in by_email:
                logger.warning(
                    "Duplicate CSV row for same project+email; using last row",
                    extra={"extras": {"project_id": pid, "email": u.get("email")}},
                )
            by_email[en] = u
        out[pid] = list(by_email.values())
    return out


def _meta_for_user(u: dict[str, Any], *, project_id: str) -> tuple[int, str, str, str]:
    """
    Pull row-tracking info out of the internal user dict.

    The payload builder attaches a `_provisioner_meta` dict so we can trace a
    planned action back to the original CSV row and file. This helper returns:

    - csv_row (int)
    - source_file (str)
    - project_name (str as written in the CSV)
    - email (str)

    `project_id` is unused here; it is accepted only to keep call sites uniform.
    """
    m = u.get("_provisioner_meta")
    if isinstance(m, dict):
        row = int(m.get("csv_row") or 0)
        src = str(m.get("source_file") or "")
        pname = str(m.get("project_name") or "")
    else:
        row, src, pname = 0, "", ""
    return row, src, pname, str(u.get("email", ""))


def plans_for_skip_diff(payloads: dict[str, list[dict[str, Any]]]) -> list[ImportRowPlan]:
    """
    Build report rows for `--skip-diff`.

    With `--skip-diff`, the tool does not compare against the live/cached project
    membership. Every resolved user would be POSTed as-is, so this returns a
    "would_post" plan entry for each user.
    """
    out: list[ImportRowPlan] = []
    for project_id, users in payloads.items():
        for u in users:
            row, src, pname, email = _meta_for_user(u, project_id=project_id)
            out.append(
                ImportRowPlan(
                    csv_row=row,
                    source_file=src,
                    email=email,
                    project_name=pname,
                    project_id=project_id,
                    action="would_post",
                    note="skip-diff: not compared to cache",
                )
            )
    return out


def apply_import_diff(
    conn: sqlite3.Connection,
    payloads: dict[str, list[dict[str, Any]]],
    *,
    logger: logging.Logger,
) -> tuple[dict[str, list[dict[str, Any]]], ImportDiffSummary, list[ImportRowPlan]]:
    """
    Decide which users to send to ACC by comparing CSV intent vs cached current state.

    Inputs:
    - `payloads`: desired users grouped by project_id (built from CSV + SQLite lookups).
    - `conn`: SQLite connection with a recently refreshed `project_user_cache` table
      (usually filled by `sync-hub` or by the import flow itself right before diffing).

    For each user in each project we decide:
    - **add**: user not present in the cache for that project
    - **skip**: user present and company/roles/admin flag match exactly
    - **update**: user present but company/roles/admin flag differ

    Returns:
    - `filtered_payloads_for_import`: only ADD/UPDATE users (SKIP users removed)
    - `ImportDiffSummary`: counts for add/update/skip
    - `plans`: per-row action records for reporting/auditing
    """
    add = update = skip_same = 0
    filtered: dict[str, list[dict[str, Any]]] = {}
    plans: list[ImportRowPlan] = []

    for project_id, users in payloads.items():
        keep: list[dict[str, Any]] = []
        for u in users:
            email = str(u.get("email", ""))
            en = normalize_key(email)
            desired_company = str(u.get("companyId", ""))
            desired_roles = frozenset(str(x) for x in (u.get("roleIds") or []))
            desired_admin = bool(u.get("companyAdmin", False))
            row, src, pname, _ = _meta_for_user(u, project_id=project_id)

            cur = get_cached_project_member(conn, project_id=project_id, email_norm=en)
            if cur is None:
                add += 1
                keep.append(u)
                plans.append(
                    ImportRowPlan(
                        csv_row=row,
                        source_file=src,
                        email=email,
                        project_name=pname,
                        project_id=project_id,
                        action="add",
                        note="not in project_user_cache",
                    )
                )
                logger.info(
                    "Plan ADD user",
                    extra={"extras": {"project_id": project_id, "email": email}},
                )
                continue

            cur_roles = frozenset(str(x) for x in cur["role_ids"])
            same = (
                cur["company_id"] == desired_company
                and cur_roles == desired_roles
                and bool(cur["company_admin"]) == desired_admin
            )
            if same:
                skip_same += 1
                plans.append(
                    ImportRowPlan(
                        csv_row=row,
                        source_file=src,
                        email=email,
                        project_name=pname,
                        project_id=project_id,
                        action="skip",
                        note="unchanged vs cache",
                    )
                )
                logger.info(
                    "Plan SKIP user (unchanged)",
                    extra={"extras": {"project_id": project_id, "email": email}},
                )
            else:
                update += 1
                keep.append(u)
                plans.append(
                    ImportRowPlan(
                        csv_row=row,
                        source_file=src,
                        email=email,
                        project_name=pname,
                        project_id=project_id,
                        action="update",
                        note="differs from cache",
                    )
                )
                logger.info(
                    "Plan UPDATE user",
                    extra={"extras": {"project_id": project_id, "email": email}},
                )

        if keep:
            filtered[project_id] = keep

    return filtered, ImportDiffSummary(add=add, update=update, skip_same=skip_same), plans
