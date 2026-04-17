"""Data Management Project API: list hubs/projects and cache projects in SQLite."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .config import HubConfig
from .db import connect, init_db, upsert_project
from .normalize import normalize_display, normalize_key

DM_HUBS_URL = "https://developer.api.autodesk.com/project/v1/hubs"


@dataclass(frozen=True)
class Project:
    """ACC/Data Management project identifier and display name."""

    project_id: str
    project_name: str


def fetch_dm_hubs(*, access_token: str) -> list[tuple[str, str]]:
    """List Data Management hubs (id, name). Follows JSON:API `links.next` pagination."""
    url: str | None = DM_HUBS_URL
    out: list[tuple[str, str]] = []
    headers = {"Authorization": f"Bearer {access_token}"}
    while url:
        resp = requests.get(url, headers=headers, timeout=120)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        for item in data.get("data", []):
            pid = item.get("id")
            name = (item.get("attributes") or {}).get("name") or ""
            if pid:
                out.append((str(pid), str(name)))
        nxt = (data.get("links") or {}).get("next")
        url = nxt if isinstance(nxt, str) and nxt.strip() else None
    return out


def resolve_dm_hub_id(*, hub: HubConfig, access_token: str) -> str:
    """
    Hub id for GET /project/v1/hubs/{hub_id}/projects.

    Prefer ``HUB_*_DM_HUB_ID`` when set (usually ``b.<uuid>``). Otherwise, fall back to
    ``HUB_*_ID`` if it already looks like a DM id, and finally try ``b.<uuid>`` from
    the Construction Admin account id.
    """
    _ = access_token  # kept for backwards-compatible call sites
    if hub.dm_hub_id and hub.dm_hub_id.strip():
        return hub.dm_hub_id.strip()
    raw = hub.hub_id.strip()
    if raw.startswith("b.") and len(raw) > 2:
        return raw
    return f"b.{raw}"


def fetch_projects_from_aps(*, hub_id: str, access_token: str) -> list[Project]:
    """
    Fetch all projects for a Data Management hub (paginated via JSON:API `links.next`).
    """
    url: str | None = f"https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects"
    projects: list[Project] = []
    headers = {"Authorization": f"Bearer {access_token}"}
    while url:
        resp = requests.get(url, headers=headers, timeout=120)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        for item in data.get("data", []):
            pid = item.get("id")
            name = (item.get("attributes") or {}).get("name")
            if pid and name:
                projects.append(Project(project_id=str(pid), project_name=str(name)))
        nxt = (data.get("links") or {}).get("next")
        url = nxt if isinstance(nxt, str) and nxt.strip() else None
    return projects


def load_projects_from_json(path: Path) -> list[Project]:
    """
    Offline project list: accepts a JSON array of ``{project_id, project_name}`` or an object
    with a ``projects`` array (optional ``_comment`` / metadata keys ignored).
    """
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        items = raw.get("projects")
        if not isinstance(items, list):
            items = []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []
    projects: list[Project] = []
    for item in items:
        if isinstance(item, dict) and "project_id" in item and "project_name" in item:
            projects.append(Project(project_id=str(item["project_id"]), project_name=str(item["project_name"])))
    return projects


def cache_projects(
    *,
    db_path: Path,
    hub_id: str | None,
    projects: list[Project],
) -> int:
    """Upsert each :class:`Project` under ``hub_id`` in SQLite; return count."""
    init_db(db_path)
    with connect(db_path) as conn:
        for p in projects:
            name = normalize_display(p.project_name)
            upsert_project(
                conn,
                project_id=p.project_id,
                hub_id=hub_id,
                project_name=name,
                project_name_norm=normalize_key(name),
            )
    return len(projects)

