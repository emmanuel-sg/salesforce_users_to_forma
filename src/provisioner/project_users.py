"""Fetch Construction Admin project user lists and normalize them for SQLite ``project_user_cache``."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests

from .config import construction_admin_request_headers
from .normalize import normalize_display, normalize_key

LIST_USERS_TMPL = "https://developer.api.autodesk.com/construction/admin/v1/projects/{project_id}/users"


def _should_retry_status(code: int) -> bool:
    """True when HTTP status should trigger a retry (429 or 5xx)."""
    return code == 429 or code >= 500


@dataclass(frozen=True)
class ProjectMemberSnapshot:
    """Normalized member fields parsed from one API list-user item."""
    email_norm: str
    company_id: str
    role_ids: tuple[str, ...]
    company_admin: bool

    def to_cache_row(self) -> tuple[str, str, str, int]:
        """Shape one row for :func:`provisioner.db.replace_project_user_cache`."""
        return (
            self.email_norm,
            self.company_id,
            json.dumps(list(self.role_ids)),
            1 if self.company_admin else 0,
        )


def _attrs_blob(item: dict[str, Any]) -> dict[str, Any]:
    """Merge JSON:API ``attributes`` with top-level id fallback for downstream parsers."""
    a = item.get("attributes")
    if isinstance(a, dict):
        out = dict(a)
        if "id" not in out and item.get("id"):
            out.setdefault("id", item.get("id"))
        return out
    return item


def parse_project_user_item(item: dict[str, Any]) -> ProjectMemberSnapshot | None:
    """Map one list-users payload element to a snapshot, or ``None`` if not a valid email user."""
    a = _attrs_blob(item)
    email = a.get("email") or a.get("userEmail") or a.get("userId")
    if not email or "@" not in str(email):
        return None
    email_s = normalize_display(str(email))
    company_id = str(a.get("companyId") or a.get("company_id") or "")
    raw_roles = a.get("roleIds") or a.get("roles") or []
    role_ids: list[str] = []
    if isinstance(raw_roles, list):
        for r in raw_roles:
            if isinstance(r, dict):
                rid = r.get("id") or r.get("roleId")
                if rid:
                    role_ids.append(str(rid))
            elif r:
                role_ids.append(str(r))
    role_ids = sorted(set(role_ids))
    admin = bool(
        a.get("companyAdmin")
        or a.get("company_admin")
        or a.get("projectAdmin")
        or a.get("project_admin")
    )
    return ProjectMemberSnapshot(
        email_norm=normalize_key(email_s),
        company_id=company_id,
        role_ids=tuple(role_ids),
        company_admin=admin,
    )


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the first list-valued collection commonly used for user arrays in API responses."""
    for key in ("results", "data", "users", "items"):
        v = payload.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return []


def fetch_project_users_for_cache(
    project_id: str,
    *,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    max_retries: int,
    base_backoff_seconds: float,
    logger: logging.Logger,
    page_limit: int = 100,
) -> list[tuple[str, str, str, int]] | None:
    """
    GET all project users from APS and return rows for project_user_cache.
    Returns None if the list could not be retrieved after retries.
    """
    token = access_token
    auth_retried = False
    offset = 0
    snapshots: list[ProjectMemberSnapshot] = []

    while True:
        url = f"{LIST_USERS_TMPL.format(project_id=project_id)}?limit={page_limit}&offset={offset}"
        transient_attempt = 0
        resp: requests.Response | None = None

        while transient_attempt < max_retries:
            headers = construction_admin_request_headers(access_token=token)
            try:
                resp = requests.get(url, headers=headers, timeout=120)
            except requests.RequestException as e:
                logger.warning(
                    "List users request error",
                    extra={"extras": {"project_id": project_id, "error": str(e)}},
                )
                transient_attempt += 1
                if transient_attempt >= max_retries:
                    return None
                time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
                continue

            code = resp.status_code
            if code == 401 and refresh_access_token and not auth_retried:
                try:
                    token = refresh_access_token()
                    auth_retried = True
                    logger.info("Refreshed token after 401 (list users)", extra={"extras": {"project_id": project_id}})
                except Exception as e:  # noqa: BLE001
                    logger.error("Token refresh failed", extra={"extras": {"error": str(e)}})
                    return None
                continue

            if 200 <= code < 300:
                break

            if _should_retry_status(code):
                logger.warning(
                    "List users transient error",
                    extra={"extras": {"project_id": project_id, "status": code}},
                )
                transient_attempt += 1
                if transient_attempt >= max_retries:
                    return None
                time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
                continue

            logger.error(
                "List users failed (no retry)",
                extra={"extras": {"project_id": project_id, "status": code, "body": (resp.text or "")[:800]}},
            )
            return None

        if resp is None:
            return None

        try:
            payload: dict[str, Any] = resp.json()
        except Exception:  # noqa: BLE001
            logger.error("List users invalid JSON", extra={"extras": {"project_id": project_id}})
            return None

        items = _extract_items(payload)
        for it in items:
            snap = parse_project_user_item(it)
            if snap:
                snapshots.append(snap)

        if len(items) < page_limit:
            break
        offset += page_limit

    return [s.to_cache_row() for s in snapshots]
