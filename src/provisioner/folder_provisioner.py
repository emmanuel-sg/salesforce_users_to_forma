"""Create per-company folder structures and assign company permissions in ACC Docs.

This module is used by `import-csv` after successful user provisioning (real run),
and is designed to be idempotent.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import requests


DM_TOP_FOLDERS_TMPL = "https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects/{project_id}/topFolders"
DM_FOLDER_CONTENTS_TMPL = (
    "https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/contents"
)
DM_CREATE_FOLDER_URL = "https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders"

# BIM 360 / ACC Docs folder permissions APIs
DOCS_FOLDER_PERMS_GET_TMPL = (
    "https://developer.api.autodesk.com/bim360/docs/v1/projects/{project_id}/folders/{folder_id}/permissions"
)
DOCS_FOLDER_PERMS_BATCH_UPDATE_TMPL = (
    "https://developer.api.autodesk.com/bim360/docs/v1/projects/{project_id}/folders/{folder_id}/permissions:batch-update"
)


EDIT_ACTIONS = sorted({"VIEW", "DOWNLOAD", "UPLOAD", "PUBLISH", "EDIT", "COLLABORATE"})


def _should_retry_status(code: int) -> bool:
    return code == 429 or code >= 500


def _request_json(
    method: str,
    url: str,
    *,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    headers: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: int = 120,
    max_retries: int = 5,
    base_backoff_seconds: float = 1.0,
    logger: logging.Logger,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    token = access_token
    auth_retried = False
    transient_attempt = 0
    hdrs = dict(headers or {})

    while transient_attempt < max_retries:
        req_headers = dict(hdrs)
        req_headers["Authorization"] = f"Bearer {token}"
        try:
            resp = requests.request(
                method,
                url,
                headers=req_headers,
                json=json_body,
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.warning(
                "Folder provisioning request error; will retry if attempts remain",
                extra={"extras": {"url": url, "method": method, "error": str(e), **(extras or {})}},
            )
            transient_attempt += 1
            if transient_attempt >= max_retries:
                return None
            time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
            continue

        code = resp.status_code
        if 200 <= code < 300:
            if resp.text.strip() == "":
                return {}
            try:
                out = resp.json()
                return out if isinstance(out, dict) else {"_raw": out}
            except Exception:  # noqa: BLE001
                return {"_raw_text": (resp.text or "")[:2000]}

        if code == 401 and refresh_access_token and not auth_retried:
            try:
                token = refresh_access_token()
                auth_retried = True
                logger.info(
                    "Refreshed access token after 401 (folder provisioning)",
                    extra={"extras": {"url": url, **(extras or {})}},
                )
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "Token refresh failed after 401 (folder provisioning)",
                    extra={"extras": {"url": url, "error": str(e), **(extras or {})}},
                )
                return None
            continue

        if _should_retry_status(code):
            logger.warning(
                "Folder provisioning transient error; backing off",
                extra={
                    "extras": {
                        "url": url,
                        "method": method,
                        "status": code,
                        "transient_attempt": transient_attempt + 1,
                        "body_preview": (resp.text or "")[:800],
                        **(extras or {}),
                    }
                },
            )
            transient_attempt += 1
            if transient_attempt >= max_retries:
                return None
            time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
            continue

        logger.error(
            "Folder provisioning request failed (no retry)",
            extra={
                "extras": {
                    "url": url,
                    "method": method,
                    "status": code,
                    "body_preview": (resp.text or "")[:1200],
                    **(extras or {}),
                }
            },
        )
        return None

    return None


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _folder_name(item: dict[str, Any]) -> str:
    attrs = item.get("attributes")
    if isinstance(attrs, dict) and isinstance(attrs.get("name"), str):
        return attrs["name"]
    return ""


def _is_folder(item: dict[str, Any]) -> bool:
    return item.get("type") == "folders"


def get_project_root_folder_id(
    *,
    dm_hub_id: str,
    project_id: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
    dry_run: bool,
) -> str | None:
    """
    Return a top folder urn to use as "project root" for our structure.
    Prefers "Project Files" when present, else uses the first top folder.
    """
    if dry_run:
        logger.info(
            "DRY-RUN: would fetch top folders",
            extra={"extras": {"dm_hub_id": dm_hub_id, "project_id": project_id}},
        )
        return "DRY_RUN_ROOT"

    url = DM_TOP_FOLDERS_TMPL.format(hub_id=dm_hub_id, project_id=project_id)
    payload = _request_json(
        "GET",
        url,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        headers={"Accept": "application/vnd.api+json"},
        logger=logger,
        extras={"project_id": project_id},
    )
    if not payload:
        return None

    items = _extract_items(payload)
    folders = [x for x in items if _is_folder(x)]
    if not folders:
        return None
    for f in folders:
        if _folder_name(f).strip().casefold() == "project files":
            return str(f.get("id"))
    return str(folders[0].get("id"))


def list_child_folders(
    *,
    project_id: str,
    parent_folder_id: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
) -> dict[str, str]:
    """Return {child_name -> child_folder_id} for immediate child folders."""
    url = DM_FOLDER_CONTENTS_TMPL.format(project_id=project_id, folder_id=parent_folder_id)
    payload = _request_json(
        "GET",
        url,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        headers={"Accept": "application/vnd.api+json"},
        logger=logger,
        extras={"project_id": project_id, "parent_folder_id": parent_folder_id},
    )
    if not payload:
        return {}
    out: dict[str, str] = {}
    for item in _extract_items(payload):
        if not _is_folder(item):
            continue
        name = _folder_name(item).strip()
        fid = item.get("id")
        if name and fid:
            out[name] = str(fid)
    return out


def create_folder(
    *,
    project_id: str,
    parent_folder_id: str,
    name: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
    dry_run: bool,
) -> str | None:
    """Create a child folder and return its urn id."""
    if dry_run:
        logger.info(
            "DRY-RUN: would create folder",
            extra={
                "extras": {
                    "project_id": project_id,
                    "parent_folder_id": parent_folder_id,
                    "name": name,
                }
            },
        )
        return f"DRY_RUN_FOLDER:{name}"

    url = DM_CREATE_FOLDER_URL.format(project_id=project_id)
    body = {
        "jsonapi": {"version": "1.0"},
        "data": {
            "type": "folders",
            "attributes": {
                "name": name,
                "extension": {"type": "folders:autodesk.bim360:Folder", "version": "1.0"},
            },
            "relationships": {"parent": {"data": {"type": "folders", "id": parent_folder_id}}},
        },
    }
    payload = _request_json(
        "POST",
        url,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        headers={
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
        },
        json_body=body,
        logger=logger,
        extras={"project_id": project_id, "parent_folder_id": parent_folder_id, "name": name},
    )
    if not payload:
        return None
    data = payload.get("data")
    if isinstance(data, dict) and data.get("id"):
        return str(data["id"])
    return None


def ensure_folder_path(
    *,
    project_id: str,
    root_folder_id: str,
    segments: list[str],
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
    dry_run: bool,
) -> str | None:
    """Ensure a nested folder path exists and return the final folder id."""
    cur = root_folder_id
    for name in segments:
        if dry_run:
            logger.info(
                "DRY-RUN: would ensure folder exists",
                extra={"extras": {"project_id": project_id, "parent_folder_id": cur, "name": name}},
            )
            cur = f"{cur}/{name}"
            continue

        children = list_child_folders(
            project_id=project_id,
            parent_folder_id=cur,
            access_token=access_token,
            refresh_access_token=refresh_access_token,
            logger=logger,
        )
        existing = children.get(name)
        if existing:
            cur = existing
            continue

        created = create_folder(
            project_id=project_id,
            parent_folder_id=cur,
            name=name,
            access_token=access_token,
            refresh_access_token=refresh_access_token,
            logger=logger,
            dry_run=dry_run,
        )
        if not created:
            return None
        cur = created
    return cur


def get_company_permission_actions(
    *,
    project_id: str,
    folder_id: str,
    company_id: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
) -> list[str] | None:
    url = DOCS_FOLDER_PERMS_GET_TMPL.format(project_id=project_id, folder_id=folder_id)
    payload = _request_json(
        "GET",
        url,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        headers={"Accept": "application/json"},
        logger=logger,
        extras={"project_id": project_id, "folder_id": folder_id, "company_id": company_id},
    )
    if payload is None:
        return None
    perms = payload.get("permissions")
    if not isinstance(perms, list):
        return []
    for p in perms:
        if not isinstance(p, dict):
            continue
        if str(p.get("subjectId") or "") != company_id:
            continue
        st = str(p.get("subjectType") or "")
        if st and st.upper() != "COMPANY":
            continue
        actions = p.get("actions")
        if isinstance(actions, list):
            return [str(a) for a in actions if a]
    return []


def ensure_company_edit_permissions(
    *,
    project_id: str,
    folder_id: str,
    company_id: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
    dry_run: bool,
) -> bool:
    if dry_run:
        logger.info(
            "DRY-RUN: would ensure company edit permissions",
            extra={"extras": {"project_id": project_id, "folder_id": folder_id, "company_id": company_id}},
        )
        return True

    current = get_company_permission_actions(
        project_id=project_id,
        folder_id=folder_id,
        company_id=company_id,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        logger=logger,
    )
    if current is None:
        return False
    have = {c.upper() for c in current}
    want = {a.upper() for a in EDIT_ACTIONS}
    if want.issubset(have):
        return True

    url = DOCS_FOLDER_PERMS_BATCH_UPDATE_TMPL.format(project_id=project_id, folder_id=folder_id)
    body = {
        "permissions": [
            {
                "subjectId": company_id,
                "subjectType": "COMPANY",
                "actions": EDIT_ACTIONS,
            }
        ]
    }
    payload = _request_json(
        "POST",
        url,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json_body=body,
        logger=logger,
        extras={"project_id": project_id, "folder_id": folder_id, "company_id": company_id},
    )
    return payload is not None


def ensure_firma_folder_and_permissions(
    *,
    dm_hub_id: str,
    project_id: str,
    firma_name: str,
    role_kind: str,
    company_id: str,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    logger: logging.Logger,
    dry_run: bool,
) -> bool:
    """
    Ensure folder structure and company permissions for one (project, firma, role_kind).

    role_kind: "lieferant" or "fachplaner"
    """
    role_kind_norm = role_kind.strip().casefold()
    if role_kind_norm not in ("lieferant", "fachplaner"):
        return True
    role_folder = "VB_Lieferant" if role_kind_norm == "lieferant" else "VA_Planer"

    root = get_project_root_folder_id(
        dm_hub_id=dm_hub_id,
        project_id=project_id,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        logger=logger,
        dry_run=dry_run,
    )
    if not root:
        return False

    firma_folder_id = ensure_folder_path(
        project_id=project_id,
        root_folder_id=root,
        segments=["V_extern_Vertraulich", role_folder, firma_name],
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        logger=logger,
        dry_run=dry_run,
    )
    if not firma_folder_id:
        return False

    ok = ensure_company_edit_permissions(
        project_id=project_id,
        folder_id=firma_folder_id,
        company_id=company_id,
        access_token=access_token,
        refresh_access_token=refresh_access_token,
        logger=logger,
        dry_run=dry_run,
    )
    return ok

