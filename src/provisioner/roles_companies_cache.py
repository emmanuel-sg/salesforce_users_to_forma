"""Load or fetch ACC hub-level roles and companies into SQLite for CSV mapping."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .config import construction_admin_request_headers
from .db import connect, init_db, upsert_hub_company, upsert_hub_role
from .normalize import normalize_display, normalize_key

# Manual hub roles: edit this file when not loading roles from the API.
DEFAULT_HUB_ROLES_JSON = Path("data/hub_roles.json")


@dataclass(frozen=True)
class Role:
    """Hub/account role id and human-readable name from APS or JSON."""

    role_id: str
    role_name: str


@dataclass(frozen=True)
class Company:
    """Hub/account company id and name from APS or JSON."""

    company_id: str
    company_name: str


def _extract_items(data: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return dict elements from a typical APS list response.

    Some endpoints return JSON:API-style ``{"data":[...]}``, while others return
    ``{"results":[...]}`` with pagination metadata.
    """
    if "data" in data and isinstance(data["data"], list):
        return [x for x in data["data"] if isinstance(x, dict)]
    if "results" in data and isinstance(data["results"], list):
        return [x for x in data["results"] if isinstance(x, dict)]
    return []


def _item_id_name(item: dict[str, Any]) -> tuple[str | None, str | None]:
    """Best-effort (id, display name) extraction for heterogeneous role/company JSON items."""
    rid = item.get("id") or item.get("roleId") or item.get("companyId")
    name = item.get("name") or item.get("roleName") or item.get("companyName")
    attrs = item.get("attributes")
    if isinstance(attrs, dict):
        name = name or attrs.get("name")
    return (str(rid) if rid else None, str(name) if name else None)


def fetch_roles_from_aps(*, hub_id: str, access_token: str) -> list[Role]:
    """
    Hub/account-level roles (Construction Admin).

    `hub_id` here should be the ACC **account id** you use with the Admin API
    (often the same value you store as HUB_*_ID in `.env`).
    """
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{hub_id}/roles"
    resp = requests.get(
        url, headers=construction_admin_request_headers(access_token=access_token), timeout=60
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()

    roles: list[Role] = []
    for item in _extract_items(data) if isinstance(data, dict) else []:
        rid, name = _item_id_name(item)
        if rid and name:
            roles.append(Role(role_id=rid, role_name=name))
    return roles


def fetch_companies_from_aps(*, hub_id: str, access_token: str) -> list[Company]:
    """
    Hub/account-level companies (Construction Admin).
    """
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{hub_id}/companies"
    resp = requests.get(
        url, headers=construction_admin_request_headers(access_token=access_token), timeout=60
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()

    companies: list[Company] = []
    for item in _extract_items(data) if isinstance(data, dict) else []:
        cid, name = _item_id_name(item)
        if cid and name:
            companies.append(Company(company_id=cid, company_name=name))
    return companies


def create_company_in_aps(*, hub_id: str, access_token: str, company_name: str) -> Company:
    """
    Create a hub/account-level company (Construction Admin).

    Note: APS payload requirements may vary by tenant. We send a minimal payload
    (name + a generic trade) and return the created company id + name.
    """
    url = f"https://developer.api.autodesk.com/construction/admin/v1/accounts/{hub_id}/companies"
    payload = {"name": company_name, "trade": "Other"}
    resp = requests.post(
        url,
        headers=construction_admin_request_headers(access_token=access_token)
        | {"Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data: Any = resp.json()

    if isinstance(data, dict):
        # Try common shapes: {"data": {...}} or {"results": [...]} or direct item
        item = None
        if isinstance(data.get("data"), dict):
            item = data["data"]
        elif isinstance(data.get("results"), list) and data["results"]:
            item = data["results"][0] if isinstance(data["results"][0], dict) else None
        else:
            item = data
        if isinstance(item, dict):
            cid, name = _item_id_name(item)
            if cid and name:
                return Company(company_id=cid, company_name=name)

    raise ValueError("Unexpected create company response shape")

def load_roles_from_json(path: Path) -> list[Role]:
    """
    Accepts:
    - {"roles":[{"id","name"}, ...]}
    - [{"role_id","role_name"}, ...]
    - [{"id","name"}, ...]
    """
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    items: list[Any]
    if isinstance(raw, dict) and "roles" in raw:
        items = raw["roles"] if isinstance(raw["roles"], list) else []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    roles: list[Role] = []
    for x in items:
        if not isinstance(x, dict):
            continue
        rid = x.get("role_id") or x.get("id") or x.get("roleId")
        name = x.get("role_name") or x.get("name") or x.get("roleName")
        if rid and name:
            roles.append(Role(role_id=str(rid), role_name=str(name)))
    return roles


def load_companies_from_json(path: Path) -> list[Company]:
    """
    Accepts:
    - {"companies":[{"id","name"}, ...]}
    - [{"company_id","company_name"}, ...]
    """
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    items: list[Any]
    if isinstance(raw, dict) and "companies" in raw:
        items = raw["companies"] if isinstance(raw["companies"], list) else []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    companies: list[Company] = []
    for x in items:
        if not isinstance(x, dict):
            continue
        cid = x.get("company_id") or x.get("id") or x.get("companyId")
        name = x.get("company_name") or x.get("name") or x.get("companyName")
        if cid and name:
            companies.append(Company(company_id=str(cid), company_name=str(name)))
    return companies


def cache_hub_roles(*, db_path: Path, hub_id: str, roles: list[Role]) -> int:
    """Upsert roles into ``hub_roles``; return number processed."""
    init_db(db_path)
    with connect(db_path) as conn:
        for r in roles:
            name = normalize_display(r.role_name)
            upsert_hub_role(
                conn,
                hub_id=hub_id,
                role_id=r.role_id,
                role_name=name,
                role_name_norm=normalize_key(name),
            )
    return len(roles)


def cache_hub_companies(*, db_path: Path, hub_id: str, companies: list[Company]) -> int:
    """Upsert companies into ``hub_companies``; return number processed."""
    init_db(db_path)
    with connect(db_path) as conn:
        for c in companies:
            name = normalize_display(c.company_name)
            upsert_hub_company(
                conn,
                hub_id=hub_id,
                company_id=c.company_id,
                company_name=name,
                company_name_norm=normalize_key(name),
            )
    return len(companies)
