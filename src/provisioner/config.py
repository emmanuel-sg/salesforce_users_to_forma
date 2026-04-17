"""Environment and hub configuration: ``.env`` loading, :class:`HubConfig`, and Construction Admin headers."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class HubConfig:
    """Resolved settings for one entry in ``HUBS=``."""

    key: str
    name: str | None
    hub_id: str
    """ACC Construction Admin account id (UUID from ACC); used for roles, companies, DB joins."""
    dm_hub_id: str | None
    """Data Management hub id (usually ``b.<uuid>``) for listing projects; optional."""
    client_id: str | None
    client_secret: str | None


def load_env(env_path: Path | None = None) -> None:
    """Load environment variables from ``.env`` (or a given path) via python-dotenv."""
    load_dotenv(dotenv_path=str(env_path) if env_path else None)


def _get_required(name: str) -> str:
    """Return a non-empty stripped env var or raise ``ValueError``."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value.strip()


def _get_optional(name: str) -> str | None:
    """Return stripped env var or ``None`` if missing/blank."""
    value = os.getenv(name)
    return None if value is None or value.strip() == "" else value.strip()


def list_hub_keys() -> list[str]:
    """Parse comma-separated hub keys from ``HUBS``."""
    raw = os.getenv("HUBS", "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys


def load_hubs_from_env() -> list[HubConfig]:
    """Build :class:`HubConfig` objects for each hub key using per-hub env vars."""
    hubs: list[HubConfig] = []
    for key in list_hub_keys():
        hub_id = _get_required(f"HUB_{key}_ID")
        dm_hub_id = _get_optional(f"HUB_{key}_DM_HUB_ID")
        hub_cid = _get_optional(f"HUB_{key}_CLIENT_ID")
        hub_sec = _get_optional(f"HUB_{key}_CLIENT_SECRET")
        hubs.append(
            HubConfig(
                key=key,
                name=_get_optional(f"HUB_{key}_NAME"),
                hub_id=hub_id,
                dm_hub_id=dm_hub_id,
                client_id=hub_cid,
                client_secret=hub_sec,
            )
        )
    return hubs


def active_hub_path() -> Path:
    """Filesystem path to ``data/active_hub.json`` (selected hub key for CLI)."""
    return Path("data") / "active_hub.json"


def set_active_hub(hub_key: str) -> None:
    """Persist the active hub key to ``active_hub_path``."""
    path = active_hub_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"hub_key": hub_key}, indent=2), encoding="utf-8")


def get_active_hub_key() -> str | None:
    """Read the active hub key from disk, or ``None`` if missing/invalid."""
    path = active_hub_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        key = payload.get("hub_key")
        return None if not isinstance(key, str) or not key.strip() else key.strip()
    except Exception:  # noqa: BLE001
        return None


def resolve_hub_by_key(hubs: list[HubConfig], hub_key: str) -> HubConfig | None:
    """Return the hub config matching ``hub_key``, or ``None``."""
    for h in hubs:
        if h.key == hub_key:
            return h
    return None


def construction_admin_request_headers(*, access_token: str) -> dict[str, str]:
    """
    Headers for Construction Admin API calls. When using a token without an embedded user
    (e.g. 2-legged), Autodesk often requires x-user-id (an account admin’s Autodesk user id),
    taken from ``APS_USER_ID_TST`` or ``APS_USER_ID`` when set.
    """
    headers: dict[str, str] = {"Authorization": f"Bearer {access_token}"}
    uid = _get_optional("APS_USER_ID_TST") or _get_optional("APS_USER_ID")
    if uid:
        headers["x-user-id"] = uid
    return headers

