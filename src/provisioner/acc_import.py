"""POST batches to ACC Construction Admin ``users:import`` with retries and token refresh."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

import requests

from .config import construction_admin_request_headers

USERS_IMPORT_TMPL = (
    "https://developer.api.autodesk.com/construction/admin/v1/projects/{project_id}/users:import"
)


def user_dict_for_api(u: dict[str, Any]) -> dict[str, Any]:
    """
    Strip internal keys; keep Construction Admin v1 users:import fields only.
    """

    body: dict[str, Any] = {
        "email": u["email"],
        "products": u.get("products") if isinstance(u.get("products"), list) else [],
        "roleIds": u["roleIds"],
        "companyId": u["companyId"],
    }
    return body


def _chunks(items: list[dict], size: int) -> Iterator[list[dict]]:
    """Splits a list into batches of size (minimum 1)"""
    for i in range(0, len(items), max(1, size)):
        yield items[i : i + size]


def _should_retry_status(code: int) -> bool:
    """True for rate limit (429) or server errors (5xx)."""
    return code == 429 or code >= 500


def post_users_import_batch(
    project_id: str,
    users: list[dict],
    *,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    max_retries: int,
    base_backoff_seconds: float,
    logger: logging.Logger, #all of those parameters get their values at runtime, and in the normal workflow they are provided by the CLI 
) -> bool:
    """
    POST one batch to users:import. Retries transient errors with backoff.
    On 401, calls refresh_access_token once (auth refresh) then retries without
    consuming a transient-retry attempt.
    Does not retry other 4xx (except 429).


    Scope: one project + one batch of users

    
    """
    url = USERS_IMPORT_TMPL.format(project_id=project_id) 
    token = access_token
    auth_retried = False
    transient_attempt = 0

    while transient_attempt < max_retries: #value of max_retries comes from cli
        headers = construction_admin_request_headers(access_token=token) #access_token is the value of the access token that is passed down as an argument, imported from the cconfig
        headers["Content-Type"] = "application/json"
        api_users = [user_dict_for_api(x) for x in users] #users is a Python list  acc_import.py does not create the users list by itself. The value is computed during the CLI run and then passed down as an argument.
        try:
            resp = requests.post(
                url,
                headers=headers,
                json={"users": api_users},
                timeout=120,
            )
        except requests.RequestException as e:
            logger.warning(
                "Import request error; will retry if attempts remain",
                extra={
                    "extras": {
                        "project_id": project_id,
                        "transient_attempt": transient_attempt + 1,
                        "error": str(e),
                    }
                },
            )
            transient_attempt += 1
            if transient_attempt >= max_retries:
                return False
            time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
            continue

        code = resp.status_code
        if 200 <= code < 300:
            return True

        if code == 401 and refresh_access_token and not auth_retried: # 
            try:
                token = refresh_access_token()
                auth_retried = True
                logger.info("Refreshed access token after 401", extra={"extras": {"project_id": project_id}})
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "Token refresh failed after 401",
                    extra={"extras": {"project_id": project_id, "error": str(e)}},
                )
                return False
            continue

        if _should_retry_status(code):
            logger.warning(
                "Import transient error; backing off",
                extra={
                    "extras": {
                        "project_id": project_id,
                        "status": code,
                        "transient_attempt": transient_attempt + 1,
                        "body_preview": (resp.text or "")[:500],
                    }
                },
            )
            transient_attempt += 1
            if transient_attempt >= max_retries:
                return False
            time.sleep(base_backoff_seconds * (2 ** (transient_attempt - 1)))
            continue

        logger.error(
            "Import failed (no retry)",
            extra={
                "extras": {
                    "project_id": project_id,
                    "status": code,
                    "body_preview": (resp.text or "")[:1000],
                }
            },
        )
        return False

    return False


def run_import_for_payloads(
    payloads: dict[str, list[dict[str, Any]]],
    *,
    access_token: str,
    refresh_access_token: Callable[[], str] | None,
    batch_size: int,
    max_retries_per_batch: int,
    base_backoff_seconds: float,
    logger: logging.Logger,
    on_batch_success: Callable[[str, list[dict[str, Any]]], None] | None = None,
) -> tuple[int, int, int]:
    """
    For each project_id, POST users in batches.

    Returns (batches_succeeded, batches_failed, total_users_sent_in_ok_batches).

    Scope: all projects + all users


    """
    batches_ok = 0
    batches_fail = 0
    users_sent = 0

    for project_id, users in payloads.items():
        if not users:
            continue
        for batch in _chunks(users, batch_size):
            ok = post_users_import_batch(
                project_id,
                batch,
                access_token=access_token,
                refresh_access_token=refresh_access_token,
                max_retries=max_retries_per_batch,
                base_backoff_seconds=base_backoff_seconds,
                logger=logger,
            )
            if ok:
                batches_ok += 1
                users_sent += len(batch)
                if on_batch_success:
                    try:
                        on_batch_success(project_id, batch)
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            "Post-import hook failed; continuing",
                            extra={"extras": {"project_id": project_id, "error": str(e)}},
                        )
            else:
                batches_fail += 1
                logger.error(
                    "Batch import failed after retries",
                    extra={"extras": {"project_id": project_id, "batch_size": len(batch)}},
                )

    return batches_ok, batches_fail, users_sent
