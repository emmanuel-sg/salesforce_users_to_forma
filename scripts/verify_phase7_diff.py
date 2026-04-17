"""Verify Phase 7 ADD/UPDATE/SKIP diff against project_user_cache (no network)."""
from __future__ import annotations

import json
import logging
import sqlite3

from provisioner.db import SCHEMA_SQL, replace_project_user_cache
from provisioner.import_plan import apply_import_diff, dedupe_users_per_project
from provisioner.normalize import normalize_key

logger = logging.getLogger("verify_phase7")
logger.addHandler(logging.NullHandler())


def _user(email: str, company: str, roles: list[str], admin: bool) -> dict:
    """Build one Construction Admin user dict as if produced from CSV/import payloads."""
    return {
        "email": email,
        "products": [],
        "roleIds": roles,
        "companyId": company,
        "companyAdmin": admin,
    }


def main() -> int:
    """Run SKIP/ADD/UPDATE scenarios for ``apply_import_diff`` against an in-memory SQLite DB."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)

    en = normalize_key("user@test.com")

    # SKIP: cache matches desired
    replace_project_user_cache(
        conn,
        "proj-a",
        [(en, "c1", json.dumps(["r1", "r2"]), 0)],
    )
    payloads = {"proj-a": [_user("user@test.com", "c1", ["r2", "r1"], False)]}
    payloads = dedupe_users_per_project(payloads, logger=logger)
    out, s, _plans = apply_import_diff(conn, payloads, logger=logger)
    assert s.skip_same == 1 and s.add == 0 and s.update == 0, s
    assert out == {}, out

    # ADD: not in cache
    payloads2 = {"proj-b": [_user("new@test.com", "c1", ["r1"], True)]}
    payloads2 = dedupe_users_per_project(payloads2, logger=logger)
    out2, s2, _p2 = apply_import_diff(conn, payloads2, logger=logger)
    assert s2.add == 1 and s2.skip_same == 0, s2
    assert len(out2["proj-b"]) == 1

    # UPDATE: cache differs
    replace_project_user_cache(
        conn,
        "proj-c",
        [(normalize_key("x@test.com"), "old", json.dumps(["r1"]), 0)],
    )
    payloads3 = {"proj-c": [_user("x@test.com", "new", ["r1"], False)]}
    payloads3 = dedupe_users_per_project(payloads3, logger=logger)
    out3, s3, _p3 = apply_import_diff(conn, payloads3, logger=logger)
    assert s3.update == 1, s3
    assert len(out3["proj-c"]) == 1

    conn.close()
    print("OK: Phase 7 diff logic verified (SQLite :memory:, no API).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
