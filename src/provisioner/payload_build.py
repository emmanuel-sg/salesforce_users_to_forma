"""Turn validated import CSV rows into ``users:import`` payload dicts using SQLite lookups."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .csv_validate import EXPECTED_HEADER, canonical_access_level
from .db import (
    connect,
    init_db,
    lookup_hub_company_id,
    lookup_hub_role_id,
    lookup_project_id,
)
from .normalize import normalize_display, normalize_key

PRODUCT_KEYS = ("build", "modelCoordination", "docs", "insight")


def _products_for_access(access_level: str) -> list[dict[str, str]]:
    """
    Build the Construction Admin v1 `products` list for a user.

    The CSV uses `Member` / `Administrator`; the API expects per-product `access`
    values like `member` / `administrator`.
    """
    access = "administrator" if access_level == "Administrator" else "member"
    return [{"key": k, "access": access} for k in PRODUCT_KEYS]


@dataclass(frozen=True)
class PayloadBuildResult:
    """Summary from writing per-project JSON files via :func:`build_import_payloads_from_csv`."""

    written_projects: int
    written_users: int
    skipped_rows: int


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV missing header")
        header = tuple([h.strip() for h in reader.fieldnames])
        if header != EXPECTED_HEADER:
            raise ValueError(f"Invalid header. Expected: {','.join(EXPECTED_HEADER)}")
        return [{k: (v or "") for k, v in row.items()} for row in reader]


def collect_import_payloads_from_csv(
    *,
    db_path: Path,
    hub_id: str,
    csv_path: Path,
    logger,
    access_token: str | None = None,
    create_missing_companies: bool = False,
) -> tuple[dict[str, list[dict]], int, list[dict[str, str | int]]]:
    """
    Map CSV rows to project_id -> users[] for POST .../users:import.
    Returns (payloads, skipped_row_count, validation_skip_details for reports).
    Each user dict may include _provisioner_meta for dry-run / export.
    """
    init_db(db_path)

    if not hub_id.strip():
        raise ValueError("hub_id is required for hub-level role/company mapping")

    rows = _read_csv_rows(csv_path)
    skipped = 0
    payloads: dict[str, list[dict]] = {}
    validation_skips: list[dict[str, str | int]] = []
    src_file = str(csv_path)

    with connect(db_path) as conn:
        for i, row in enumerate(rows, start=2):  # CSV row numbers start at 2
            email = normalize_display(row["email"])
            project_name = normalize_display(row["project_name"])
            company_name = normalize_display(row["company"])
            roles_raw = row["roles"] or ""

            access = canonical_access_level(row.get("access_level", ""))
            if access is None:
                skipped += 1
                validation_skips.append(
                    {
                        "csv_row": i,
                        "source_file": src_file,
                        "email": email,
                        "project_name": project_name,
                        "reason": "invalid access_level (expect Member or Administrator)",
                    }
                )
                logger.warning(
                    "Invalid access_level; skipping row",
                    extra={
                        "extras": {
                            "file": str(csv_path),
                            "row": i,
                            "email": normalize_display(row.get("email", "")),
                            "project_name": project_name,
                        }
                    },
                )
                continue
            company_admin = access == "Administrator"
            products = _products_for_access(access)

            pid = lookup_project_id(conn, hub_id=hub_id, project_name_norm=normalize_key(project_name))
            if not pid:
                skipped += 1
                validation_skips.append(
                    {
                        "csv_row": i,
                        "source_file": src_file,
                        "email": email,
                        "project_name": project_name,
                        "reason": "project not found in SQLite cache",
                    }
                )
                logger.warning(
                    "Project not found in cache; skipping row",
                    extra={"extras": {"file": str(csv_path), "row": i, "email": email, "project_name": project_name}},
                )
                continue

            cid = lookup_hub_company_id(
                conn, hub_id=hub_id, company_name_norm=normalize_key(company_name)
            )
            if not cid:
                if create_missing_companies and access_token and company_name:
                    try:
                        from .db import upsert_hub_company
                        from .roles_companies_cache import create_company_in_aps

                        created = create_company_in_aps(
                            hub_id=hub_id,
                            access_token=access_token,
                            company_name=company_name,
                        )
                        cname = normalize_display(created.company_name)
                        upsert_hub_company(
                            conn,
                            hub_id=hub_id,
                            company_id=created.company_id,
                            company_name=cname,
                            company_name_norm=normalize_key(cname),
                        )
                        cid = created.company_id
                        logger.info(
                            "Created missing company in hub",
                            extra={
                                "extras": {
                                    "company_name": company_name,
                                    "company_id": created.company_id,
                                    "hub_id": hub_id,
                                }
                            },
                        )
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            "Failed to create missing company; skipping row",
                            extra={
                                "extras": {
                                    "file": str(csv_path),
                                    "row": i,
                                    "email": email,
                                    "project_name": project_name,
                                    "company": company_name,
                                    "error": str(e),
                                }
                            },
                        )
                        skipped += 1
                        validation_skips.append(
                            {
                                "csv_row": i,
                                "source_file": src_file,
                                "email": email,
                                "project_name": project_name,
                                "reason": f"company not found and could not be created: {company_name}",
                            }
                        )
                        continue

                if cid:
                    # created successfully above
                    pass
                else:
                skipped += 1
                validation_skips.append(
                    {
                        "csv_row": i,
                        "source_file": src_file,
                        "email": email,
                        "project_name": project_name,
                        "reason": (
                            "company not found in SQLite cache"
                            if not access_token
                            else "company not found in SQLite cache (would be created on real import)"
                        ),
                    }
                )
                logger.warning(
                    "Company not found in cache; skipping row",
                    extra={"extras": {"file": str(csv_path), "row": i, "email": email, "project_name": project_name}},
                )
                continue

            role_names = [normalize_display(x) for x in roles_raw.split(";")]
            role_names = [x for x in role_names if x]
            role_kinds: list[str] = []
            for rn in role_names:
                k = rn.casefold()
                if "lieferant" in k and "lieferant" not in role_kinds:
                    role_kinds.append("lieferant")
                if "fachplaner" in k and "fachplaner" not in role_kinds:
                    role_kinds.append("fachplaner")
            role_ids: list[str] = []
            missing_role = False
            for rn in role_names:
                rid = lookup_hub_role_id(conn, hub_id=hub_id, role_name_norm=normalize_key(rn))
                if not rid:
                    missing_role = True
                    validation_skips.append(
                        {
                            "csv_row": i,
                            "source_file": src_file,
                            "email": email,
                            "project_name": project_name,
                            "reason": f"role not found in cache: {rn}",
                        }
                    )
                    logger.warning(
                        "Role not found in cache; skipping row",
                        extra={
                            "extras": {
                                "file": str(csv_path),
                                "row": i,
                                "email": email,
                                "project_name": project_name,
                                "role": rn,
                            }
                        },
                    )
                    break
                role_ids.append(rid)
            if missing_role:
                skipped += 1
                continue

            payloads.setdefault(pid, []).append(
                {
                    "email": email,
                    "products": products,
                    "roleIds": role_ids,
                    "companyId": cid,
                    "companyAdmin": company_admin,
                    "_provisioner_meta": {
                        "csv_row": i,
                        "source_file": src_file,
                        "project_name": project_name,
                        "company_name": company_name,
                        "role_names": list(role_names),
                        "role_kinds": list(role_kinds),
                    },
                }
            )

    return payloads, skipped, validation_skips


def build_import_payloads_from_csv(
    *,
    db_path: Path,
    hub_id: str,
    csv_path: Path,
    output_dir: Path,
    logger,
) -> PayloadBuildResult:
    """Like :func:`collect_import_payloads_from_csv` but writes ``import-{project_id}.json`` per project."""
    output_dir.mkdir(parents=True, exist_ok=True)
    payloads, skipped, _skips = collect_import_payloads_from_csv(
        db_path=db_path,
        hub_id=hub_id,
        csv_path=csv_path,
        logger=logger,
    )

    written_projects = 0
    written_users = 0
    for pid, users in payloads.items():
        out = output_dir / f"import-{pid}.json"
        out.write_text(json.dumps({"users": users}, indent=2), encoding="utf-8")
        written_projects += 1
        written_users += len(users)

    return PayloadBuildResult(written_projects=written_projects, written_users=written_users, skipped_rows=skipped)

