"""Command-line entry for Provisioner: hub/auth, SQLite caches, CSV validation, sync, and ACC import."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .normalize import normalize_key


def _default_db_path_for_hub(hub_key: str) -> Path:
    """Default per-hub SQLite path (keeps dev/prod caches separate)."""
    return Path("data") / f"cache_{hub_key}.db"


def build_parser() -> argparse.ArgumentParser:
    """Build the root ``argparse`` parser and the supported subcommands."""
    parser = argparse.ArgumentParser(
        prog="provisioner",
        description="ACC/FORMA user management CLI (APS).",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional path to a .env file (defaults to .env if present).",
    )

    subparsers = parser.add_subparsers(dest="command")

    cache_projects = subparsers.add_parser(
        "cache-projects",
        help="Cache projects into SQLite for a hub (projects only).",
    )
    cache_projects.add_argument(
        "--hub-key",
        required=True,
        help="Hub key from HUBS= (selects which hub/account to cache).",
    )
    cache_projects.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB file (default: data/cache_<hub_key>.db).",
    )
    cache_projects.add_argument(
        "--access-token",
        default=None,
        help="APS access token (if omitted, uses OAuth tokens for the hub).",
    )
    cache_projects.add_argument(
        "--no-browser",
        action="store_true",
        help="With 3-legged auth: never open a browser. Ignored for client_credentials.",
    )

    sync_hub = subparsers.add_parser(
        "sync-hub",
        help="Fetch all projects and all project users from APS; write SQLite cache and print summary.",
    )
    sync_hub.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB file (default: data/cache_<hub_key>.db).",
    )
    sync_hub.add_argument(
        "--hub-key",
        required=True,
        help="Hub key from HUBS= (selects which hub/account to sync).",
    )
    sync_hub.add_argument(
        "--access-token",
        default=None,
        help="APS access token; if omitted, uses OAuth tokens for the hub.",
    )
    sync_hub.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max transient-retry attempts per project user list (429/5xx/network).",
    )
    sync_hub.add_argument(
        "--no-browser",
        action="store_true",
        help="With 3-legged auth: never open a browser. Ignored for client_credentials.",
    )

    import_csv = subparsers.add_parser(
        "import-csv",
        help="POST users:import for each project from a CSV (Phase 6).",
    )
    import_csv.add_argument(
        "--hub-key",
        required=True,
        help="Hub key from HUBS= (selects which hub/account to import into).",
    )
    import_csv.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB file (default: data/cache_<hub_key>.db).",
    )
    import_csv.add_argument("--csv", required=True, help="CSV file path")
    import_csv.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Max users per POST (same project).",
    )
    import_csv.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max transient-retry attempts per batch (429/5xx/network).",
    )
    import_csv.add_argument(
        "--access-token",
        default=None,
        help="Override token; no OAuth refresh on 401 if set.",
    )
    import_csv.add_argument(
        "--skip-diff",
        action="store_true",
        help="Skip Phase 7: do not list project users or ADD/UPDATE/SKIP filtering.",
    )
    import_csv.add_argument(
        "--dry-run",
        action="store_true",
        help="Phase 8: no APS calls. Diff uses SQLite project_user_cache only (run sync-hub to refresh).",
    )
    import_csv.add_argument(
        "--report",
        default=None,
        help="Phase 8: write per-row plan report (CSV or JSON by file extension).",
    )
    import_csv.add_argument(
        "--no-browser",
        action="store_true",
        help="With 3-legged auth: never open a browser (exit with error if login required). Ignored for client_credentials.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse ``argv`` (or ``sys.argv``), run the selected subcommand, and return a shell exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        from . import __version__

        print(__version__)
        return 0

    from .config import load_env, load_hubs_from_env, resolve_hub_by_key

    load_env(Path(args.env_file) if args.env_file else None)

    if args.command == "cache-projects":
        from .db import init_db
        from .oauth_aps import access_token_after_401, ensure_access_token, oauth_use_client_credentials
        from .projects_cache import cache_projects, fetch_projects_from_aps, resolve_dm_hub_id

        hubs_list = load_hubs_from_env()
        hub = resolve_hub_by_key(hubs_list, args.hub_key)
        if not hub:
            known = ", ".join(h.key for h in hubs_list) if hubs_list else "(none)"
            print(f"ERROR: Unknown hub key {args.hub_key!r}. Configured hub keys: {known}")
            return 2

        if args.access_token:
            token = args.access_token.strip()
        else:

            def _no_browser_fail():
                raise RuntimeError(
                    "Login required. Use APS_AUTH_MODE=client_credentials in .env, or pass --access-token, "
                    "or omit --no-browser."
                )

            try:
                if args.no_browser and not oauth_use_client_credentials():
                    bundle = ensure_access_token(args.hub_key, hub, on_relogin=_no_browser_fail)
                else:
                    bundle = ensure_access_token(args.hub_key, hub)
            except Exception as e:  # noqa: BLE001
                print(f"ERROR: {e}")
                return 2

            token = bundle.access_token

        try:
            dm_hub_id = resolve_dm_hub_id(hub=hub, access_token=token)
        except ValueError as e:
            print(f"ERROR: {e}")
            return 2

        db_path = Path(args.db) if args.db else _default_db_path_for_hub(args.hub_key)
        init_db(db_path)
        projects = fetch_projects_from_aps(hub_id=dm_hub_id, access_token=token)
        count = cache_projects(db_path=db_path, hub_id=hub.hub_id, projects=projects)
        print(f"OK: cached {count} project(s) into {db_path}")
        return 0

    if args.command == "sync-hub":
        from .db import (
            connect,
            init_db,
            project_user_cache_rows,
            projects_for_hub,
            purge_stale_projects_for_hub,
            replace_project_user_cache,
        )
        from .logging_utils import setup_logging
        from .oauth_aps import access_token_after_401, ensure_access_token, oauth_use_client_credentials
        from .project_users import fetch_project_users_for_cache
        from .projects_cache import cache_projects, fetch_projects_from_aps, resolve_dm_hub_id

        hubs_list = load_hubs_from_env()
        hub_key = args.hub_key
        hub = resolve_hub_by_key(hubs_list, hub_key)
        if not hub:
            if not hubs_list:
                print(
                    "ERROR: No hubs loaded from the environment. Create a .env in the project root with "
                    "HUBS=... and HUB_<key>_ID=..., or pass the file explicitly before the subcommand, e.g.:\n"
                    "  python -m provisioner --env-file path\\.env sync-hub"
                )
            else:
                known = ", ".join(h.key for h in hubs_list)
                print(f"ERROR: Unknown hub key {hub_key!r}. Configured hub keys: {known}")
            return 2

        if args.access_token:
            token = args.access_token.strip()
            refresher = None
        else:

            def _sync_no_browser_fail():
                """Raise when ``sync-hub`` cannot refresh tokens and browser login is disallowed."""
                raise RuntimeError(
                    "Login required. Use: APS_AUTH_MODE=client_credentials in .env, or "
                    "`python -m provisioner auth login`, or pass --access-token, "
                    "or omit --no-browser."
                )

            try:
                if args.no_browser and not oauth_use_client_credentials():
                    bundle = ensure_access_token(hub_key, hub, on_relogin=_sync_no_browser_fail)
                else:
                    bundle = ensure_access_token(hub_key, hub)
            except Exception as e:  # noqa: BLE001
                print(f"ERROR: {e}")
                return 2

            def refresher() -> str:
                """Return a new access token after 401 while listing project users during ``sync-hub``."""
                return access_token_after_401(hub_key, hub)

            token = bundle.access_token

        logger, _log_path = setup_logging(logs_dir=Path("logs"))

        try:
            dm_hub_id = resolve_dm_hub_id(hub=hub, access_token=token)
        except ValueError as e:
            print(f"ERROR: {e}")
            return 2

        db_path = Path(args.db) if args.db else _default_db_path_for_hub(hub_key)
        init_db(db_path)

        try:
            projects = fetch_projects_from_aps(hub_id=dm_hub_id, access_token=token)
        except Exception as e:  # noqa: BLE001
            print(f"ERROR: could not list projects: {e}")
            return 2

        n_proj = cache_projects(db_path=db_path, hub_id=hub.hub_id, projects=projects)
        print(f"Cached {n_proj} project(s) for hub_id={hub.hub_id} (DM hub={dm_hub_id})")

        user_failures = 0
        with connect(db_path) as conn:
            deleted = purge_stale_projects_for_hub(
                conn, hub_id=hub.hub_id, keep_project_ids={p.project_id for p in projects}
            )
            if deleted:
                logger.info(
                    "Purged stale projects from cache",
                    extra={"extras": {"hub_id": hub.hub_id, "deleted_projects": deleted}},
                )
            for p in projects:
                rows = fetch_project_users_for_cache(
                    p.project_id,
                    access_token=token,
                    refresh_access_token=refresher,
                    max_retries=args.max_retries,
                    base_backoff_seconds=1.0,
                    logger=logger,
                )
                if rows is None:
                    user_failures += 1
                    logger.error(
                        "Could not cache users for project",
                        extra={"extras": {"project_id": p.project_id, "project_name": p.project_name}},
                    )
                    continue
                replace_project_user_cache(conn, p.project_id, rows)

            prows = projects_for_hub(conn, hub_id=hub.hub_id)
            print("")
            for pr in prows:
                pid = str(pr["project_id"])
                pname = str(pr["project_name"])
                urows = project_user_cache_rows(conn, project_id=pid)
                print(f"Project: {pname}")
                print(f"  project_id={pid}")
                print(f"  users={len(urows)}")
                for ur in urows:
                    print(
                        f"    email={ur['email_norm']} company_id={ur['company_id']} "
                        f"roles_json={ur['role_ids_json']} company_admin={ur['company_admin']}"
                    )
                print("")

        if user_failures:
            print(f"WARNING: failed to fetch users for {user_failures} project(s); see logs.")
            return 1
        return 0

    if args.command == "import-csv":
        from .acc_import import run_import_for_payloads
        from .db import connect, hub_companies_count, hub_roles_count, init_db, replace_project_user_cache
        from .import_plan import (
            ImportDiffSummary,
            ImportRowPlan,
            apply_import_diff,
            dedupe_users_per_project,
            plans_for_skip_diff,
        )
        from .import_report import write_import_report
        from .logging_utils import setup_logging
        from .payload_build import collect_import_payloads_from_csv
        from .project_users import fetch_project_users_for_cache
        from .roles_companies_cache import (
            cache_hub_companies,
            cache_hub_roles,
            fetch_companies_from_aps,
            fetch_roles_from_aps,
        )
        from .folder_provisioner import ensure_firma_folder_and_permissions
        from .projects_cache import resolve_dm_hub_id

        hubs_list = load_hubs_from_env()
        hub = resolve_hub_by_key(hubs_list, args.hub_key)
        if not hub:
            known = ", ".join(h.key for h in hubs_list) if hubs_list else "(none)"
            print(f"ERROR: Unknown hub key {args.hub_key!r}. Configured hub keys: {known}")
            return 2
        effective_hub_id = hub.hub_id

        logger, _log_path = setup_logging(logs_dir=Path("logs"))
        db_path = Path(args.db) if args.db else _default_db_path_for_hub(args.hub_key)

        token: str | None = None
        refresher = None
        if args.access_token:
            token = args.access_token.strip()
            refresher = None
        else:
            from .oauth_aps import access_token_after_401, ensure_access_token, oauth_use_client_credentials

            def _import_no_browser_fail():
                """Raise when ``import-csv`` needs OAuth but ``--no-browser`` blocks interactive login."""
                raise RuntimeError(
                    "Login required. Add APS_AUTH_MODE=client_credentials to .env (and CLIENT_SECRET), "
                    "or pass --access-token, or omit --no-browser."
                )

            if args.no_browser and not oauth_use_client_credentials():
                try:
                    bundle = ensure_access_token(args.hub_key, hub, on_relogin=_import_no_browser_fail)
                except Exception as e:  # noqa: BLE001
                    print(f"ERROR: {e}")
                    return 2
            else:
                bundle = ensure_access_token(args.hub_key, hub)
            token = bundle.access_token

            def refresher() -> str:
                """Return a new access token after 401 during Construction Admin import calls."""
                return access_token_after_401(args.hub_key, hub)

        init_db(db_path)

        # Ensure hub-level mapping tables are present so CSV name -> ID lookups work.
        # For non-dry-run imports we can fetch from APS automatically using the resolved token.
        with connect(db_path) as conn:
            need_roles = hub_roles_count(conn, hub_id=effective_hub_id) == 0
            need_companies = hub_companies_count(conn, hub_id=effective_hub_id) == 0

        if (need_roles or need_companies) and token is not None:
            try:
                if need_roles:
                    roles = fetch_roles_from_aps(hub_id=effective_hub_id, access_token=token)
                    n = cache_hub_roles(db_path=db_path, hub_id=effective_hub_id, roles=roles)
                    logger.info("Cached hub roles", extra={"extras": {"hub_id": effective_hub_id, "roles": n}})
                if need_companies:
                    companies = fetch_companies_from_aps(hub_id=effective_hub_id, access_token=token)
                    n = cache_hub_companies(
                        db_path=db_path, hub_id=effective_hub_id, companies=companies
                    )
                    logger.info("Cached hub companies", extra={"extras": {"hub_id": effective_hub_id, "companies": n}})
            except Exception as e:  # noqa: BLE001
                print(f"ERROR: could not fetch/cache hub roles/companies: {e}")
                return 2
        elif need_roles or need_companies:
            print(
                "ERROR: missing cached hub roles/companies and no access token is available. "
                "Pass --access-token, or configure APS credentials so the tool can obtain a token."
            )
            return 2

        payloads, skipped, validation_skips = collect_import_payloads_from_csv(
            db_path=db_path,
            hub_id=effective_hub_id,
            csv_path=Path(args.csv),
            logger=logger,
            access_token=token,
            create_missing_companies=(token is not None and not args.dry_run),
        )
        fetch_dropped_users = 0
        diff_sum = ImportDiffSummary(0, 0, 0)
        row_plans: list[ImportRowPlan] = []

        if args.skip_diff:
            payloads = dedupe_users_per_project(payloads, logger=logger)
            row_plans = plans_for_skip_diff(payloads)
        else:
            with connect(db_path) as conn:
                payloads = dedupe_users_per_project(payloads, logger=logger)
                if args.dry_run:
                    logger.info(
                        "Dry-run: skipping list-users API; comparing CSV to SQLite project_user_cache only",
                    )
                elif token is not None:
                    for pid in list(payloads.keys()):
                        rows = fetch_project_users_for_cache(
                            pid,
                            access_token=token,
                            refresh_access_token=refresher,
                            max_retries=args.max_retries,
                            base_backoff_seconds=1.0,
                            logger=logger,
                        )
                        if rows is None:
                            fetch_dropped_users += len(payloads[pid])
                            logger.error(
                                "Could not list project users; skipping import for this project",
                                extra={"extras": {"project_id": pid, "users_dropped": len(payloads[pid])}},
                            )
                            del payloads[pid]
                            continue
                        replace_project_user_cache(conn, pid, rows)
                payloads, diff_sum, row_plans = apply_import_diff(conn, payloads, logger=logger)

        def _emit_report(post_import: dict | None) -> None:
            """Write the optional JSON import report including diff, validation skips, and batch stats."""
            if not args.report:
                return
            write_import_report(
                Path(args.report),
                hub_id=effective_hub_id,
                csv_path=str(Path(args.csv)),
                diff_summary=diff_sum if not args.skip_diff else None,
                validation_skips=validation_skips,
                row_plans=row_plans,
                dry_run=args.dry_run,
                skip_diff=args.skip_diff,
                fetch_dropped_users=fetch_dropped_users,
                post_import=post_import,
            )

        if not payloads:
            _emit_report(None)
            prefix = "DRY-RUN " if args.dry_run else ""
            print(
                f"{prefix}Summary: add={diff_sum.add} update={diff_sum.update} skip_same={diff_sum.skip_same} "
                f"fetch_dropped_users={fetch_dropped_users} batches_ok=0 batches_fail=0 users_sent=0 "
                f"skipped_rows={skipped}"
            )
            if args.report:
                print(f"Report: {args.report}")
            return 0 if fetch_dropped_users == 0 else 1

        if args.dry_run:
            # Folder provisioning: no API calls in dry-run; just log intended actions for qualifying rows.
            dm_hub_id = resolve_dm_hub_id(hub=hub, access_token=token or "")
            for project_id, users in payloads.items():
                for u in users:
                    meta = u.get("_provisioner_meta")
                    if not isinstance(meta, dict):
                        continue
                    firma = str(meta.get("company_name") or "").strip()
                    role_kinds = meta.get("role_kinds")
                    if not firma or not isinstance(role_kinds, list) or not role_kinds:
                        continue
                    company_id = str(u.get("companyId") or "").strip()
                    if not company_id:
                        continue
                    for rk in [str(x) for x in role_kinds if x]:
                        ensure_firma_folder_and_permissions(
                            dm_hub_id=dm_hub_id,
                            project_id=project_id,
                            firma_name=firma,
                            role_kind=rk,
                            company_id=company_id,
                            access_token=token or "",
                            refresh_access_token=None,
                            logger=logger,
                            dry_run=True,
                        )
            _emit_report(None)
            print(
                f"DRY-RUN Summary: add={diff_sum.add} update={diff_sum.update} skip_same={diff_sum.skip_same} "
                f"fetch_dropped_users=0 batches_ok=0 batches_fail=0 users_sent=0 skipped_rows={skipped}"
            )
            if args.report:
                print(f"Report: {args.report}")
            return 0

        assert token is not None
        dm_hub_id = resolve_dm_hub_id(hub=hub, access_token=token)

        def _post_batch(project_id: str, batch_users: list[dict]) -> None:
            """
            After a successful users:import batch, ensure folder structure + permissions
            for qualifying users (role_kind Lieferant/Fachplaner) with a company assigned.
            """
            for u in batch_users:
                meta = u.get("_provisioner_meta")
                if not isinstance(meta, dict):
                    continue
                firma = str(meta.get("company_name") or "").strip()
                role_kinds = meta.get("role_kinds")
                if not firma or not isinstance(role_kinds, list) or not role_kinds:
                    continue
                company_id = str(u.get("companyId") or "").strip()
                if not company_id:
                    continue
                for rk in [str(x) for x in role_kinds if x]:
                    ok = ensure_firma_folder_and_permissions(
                        dm_hub_id=dm_hub_id,
                        project_id=project_id,
                        firma_name=firma,
                        role_kind=rk,
                        company_id=company_id,
                        access_token=token,
                        refresh_access_token=refresher,
                        logger=logger,
                        dry_run=False,
                    )
                    if not ok:
                        logger.error(
                            "Folder provisioning failed",
                            extra={
                                "extras": {
                                    "project_id": project_id,
                                    "company_id": company_id,
                                    "firma": firma,
                                    "role_kind": rk,
                                }
                            },
                        )

        batches_ok, batches_fail, users_sent = run_import_for_payloads(
            payloads,
            access_token=token,
            refresh_access_token=refresher,
            batch_size=args.batch_size,
            max_retries_per_batch=args.max_retries,
            base_backoff_seconds=1.0,
            logger=logger,
            on_batch_success=_post_batch,
        )
        _emit_report(
            {
                "batches_ok": batches_ok,
                "batches_fail": batches_fail,
                "users_sent": users_sent,
            }
        )
        print(
            f"Summary: add={diff_sum.add} update={diff_sum.update} skip_same={diff_sum.skip_same} "
            f"fetch_dropped_users={fetch_dropped_users} batches_ok={batches_ok} batches_fail={batches_fail} "
            f"users_sent={users_sent} skipped_rows={skipped}"
        )
        if args.report:
            print(f"Report: {args.report}")
        return 0 if batches_fail == 0 and fetch_dropped_users == 0 else 1

    return 0
