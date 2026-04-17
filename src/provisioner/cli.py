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
    """Build the root ``argparse`` parser and all subcommands (hubs, auth, db, cache, sync, import, validate)."""
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

    hubs = subparsers.add_parser(
        "hubs",
        help="List and select hubs (Phase 3).",
    )
    hubs_sub = hubs.add_subparsers(dest="hubs_command")

    hubs_list = hubs_sub.add_parser("list", help="List hubs from .env")
    hubs_list.add_argument("--show-secrets", action="store_true", help="Show client_secret length (not the secret).")

    hubs_choose = hubs_sub.add_parser("choose", help="Choose active hub (interactive or via --hub-key)")
    hubs_choose.add_argument("--hub-key", default=None, help="Hub key to select (non-interactive).")

    db_init = subparsers.add_parser(
        "db-init",
        help="Initialize the local SQLite cache (Phase 2).",
    )
    db_init.add_argument("--db", default="data/cache.db", help="Path to SQLite DB file.")

    cache_projects = subparsers.add_parser(
        "cache-projects",
        help="Cache projects into SQLite (Phase 2).",
    )
    cache_projects.add_argument("--db", default="data/cache.db", help="Path to SQLite DB file.")
    cache_projects.add_argument("--hub-id", default=None, help="Hub ID (optional; stored with projects).")
    cache_projects.add_argument(
        "--from-json",
        default=None,
        help="Offline mode: load projects from JSON file (list of {project_id, project_name}).",
    )
    cache_projects.add_argument(
        "--access-token",
        default=None,
        help="APS access token (for live fetch).",
    )

    lookup_project = subparsers.add_parser(
        "lookup-project",
        help="Lookup a project_id by project_name (Phase 2).",
    )
    lookup_project.add_argument("--db", default="data/cache.db", help="Path to SQLite DB file.")
    lookup_project.add_argument("--hub-id", default=None, help="Hub ID to scope lookup (optional).")
    lookup_project.add_argument("--project-name", required=True, help="Project name to look up.")

    cache_hub_roles = subparsers.add_parser(
        "cache-hub-roles",
        help="Cache hub/account-level roles into SQLite (Phase 5).",
    )
    cache_hub_roles.add_argument("--db", default="data/cache.db", help="Path to SQLite DB file.")
    cache_hub_roles.add_argument(
        "--hub-id",
        required=True,
        help="ACC account/hub id (same as HUB_*_ID in .env for Admin API).",
    )
    cache_hub_roles.add_argument(
        "--from-json",
        default=None,
        help=(
            "Offline: JSON file (default: data/hub_roles.json if --access-token not set). "
            'Shape: {"roles":[{"id","name"},...]} or list of {role_id,role_name}.'
        ),
    )
    cache_hub_roles.add_argument("--access-token", default=None, help="APS access token (live fetch)")

    cache_hub_companies = subparsers.add_parser(
        "cache-hub-companies",
        help="Cache hub/account-level companies into SQLite (Phase 5).",
    )
    cache_hub_companies.add_argument("--db", default="data/cache.db", help="Path to SQLite DB file.")
    cache_hub_companies.add_argument("--hub-id", required=True, help="ACC account/hub id (HUB_*_ID).")
    cache_hub_companies.add_argument(
        "--from-json",
        default=None,
        help=(
            'Optional offline JSON: {"companies":[{"id","name"},...]}. '
            "Without this flag, companies are loaded from the API (--access-token required)."
        ),
    )
    cache_hub_companies.add_argument("--access-token", default=None, help="APS access token (live fetch)")

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
        default=None,
        help="Hub key from HUBS= (defaults to active hub from `hubs choose`).",
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

    build_payload = subparsers.add_parser(
        "build-payload",
        help="Build users:import JSON payload(s) from a CSV (Phase 5).",
    )
    build_payload.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB file (default: data/cache_<active_hub>.db).",
    )
    build_payload.add_argument(
        "--hub-id",
        default=None,
        help="Hub/account id for project + role + company lookup (or use active hub from `hubs choose`).",
    )
    build_payload.add_argument("--csv", required=True, help="CSV file path")
    build_payload.add_argument("--out-dir", default="data/payloads", help="Output directory for JSON payloads")

    import_csv = subparsers.add_parser(
        "import-csv",
        help="POST users:import for each project from a CSV (Phase 6).",
    )
    import_csv.add_argument(
        "--db",
        default=None,
        help="Path to SQLite DB file (default: data/cache_<active_hub>.db).",
    )
    import_csv.add_argument(
        "--hub-id",
        default=None,
        help="Hub/account id for lookups (or active hub from `hubs choose`).",
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

    validate = subparsers.add_parser(
        "validate-csv",
        help="Validate CSVs in users_to_import/ (Phase 1).",
    )
    validate.add_argument(
        "--input-dir",
        default="users_to_import",
        help="Directory containing input CSV files.",
    )
    validate.add_argument(
        "--logs-dir",
        default="logs",
        help="Directory to write log files.",
    )

    auth = subparsers.add_parser(
        "auth",
        help="APS 3-legged OAuth (Phase 4).",
    )
    auth_sub = auth.add_subparsers(dest="auth_command")

    auth_login = auth_sub.add_parser("login", help="Open browser; save tokens under data/tokens/")
    auth_login.add_argument("--hub-key", default=None, help="Hub key (defaults to active hub).")

    auth_status = auth_sub.add_parser("status", help="Show token file status for active hub")
    auth_status.add_argument("--hub-key", default=None, help="Hub key (defaults to active hub).")

    auth_token = auth_sub.add_parser(
        "token",
        help="Print a valid access token (refresh or browser login if needed).",
    )
    auth_token.add_argument("--hub-key", default=None, help="Hub key (defaults to active hub).")
    auth_token.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open browser; exit with error if login required.",
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

    # Phase 3: env + hubs
    from .config import (
        get_active_hub_key,
        load_env,
        load_hubs_from_env,
        resolve_hub_by_key,
        set_active_hub,
    )

    load_env(Path(args.env_file) if args.env_file else None)

    if args.command == "hubs":
        hubs_list = load_hubs_from_env()

        if args.hubs_command == "list":
            if not hubs_list:
                print("No hubs configured. See .env.example")
                return 0
            active = get_active_hub_key()
            for h in hubs_list:
                marker = "*" if active == h.key else " "
                secret_info = ""
                if args.show_secrets:
                    secret_info = f" client_secret_len={0 if not h.client_secret else len(h.client_secret)}"
                print(f"{marker} {h.key} name={h.name or ''} hub_id={h.hub_id}{secret_info}".rstrip())
            return 0

        if args.hubs_command == "choose":
            if not hubs_list:
                print("ERROR: No hubs configured. See .env.example")
                return 2

            if args.hub_key:
                chosen = resolve_hub_by_key(hubs_list, args.hub_key)
                if not chosen:
                    print("ERROR: Unknown hub key")
                    return 2
                set_active_hub(chosen.key)
                print(f"OK: active hub set to {chosen.key}")
                return 0

            # interactive
            for i, h in enumerate(hubs_list, start=1):
                print(f"[{i}] {h.key} ({h.name or h.hub_id})")
            try:
                raw = input("Select hub number: ").strip()
                idx = int(raw)
            except Exception:  # noqa: BLE001
                print("ERROR: invalid selection")
                return 2
            if idx < 1 or idx > len(hubs_list):
                print("ERROR: selection out of range")
                return 2
            chosen = hubs_list[idx - 1]
            set_active_hub(chosen.key)
            print(f"OK: active hub set to {chosen.key}")
            return 0

        print("ERROR: missing hubs subcommand (list|choose)")
        return 2

    if args.command == "auth":
        from .oauth_aps import (
            ensure_access_token,
            fetch_client_credentials_token,
            interactive_browser_login,
            load_tokens,
            oauth_use_client_credentials,
            save_tokens,
            token_path_for_hub,
        )

        hubs_list = load_hubs_from_env()
        hub_key = args.hub_key or get_active_hub_key()
        if not hub_key:
            print("ERROR: No hub selected. Run: python -m provisioner hubs choose")
            return 2
        hub = resolve_hub_by_key(hubs_list, hub_key)
        if not hub:
            print("ERROR: Unknown hub key")
            return 2

        if args.auth_command == "login":
            try:
                if oauth_use_client_credentials():
                    bundle = fetch_client_credentials_token(hub=hub)
                else:
                    bundle = interactive_browser_login(hub)
            except Exception as e:  # noqa: BLE001
                print(f"ERROR: {e}")
                return 2
            save_tokens(hub_key, bundle)
            mode = "client_credentials" if oauth_use_client_credentials() else "three_legged"
            print(f"OK: tokens saved for hub {hub_key} ({mode}) -> {token_path_for_hub(hub_key)}")
            return 0

        if args.auth_command == "status":
            path = token_path_for_hub(hub_key)
            b = load_tokens(hub_key)
            mode = "client_credentials" if oauth_use_client_credentials() else "three_legged"
            print(f"hub_key={hub_key}")
            print(f"auth_mode={mode}")
            print(f"token_file={path} exists={path.exists()}")
            if b:
                print(f"expired={b.is_expired()} has_refresh={bool(b.refresh_token)}")
            return 0

        if args.auth_command == "token":
            if args.no_browser and not oauth_use_client_credentials():

                def relogin_fail():
                    """Used with ``auth token --no-browser`` when a new login would be required."""
                    raise RuntimeError("Login required; run: python -m provisioner auth login")

                try:
                    bundle = ensure_access_token(hub_key, hub, on_relogin=relogin_fail)
                except Exception as e:  # noqa: BLE001
                    print(f"ERROR: {e}")
                    return 2
            else:
                bundle = ensure_access_token(hub_key, hub)
            print(bundle.access_token)
            return 0

        print("ERROR: missing auth subcommand (login|status|token)")
        return 2

    if args.command == "db-init":
        from .db import init_db

        db_path = Path(args.db)
        init_db(db_path)
        print(f"OK: initialized db at {db_path}")
        return 0

    if args.command == "cache-projects":
        from .projects_cache import cache_projects, fetch_projects_from_aps, load_projects_from_json

        db_path = Path(args.db)
        hub_id = args.hub_id

        if args.from_json:
            projects = load_projects_from_json(Path(args.from_json))
        else:
            if not args.access_token or not hub_id:
                print("ERROR: live fetch requires --hub-id and --access-token (or use --from-json).")
                return 2
            projects = fetch_projects_from_aps(hub_id=hub_id, access_token=args.access_token)

        count = cache_projects(db_path=db_path, hub_id=hub_id, projects=projects)
        print(f"OK: cached {count} projects into {db_path}")
        return 0

    if args.command == "lookup-project":
        from .db import connect, init_db, lookup_project_id

        db_path = Path(args.db)
        init_db(db_path)
        with connect(db_path) as conn:
            pid = lookup_project_id(
                conn,
                hub_id=args.hub_id,
                project_name_norm=normalize_key(args.project_name),
            )

        if pid is None:
            print("NOT_FOUND")
            return 1
        print(pid)
        return 0

    if args.command == "cache-hub-roles":
        from .roles_companies_cache import (
            DEFAULT_HUB_ROLES_JSON,
            cache_hub_roles,
            fetch_roles_from_aps,
            load_roles_from_json,
        )

        db_path = Path(args.db)
        hub_id = args.hub_id.strip()
        if args.access_token:
            roles = fetch_roles_from_aps(hub_id=hub_id, access_token=args.access_token)
        else:
            json_path = Path(args.from_json) if args.from_json else DEFAULT_HUB_ROLES_JSON
            if not json_path.is_file():
                print(
                    "ERROR: No --access-token and JSON file missing: "
                    f"{json_path} (pass --from-json or create that file)"
                )
                return 2
            roles = load_roles_from_json(json_path)
        n = cache_hub_roles(db_path=db_path, hub_id=hub_id, roles=roles)
        print(f"OK: cached {n} hub roles for hub_id={hub_id}")
        return 0

    if args.command == "cache-hub-companies":
        from .roles_companies_cache import cache_hub_companies, fetch_companies_from_aps, load_companies_from_json

        db_path = Path(args.db)
        hub_id = args.hub_id.strip()
        if args.access_token:
            companies = fetch_companies_from_aps(hub_id=hub_id, access_token=args.access_token)
        elif args.from_json:
            json_path = Path(args.from_json)
            if not json_path.is_file():
                print(f"ERROR: JSON file not found: {json_path}")
                return 2
            companies = load_companies_from_json(json_path)
        else:
            print(
                "ERROR: Companies are fetched from the hub via API. "
                "Pass --access-token (e.g. from `python -m provisioner auth token`), "
                "or pass --from-json only for rare offline/testing."
            )
            return 2
        n = cache_hub_companies(db_path=db_path, hub_id=hub_id, companies=companies)
        print(f"OK: cached {n} hub companies for hub_id={hub_id}")
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
        hub_key = args.hub_key or get_active_hub_key()
        if not hub_key:
            print("ERROR: Set --hub-key or run: python -m provisioner hubs choose")
            return 2
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

    if args.command == "build-payload":
        from .logging_utils import setup_logging
        from .payload_build import build_import_payloads_from_csv

        hubs_list = load_hubs_from_env()
        effective_hub_id = (args.hub_id or "").strip() or None
        if not effective_hub_id:
            ak = get_active_hub_key()
            hcfg = resolve_hub_by_key(hubs_list, ak) if ak else None
            effective_hub_id = hcfg.hub_id if hcfg else None
        if not effective_hub_id:
            print("ERROR: Set --hub-id or run: python -m provisioner hubs choose")
            return 2

        db_path = Path(args.db) if args.db else _default_db_path_for_hub(get_active_hub_key() or "default")
        logger, _log_path = setup_logging(logs_dir=Path("logs"))
        res = build_import_payloads_from_csv(
            db_path=db_path,
            hub_id=effective_hub_id,
            csv_path=Path(args.csv),
            output_dir=Path(args.out_dir),
            logger=logger,
        )
        print(
            f"OK: wrote_projects={res.written_projects} wrote_users={res.written_users} skipped_rows={res.skipped_rows}"
        )
        return 0

    if args.command == "import-csv":
        from .acc_import import run_import_for_payloads
        from .db import connect, init_db, replace_project_user_cache
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

        hubs_list = load_hubs_from_env()
        effective_hub_id = (args.hub_id or "").strip() or None
        if not effective_hub_id:
            ak = get_active_hub_key()
            hcfg = resolve_hub_by_key(hubs_list, ak) if ak else None
            effective_hub_id = hcfg.hub_id if hcfg else None
        if not effective_hub_id:
            print("ERROR: Set --hub-id or run: python -m provisioner hubs choose")
            return 2

        logger, _log_path = setup_logging(logs_dir=Path("logs"))
        active_key = get_active_hub_key()
        db_path = Path(args.db) if args.db else _default_db_path_for_hub(active_key or "default")

        payloads, skipped, validation_skips = collect_import_payloads_from_csv(
            db_path=db_path,
            hub_id=effective_hub_id,
            csv_path=Path(args.csv),
            logger=logger,
        )

        token: str | None = None
        refresher = None
        if not args.dry_run:
            if args.access_token:
                token = args.access_token.strip()
                refresher = None
            else:
                hub_key = get_active_hub_key()
                if not hub_key:
                    print("ERROR: No active hub for OAuth. Run hubs choose or pass --access-token")
                    return 2
                hub = resolve_hub_by_key(hubs_list, hub_key)
                if not hub:
                    print("ERROR: Unknown active hub key")
                    return 2
                from .oauth_aps import access_token_after_401, ensure_access_token, oauth_use_client_credentials

                def _import_no_browser_fail():
                    """Raise when ``import-csv`` needs OAuth but ``--no-browser`` blocks interactive login."""
                    raise RuntimeError(
                        "Login required. Add APS_AUTH_MODE=client_credentials to .env (and CLIENT_SECRET), "
                        "or run `python -m provisioner auth login`, or pass --access-token, "
                        "or omit --no-browser."
                    )

                if args.no_browser and not oauth_use_client_credentials():
                    try:
                        bundle = ensure_access_token(hub_key, hub, on_relogin=_import_no_browser_fail)
                    except Exception as e:  # noqa: BLE001
                        print(f"ERROR: {e}")
                        return 2
                else:
                    bundle = ensure_access_token(hub_key, hub)
                token = bundle.access_token

                def refresher() -> str:
                    """Return a new access token after 401 during Construction Admin import calls."""
                    return access_token_after_401(hub_key, hub)

        init_db(db_path)
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
            _emit_report(None)
            print(
                f"DRY-RUN Summary: add={diff_sum.add} update={diff_sum.update} skip_same={diff_sum.skip_same} "
                f"fetch_dropped_users=0 batches_ok=0 batches_fail=0 users_sent=0 skipped_rows={skipped}"
            )
            if args.report:
                print(f"Report: {args.report}")
            return 0

        assert token is not None
        batches_ok, batches_fail, users_sent = run_import_for_payloads(
            payloads,
            access_token=token,
            refresh_access_token=refresher,
            batch_size=args.batch_size,
            max_retries_per_batch=args.max_retries,
            base_backoff_seconds=1.0,
            logger=logger,
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

    if args.command == "validate-csv":
        from .csv_validate import iter_csv_files, validate_csv_file
        from .logging_utils import log_row_issue, setup_logging

        input_dir = Path(args.input_dir)
        logger, log_path = setup_logging(logs_dir=Path(args.logs_dir))

        files = iter_csv_files(input_dir)
        if not files:
            logger.info(
                "No CSV files found.",
                extra={"extras": {"input_dir": str(input_dir), "log_file": str(log_path)}},
            )
            print("Summary: processed=0 added=0 updated=0 skipped=0 failed=0")
            return 0

        totals = {"processed": 0, "valid": 0, "skipped": 0, "failed": 0}

        def on_valid_row(_path: Path, _row: int, _validated) -> None:
            """No-op row hook: Phase 1 validation only counts rows via ``validate_csv_file`` summaries."""
            return

        def on_row_error(path: Path, row: int, email: str | None, project_name: str | None, reason: str) -> None:
            """Record a single invalid CSV row to the structured log."""
            log_row_issue(
                logger,
                level=logging.WARNING,
                file=str(path),
                row=row,
                email=email,
                project_name=project_name,
                reason=reason,
            )

        def on_file_error(path: Path, reason: str) -> None:
            """Log a fatal error for an entire CSV file (e.g. unreadable or wrong shape)."""
            logger.error(reason, extra={"extras": {"file": str(path), "reason": reason}})

        for p in files:
            logger.info("Validating CSV", extra={"extras": {"file": str(p)}})
            summary = validate_csv_file(
                p,
                on_valid_row=on_valid_row,
                on_row_error=on_row_error,
                on_file_error=on_file_error,
            )
            totals["processed"] += summary.processed
            totals["valid"] += summary.valid
            totals["skipped"] += summary.skipped
            totals["failed"] += summary.failed

        # Match the later import summary shape, even though Phase 1 doesn't add/update yet.
        print(
            "Summary: "
            f"processed={totals['processed']} "
            "added=0 updated=0 "
            f"skipped={totals['skipped']} "
            f"failed={totals['failed']}"
        )
        return 0

    return 0
