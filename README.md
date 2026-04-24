## Provisioner (ACC/FORMA user provisioning CLI)

Provisioner is a small command-line tool that helps you **bulk add/update project users** in Autodesk Construction Cloud (ACC / Forma) from a CSV. It can also **sync** the current hub state (projects + project members) into a local SQLite cache so you can do a safe “what will change?” dry-run before importing.

### What you usually do (3 commands)

1) **Choose which hub/account you are working with**

List configured hubs (from `.env`):

```bash
python -m provisioner hubs list
```

Choose the active hub (interactive; saves to `data/active_hub.json`):

```bash
python -m provisioner hubs choose
```

2) **Sync projects + current users into the local DB**

```bash
python -m provisioner sync-hub
```

3) **Import a CSV (dry-run first, then real import)**

```bash
python -m provisioner import-csv --csv users_to_import/your.csv --dry-run --report logs/import-plan.json
python -m provisioner import-csv --csv users_to_import/your.csv --report logs/import-result.json
```

### Quick setup

- **Python**: 3.10+ recommended
- Install the package (dev/editable):

```bash
python -m pip install -e .
```

### Configure hubs (dev vs prod)

Copy `.env.example` to `.env` and fill in your values.

- `HUBS=...`: comma-separated hub keys (for example: `swissgrid_dev,swissgrid_prod`)
- For each hub key you define:
  - `HUB_<key>_ID`: **Construction Admin account id** (UUID)
  - `HUB_<key>_DM_HUB_ID`: **Data Management hub id** (usually `b.<uuid>`) for listing projects
  - `HUB_<key>_CLIENT_ID` / `HUB_<key>_CLIENT_SECRET`: APS app credentials

List hubs you configured:

```bash
python -m provisioner hubs list
```

Choose the active hub (saved in `data/active_hub.json`):

```bash
python -m provisioner hubs choose
```

If you have both dev+prod hubs configured, you typically:
- choose `swissgrid_dev` when testing
- choose `swissgrid_prod` when ready for the real import

### Authentication (APS)

Provisioner can obtain an APS access token for you.

- **3-legged (default)**: opens a browser once, then stores tokens under `data/tokens/` (ignored by git)

```bash
python -m provisioner auth login
python -m provisioner auth status
```

- **Get/print a valid token** (refreshes if needed):

```bash
python -m provisioner auth token
```

Tip: for automation you can pass `--no-browser` to fail fast (instead of opening a login window).

### How to import a CSV into Forma (end-to-end)

This is the “friendly” flow that answers: **choose hub → dry-run → real import**.

1) Choose the hub you want (dev or prod)

```bash
python -m provisioner hubs choose
```

2) Sync the hub (projects + existing users) into the DB

```bash
python -m provisioner sync-hub
```

3) Make sure hub roles + companies are cached (needed to turn names into IDs)

- **Roles** (offline JSON list you maintain):

```bash
python -m provisioner cache-hub-roles --hub-id YOUR_ACCOUNT_ID --from-json data/hub_roles_TST.json
```

- **Companies** (fetched from APS):

```bash
python -m provisioner cache-hub-companies --hub-id YOUR_ACCOUNT_ID --access-token "$(python -m provisioner auth token --no-browser)"
```

4) Dry-run import (no APS writes; produces a plan/report)

```bash
python -m provisioner import-csv --csv users_to_import/your.csv --dry-run --report logs/import-plan.json
```

5) Real import (calls `users:import`; produces an outcome report)

```bash
python -m provisioner import-csv --csv users_to_import/your.csv --report logs/import-result.json
```

### How to sync projects (and users) for the active hub

`sync-hub` lists all projects in the hub, then for each project it lists current members, and stores it into SQLite.

```bash
python -m provisioner sync-hub
```

### How to “sync hubs”

Provisioner does not “sync hubs from APS” into the DB (hubs come from your `.env`). What you can do is:

- list hubs you configured:

```bash
python -m provisioner hubs list
```

- change the active hub:

```bash
python -m provisioner hubs choose
```

### CLI command reference (easy descriptions)

Run `python -m provisioner --help` to see all commands. Here is what each one does:

- **`hubs list`**: Show hub keys loaded from `.env` (your dev/prod choices).
- **`hubs choose`**: Set the “active hub” used by commands that don’t get `--hub-key`/`--hub-id`.

- **`auth login`**: Sign in (3-legged) and save tokens under `data/tokens/`.
- **`auth status`**: Show whether the token file exists and if it’s expired.
- **`auth token`**: Print a valid access token (refresh or login if needed).

- **`sync-hub`**: Fetch all projects for the active hub, then fetch all users per project; update the SQLite cache (used for dry-runs and diffing).

- **`validate-csv`**: Validate all CSV files in `users_to_import/` and report row-level issues.

- **`build-payload`**: Convert a CSV into per-project JSON payload files (what would be POSTed to `users:import`).
- **`import-csv`**: Import users from a CSV into ACC (batched per project). Use `--dry-run` to only compute the plan from the DB.

- **`db-init`**: Create the SQLite schema file (usually not needed; other commands initialize automatically).
- **`cache-projects`**: Cache projects into SQLite (mostly for troubleshooting; `sync-hub` normally handles this).
- **`lookup-project`**: Find a `project_id` from a project name using the SQLite cache.
- **`cache-hub-roles`**: Cache account-level roles into SQLite (required for mapping role names → roleIds).
- **`cache-hub-companies`**: Cache account-level companies into SQLite (required for mapping company names → companyId).

### Project structure

- `src/`: Python package source
- `users_to_import/`: input CSV files to import
- `data/`: local cache (SQLite DB, OAuth tokens, active hub selection)
- `data_test/`: sample JSON/CSV/env fixtures for local testing
- `logs/`: log files and import reports
