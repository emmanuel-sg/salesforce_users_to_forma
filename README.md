## Provisioner (ACC/FORMA User Management CLI)

CLI tool to bulk assign and update users in Autodesk Construction Cloud (ACC/FORMA) using Autodesk Platform Services (APS).

### Requirements

- Python 3.10+ recommended

### Setup

Install dependencies (editable install recommended for development):

```bash
python -m pip install -e .
```

### Configure hubs

Copy `.env.example` to `.env` and fill in your hubs.

List configured hubs:

```bash
python -m provisioner hubs list
```

Choose the active hub:

```bash
python -m provisioner hubs choose
```

### APS login

**Default: 3-legged OAuth** — register a **Callback URL** in your APS app that matches `APS_REDIRECT_URI` in `.env` (default `http://127.0.0.1:8089/callback`). Then sign in once; tokens are stored under `data/tokens/` (ignored by git):

```bash
python -m provisioner auth login
python -m provisioner auth status
```

**Optional: 2-legged (client credentials)** — set `APS_AUTH_MODE=client_credentials` (or `APS_USE_CLIENT_CREDENTIALS=1`). Your APS app must be a **confidential** app. No browser: `auth login` requests a token with `grant_type=client_credentials`.

**ACC environment variables (optional):** set `ACC_ENV=TST` (or `PROD`, etc.) to load credentials and user id from suffixed names instead of duplicating per hub:

- `APS_CLIENT_ID_{ACC_ENV}`, `APS_CLIENT_SECRET_{ACC_ENV}` — used for any hub whose `HUB_<key>_CLIENT_ID` / `HUB_<key>_CLIENT_SECRET` are empty (per-hub values still win when set).
- `APS_USER_ID_{ACC_ENV}` — sent as **`x-user-id`** on Construction Admin calls (e.g. `users:import`, list users). If `ACC_ENV` is unset, the header still falls back to `APS_USER_ID_TST` or `APS_USER_ID`.

If you previously used 3-legged, the tool **ignores** a cached token file that still contains a `refresh_token` when client-credentials mode is on, and fetches a new 2-legged token instead (so it does not open the browser for refresh/login).

For **`import-csv`** / **`sync-hub`** with 3-legged auth only, pass **`--no-browser`** to fail fast instead of opening a login window (or use `--access-token`).

Print a valid access token (3-legged: refreshes if needed; 2-legged: fetches or uses cache):

```bash
python -m provisioner auth token
```

### Hub roles & companies (Phase 5)

**Roles:** maintain a single manual file (when you are not using the roles API):

- `data/hub_roles.json` — shape: `{"roles":[{"id":"<uuid>","name":"Role Display Name"}, ...]}`

Load into SQLite:

```bash
python -m provisioner cache-hub-roles --hub-id YOUR_HUB_ID
```

Optional: `--from-json other/path.json` or `--access-token ...` to load roles from APS instead.

**Companies:** loaded from the **hub/account API** (no default JSON file):

```bash
python -m provisioner cache-hub-companies --hub-id YOUR_HUB_ID --access-token "$(python -m provisioner auth token --no-browser)"
```

(`--from-json` exists only for rare offline/testing.)

Build `users:import` JSON from a CSV (`--hub-id` or active hub from `hubs choose`):

```bash
python -m provisioner build-payload --csv users_to_import/your.csv --out-dir data/payloads
```

Call the **users:import** API (batched per project, retries on 429/5xx; OAuth refresh on 401 when using stored tokens):

```bash
python -m provisioner import-csv --csv users_to_import/your.csv
```

Optional: `--access-token ...`, `--batch-size 25`, `--max-retries 5`.

**Phase 8 — dry-run and report:** `--dry-run` loads the CSV and compares to **`project_user_cache` in SQLite only** (no `users:import`, no list-users API). Refresh the cache first with `sync-hub` if you want an up-to-date ADD/UPDATE/SKIP plan. `--report path.csv` or `--report path.json` writes validation skips plus per-row planned actions; after a real import, JSON also includes `post_import` batch counts.

```bash
python -m provisioner import-csv --csv users_to_import/your.csv --dry-run --report logs/import-plan.json
```

Offline / sample fixtures (optional) live under `data_test/` — e.g. `data_test/_phase2_projects.json`, `data_test/_phase5_test.csv`, `data_test/_phase3.env` for hub CLI checks.

### Run

Show CLI help:

```bash
python -m provisioner --help
```

### Project structure

- `src/`: Python package source
- `users_to_import/`: input CSV files to import
- `data/`: local cache (SQLite, OAuth tokens, `hub_roles.json`, `active_hub.json`)
- `data_test/`: sample JSON/CSV/env for local testing (not required for production)
- `logs/`: log files
