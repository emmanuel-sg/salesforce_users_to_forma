## Provisioner (ACC/FORMA user provisioning CLI)

Provisioner is a small command-line tool that helps you **bulk add/update project users** in Autodesk Construction Cloud (ACC / Forma) from a CSV. It can also **sync** the current hub state (projects + project members) into a local SQLite cache so you can do a safe “what will change?” dry-run before importing.

### What you usually do (3 commands)

1) **Sync projects + current users into the local DB**

```bash
python -m provisioner sync-hub --hub-key swissgrid_dev
```

2) **Import a CSV (dry-run first, then real import)**

```bash
python -m provisioner import-csv --hub-key swissgrid_dev --csv users_to_import/your.csv --dry-run --report logs/import-plan.json
python -m provisioner import-csv --hub-key swissgrid_dev --csv users_to_import/your.csv --report logs/import-result.json
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

You select which hub to use per command with `--hub-key ...` (for example `swissgrid_dev` vs `swissgrid_prod`).

### Authentication (APS)

Provisioner can obtain an APS access token for you.

- **3-legged (default)**: opens a browser once, then stores tokens under `data/tokens/` (ignored by git)

```bash
python -m provisioner import-csv --hub-key swissgrid_dev --csv users_to_import/your.csv --dry-run
```

Tip: for automation you can pass `--no-browser` and/or an explicit `--access-token ...`.

### How to import a CSV into Forma (end-to-end)

This is the “friendly” flow that answers: **choose hub → dry-run → real import**.

1) Choose the hub you want (dev or prod)

```bash
python -m provisioner sync-hub --hub-key swissgrid_dev
```

2) Sync the hub (projects + existing users) into the DB

```bash
python -m provisioner sync-hub --hub-key swissgrid_dev
```

3) Dry-run import (no APS writes; produces a plan/report)

```bash
python -m provisioner import-csv --hub-key swissgrid_dev --csv users_to_import/your.csv --dry-run --report logs/import-plan.json
```

4) Real import (calls `users:import`; produces an outcome report)

```bash
python -m provisioner import-csv --hub-key swissgrid_dev --csv users_to_import/your.csv --report logs/import-result.json
```

Note: `import-csv` will automatically fetch/cache hub roles + companies into SQLite when needed (so you usually don’t need separate cache commands).

Dry-run note: `import-csv --dry-run` does **not** import users, but it may still call APS to fetch/cache hub roles and companies if they are missing locally (read-only). It will **not** create missing companies in dry-run; those rows are reported as “would be created on real import”.

### How to sync projects (and users) for the active hub

`sync-hub` lists all projects in the hub, then for each project it lists current members, and stores it into SQLite.

```bash
python -m provisioner sync-hub --hub-key swissgrid_dev
```

### CLI command reference (easy descriptions)

Run `python -m provisioner --help` to see all commands. Here is what each one does:

- **`sync-hub`**: Fetch all projects for the active hub, then fetch all users per project; update the SQLite cache (used for dry-runs and diffing).
- **`import-csv`**: Import users from a CSV into ACC (batched per project). Use `--dry-run` to only compute the plan from the DB.
- **`cache-projects`**: Cache projects into SQLite (projects only; `sync-hub` normally supersedes this).

### Project structure

- `src/`: Python package source
- `users_to_import/`: input CSV files to import
- `data/`: local cache (SQLite DB, OAuth tokens, active hub selection)
- `data_test/`: sample JSON/CSV/env fixtures for local testing
- `logs/`: log files and import reports
