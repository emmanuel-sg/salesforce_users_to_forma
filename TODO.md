## Implementation TODO (phased, easy → hard)

### Phase 0 — Project scaffolding (easy)
- [x] **Create repo structure + README**
  - **Acceptance criteria**
    - A clear folder layout exists (e.g. `src/`, `users_to_import/`, `data/`, `logs/`).
    - `README.md` explains install, config, login, and running an import.

- [x] **Add dependency management**
  - **Acceptance criteria**
    - A `requirements.txt` exists and installs successfully.

---

### Phase 1 — Input + logging (easy)
- [x] **CSV loader for `/users_to_import/`**
  - **Acceptance criteria**
    - Reads all CSV files in `/users_to_import/`.
    - Validates header exactly: `first_name,last_name,email,project_name,roles,company,access_level`.
    - Parses `roles` as a list split by `;` (trimmed).
    - Normalizes `project_name` and `company` (trim + consistent casing rules).

- [x] **Access level validation**
  - **Acceptance criteria**
    - Only `Member` or `Administrator` are accepted (case-insensitive input ok).
    - Invalid access levels are logged and the row is skipped.

- [x] **Structured logging + “log and continue”**
  - **Acceptance criteria**
    - Every skipped/failed row logs: file, row number, email, project_name, reason.
    - A summary at end shows counts: processed / added / updated / skipped / failed.

---

### Phase 2 — SQLite “memory” (easy → medium)
- [x] **SQLite database initialization**
  - **Acceptance criteria**
    - Creates a local SQLite DB file (e.g. `data/cache.db`) on first run.
    - Has tables for hubs, projects, roles, companies (and optional users later).
    - Basic indexes exist for fast lookups (e.g. by normalized name + project_id).

- [x] **Project cache: `fetch_projects()` + lookup by `project_name`**
  - **Acceptance criteria**
    - Fetches projects for a selected hub and stores `project_id`, `project_name`.
    - Lookup is case-insensitive exact match after normalization.
    - Missing project_name logs an error and skips the row.

---

### Phase 3 — Hub selection + configuration (medium)
- [x] **.env configuration for multiple hubs**
  - **Acceptance criteria**
    - Supports multiple hub entries (hub name + hub id + client id + client secret).
    - CLI can list hubs and select one interactively or via flag.

- [x] **Hub selection flow**
  - **Acceptance criteria**
    - Import command requires a hub selection (flag or prompt).
    - Selected hub context is used for all subsequent API calls in the run.

---

### Phase 4 — APS authentication (2-legged, login once) (medium)
- [ ] **3-legged OAuth “login once” with token cache**
  - **Acceptance criteria**
    - First run opens browser login and completes successfully.
    - Refresh token (or equivalent) is stored locally and reused on next run.
    - If token refresh fails, tool re-triggers login without crashing.

---

### Phase 5 — Roles & companies mapping (medium → hard)
- [x] **create a json file and placeholder to manually insert the name of the roles with its id**
    -the json file would look something like:
              {
            "roles": [
              {
                "id": "0a42401b-2968-460a-8dc9-55b96cd7b3ee",
                "name": "Lieferant_HV Kabel"
              },
              {
                "id": "0c3f5ef7-653f-4abf-a279-2735411c2eef",
                "name": "Fachplaner_Umwelt"
              }
            ]
          }

- [x] **Fetch + cache companies per hub**
  - **Acceptance criteria**
    - `fetch_companies(hub_id)` stores `company_name ↔ company_id` in SQLite.
    - Company name mapping works with normalization (trim + case-insensitive match).
    - Unknown company logs error and skips the row.

- [x] **Build import payload per row**
  - **Acceptance criteria**
    - For each row, resolves `project_id`, `company_id`, and all `roleIds`.
    - Produces JSON compatible with `POST /construction/admin/v1/projects/{projectId}/users:import`.

---

### Phase 6 — Import execution (hard)
- [x] **Call `users:import` API for each project**
  - **Acceptance criteria**
    - Sends request to `POST /construction/admin/v1/projects/{projectId}/users:import`.
    - Handles API errors gracefully (logs and continues).
    - Supports batching multiple users for the same project in one request (optional but recommended).

- [x] **Rate limiting + retries**
  - **Acceptance criteria**
    - Retries transient failures (e.g. 429/5xx) with backoff.
    - Does not retry permanent validation errors (4xx) except auth refresh.

---

### Phase 7 — True ADD vs UPDATE vs SKIP logic (hardest)
- [x] **Fetch current project users (source of truth)**
  - **Acceptance criteria**
    - For each target project in the run, tool can retrieve current user membership from ACC Hub(FORMA).
    - Data includes enough fields to compare company, roles, and access level.

- [x] **Diff engine: decide ADD / UPDATE / SKIP**
  - **Acceptance criteria**
    - If user doesn’t exist → marked ADD.
    - If exists and any of (company/roles/access_level) differs → marked UPDATE.
    - If all same → marked SKIP.
    - Decisions are reflected in final summary counts.

- [x] **(Optional) Cache users with refresh-per-run**
  - **Acceptance criteria**
    - Users can be stored in SQLite for fast comparisons.
    - At start of each run, cache refreshes so it doesn’t rely on stale data.

---

### Phase 8 — UX polish (optional)
- [x] **Dry-run mode**
  - **Acceptance criteria**
    - `--dry-run` prints intended changes (ADD/UPDATE/SKIP) without calling APS APIs.

- [x] **Exportable results report**
  - **Acceptance criteria**
    - Writes a CSV/JSON report of outcomes per row (status + reason if failed/skipped).

