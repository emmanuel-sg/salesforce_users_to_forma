## User Management Tool (ACC/FORMA) — Specification

### 1. Overview
Build a **User Management Tool** for **Autodesk Construction Cloud (ACC, now called FORMA)** using APIs from **Autodesk Platform Services (APS)**.

The tool is a **CLI** that imports and updates project users in bulk.

---

### 2. Core Features

#### 2.1 Bulk User Assignment
The system must support:
- Add multiple users to a single project
- Add multiple users to multiple projects
- Add one user to multiple projects

In practice, the input file uses **one row = one (user, project) pair**. The same user may appear on multiple rows for different projects.

---

### 3. User Data Model
Each user record must include:
- **email**: string (unique identifier)
- **first_name**: string
- **last_name**: string
- **company**: string
- **role(s)**: string (may contain multiple roles)
- **access_level**: string (only: `Member` or `Administrator`)

---

### 4. Input Data Source
Input files are CSV files located in:
- `/users_to_import/`

#### 4.1 CSV Format
The exact header is:

`first_name,last_name,email,project_name,roles,company,access_level`

Example row:

`Joel,Hirschi,joel.hirschi@swissgrid.ch,S055_N00001552,swissgrid_intern;Architect,Swissgrid AG,Member`

Notes:
- **roles** can contain multiple roles separated by `;` (semicolon).
- **project_name** is used to look up the corresponding `project_id` via the local database (cached projects list).
- **roles** is used to look up the corresponding `roles_id` via the local database (cached projects list).
- **company** is used to look up the corresponding `company_id` via the local database (cached projects list).

we need to remember the json body to import a user: you find it here https://aps.autodesk.com/en/docs/acc/v1/reference/http/admin-v2-projects-project-Id-users-import-POST/



---

### 5. User Processing Logic
For each **CSV row** (user + target project):

IF user does not exist in project:
- ADD user with specified **company**, **role(s)**, and **access_level**

ELSE IF user exists AND (**company OR role(s) OR access_level** is different):
- UPDATE user with new values

ELSE IF user exists AND (**company AND role(s) AND access_level** are the same):
- SKIP user

Error handling:
- If any row fails, **log the error and continue** (do not stop the whole run).

---

### 6. Project Handling
Projects in ACC are identified by `project_id`.

#### 6.1 Fetch Projects
FUNCTION `fetch_projects()`:
- Call APS API to retrieve all projects
- Store `project_id` and `project_name` locally (SQLite cache)

Project name matching:
- Use **case-insensitive exact match after normalization**
- In ACC, **project names are unique** (no duplicates expected)

---

### 7. Roles & Companies Handling
ACC uses IDs internally, so names must be mapped to IDs per project.

#### 7.1 Fetch Roles
FUNCTION `fetch_roles(project_id)`:
- Call APS API
- Return `role_name ↔ role_id` mapping

#### 7.2 Fetch Companies
FUNCTION `fetch_companies(project_id)`:
- Call APS API
- Return `company_name ↔ company_id` mapping

#### 7.3 SQLite caching recommendation (roles, companies, users)
- **Roles**: Cache `role_name ↔ role_id` per `project_id` in SQLite (recommended) to speed up imports and avoid refetching every run.
- **Companies**: Cache `company_name ↔ company_id` per `project_id` in SQLite (recommended) for fast, consistent mapping.
- **Users**: SQLite caching is **optional**.
  - If implementing full **ADD vs UPDATE vs SKIP**, the tool must know current project membership.
  - Best practice: **refresh users per project at the start of a run** (or fetch on demand), then use SQLite for fast comparisons during the import to avoid stale local data.

---

### 8. Data Mapping Logic
When processing CSV rows:
- Map `company` → `company_id`
- Map each role in `roles` → `role_id`

If mapping fails:
- Log an error
- if role does not exist, log also an error
- Skip that row/user for that project

---

### 9. Import API / Roles Support
The API normally used for import is:

`POST https://developer.api.autodesk.com/construction/admin/v1/projects/{projectId}/users:import`

The body supports multiple roles via `roleIds`, e.g.:

```json
{
  "users": [
    {
      "email": "someone@swissgrid.ch",
      "products": [],
      "roleIds": ["<ROLE_ID_1>", "<ROLE_ID_2>"],
      "companyId": "<COMPANY_ID>"
    }
  ]
}
```

Requirement:
- Apply **all** roles from the CSV to `roleIds` (after mapping each role name to an ID).

---

### 10. Storage Decision
Recommended local storage: **SQLite**.

Rationale:
- Lightweight “memory” for caching **projects**, **roles**, **companies**
- Enables fast lookups, deduplication, and change tracking

---

### 11. Hub Handling (Multi-Hub)
There will be **different hubs**, configured in `.env` with:
- hub name and id
- per-hub `client_id` and `client_secret`
 

Requirement:
- The CLI must allow the user to **choose which hub** to use for importing users.

---

### 12. Authentication
Authentication type:
- **3-legged OAuth**

Flow requirement:
- Use **browser login once**
- Cache credentials/tokens for reuse (so subsequent runs are non-interactive unless token refresh fails)

---

### 13. Execution Style
- CLI tool (script)
- On errors: **log and continue**
- No Web UI for now
- No scheduled job for now

