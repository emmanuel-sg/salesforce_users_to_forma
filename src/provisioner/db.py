"""SQLite schema and helpers for hub/project/role/company caches and project user snapshots (Phase 7)."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DbPaths:
    """Placeholder paths holder for future multi-db use."""

    db_path: Path


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS hubs (
  hub_id TEXT PRIMARY KEY,
  hub_name TEXT,
  hub_name_norm TEXT
);

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  hub_id TEXT,
  project_name TEXT NOT NULL,
  project_name_norm TEXT NOT NULL,
  FOREIGN KEY (hub_id) REFERENCES hubs(hub_id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_hub_name_norm
  ON projects(hub_id, project_name_norm);

CREATE TABLE IF NOT EXISTS roles (
  role_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  role_name TEXT NOT NULL,
  role_name_norm TEXT NOT NULL,
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_roles_project_name_norm
  ON roles(project_id, role_name_norm);

CREATE TABLE IF NOT EXISTS companies (
  company_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  company_name TEXT NOT NULL,
  company_name_norm TEXT NOT NULL,
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_project_name_norm
  ON companies(project_id, company_name_norm);

-- Hub-level roles/companies (ACC/FORMA: shared across projects in the account/hub)
CREATE TABLE IF NOT EXISTS hub_roles (
  hub_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  role_name TEXT NOT NULL,
  role_name_norm TEXT NOT NULL,
  PRIMARY KEY (hub_id, role_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hub_roles_hub_name_norm
  ON hub_roles(hub_id, role_name_norm);

CREATE TABLE IF NOT EXISTS hub_companies (
  hub_id TEXT NOT NULL,
  company_id TEXT NOT NULL,
  company_name TEXT NOT NULL,
  company_name_norm TEXT NOT NULL,
  PRIMARY KEY (hub_id, company_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hub_companies_hub_name_norm
  ON hub_companies(hub_id, company_name_norm);

-- Refreshed at the start of each import run (Phase 7); source = APS project users list.
CREATE TABLE IF NOT EXISTS project_user_cache (
  project_id TEXT NOT NULL,
  email_norm TEXT NOT NULL,
  company_id TEXT NOT NULL,
  role_ids_json TEXT NOT NULL,
  company_admin INTEGER NOT NULL,
  PRIMARY KEY (project_id, email_norm)
);

CREATE INDEX IF NOT EXISTS idx_project_user_cache_project
  ON project_user_cache(project_id);
"""


@contextmanager
def connect(db_path: Path) -> sqlite3.Connection:
    """
    Context-managed SQLite connection.

    This helper is intentionally designed to be used as:
    `with connect(path) as conn: ...`
    so the connection is always closed (important on Windows to avoid locked DB files).
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path) -> None:
    """Create tables and indexes if missing by executing :data:`SCHEMA_SQL`."""
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


def upsert_hub(conn: sqlite3.Connection, *, hub_id: str, hub_name: str | None, hub_name_norm: str | None) -> None:
    """Insert or update a row in ``hubs``."""
    conn.execute(
        """
        INSERT INTO hubs (hub_id, hub_name, hub_name_norm)
        VALUES (?, ?, ?)
        ON CONFLICT(hub_id) DO UPDATE SET
          hub_name = excluded.hub_name,
          hub_name_norm = excluded.hub_name_norm
        """,
        (hub_id, hub_name, hub_name_norm),
    )


def upsert_project(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    hub_id: str | None,
    project_name: str,
    project_name_norm: str,
) -> None:
    """Insert or update a project row tied to an optional hub/account id."""
    conn.execute(
        """
        INSERT INTO projects (project_id, hub_id, project_name, project_name_norm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(project_id) DO UPDATE SET
          hub_id = excluded.hub_id,
          project_name = excluded.project_name,
          project_name_norm = excluded.project_name_norm
        """,
        (project_id, hub_id, project_name, project_name_norm),
    )


def purge_stale_projects_for_hub(
    conn: sqlite3.Connection, *, hub_id: str, keep_project_ids: set[str]
) -> int:
    """
    Delete cached projects for a hub that no longer exist upstream.

    This keeps the `projects` table aligned with the current hub project list after a sync.
    Returns the number of deleted projects.

    Notes:
    - Also deletes `project_user_cache` rows for removed project_ids (that table has no FK).
    - Does not touch other hubs' rows.
    """
    rows = conn.execute(
        "SELECT project_id FROM projects WHERE hub_id = ?",
        (hub_id,),
    ).fetchall()
    existing = {str(r[0]) for r in rows}
    stale = sorted(existing - set(keep_project_ids))
    if not stale:
        return 0
    for pid in stale:
        conn.execute("DELETE FROM project_user_cache WHERE project_id = ?", (pid,))
        conn.execute("DELETE FROM projects WHERE project_id = ? AND hub_id = ?", (pid, hub_id))
    return len(stale)


def lookup_project_id(conn: sqlite3.Connection, *, hub_id: str | None, project_name_norm: str) -> str | None:
    """Resolve ``project_id`` from normalized project name, optionally scoped to ``hub_id``."""
    if hub_id is None:
        row = conn.execute(
            "SELECT project_id FROM projects WHERE project_name_norm = ?",
            (project_name_norm,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT project_id FROM projects WHERE hub_id = ? AND project_name_norm = ?",
            (hub_id, project_name_norm),
        ).fetchone()
    return None if row is None else str(row["project_id"])


def upsert_role(
    conn: sqlite3.Connection,
    *,
    role_id: str,
    project_id: str,
    role_name: str,
    role_name_norm: str,
) -> None:
    """Insert or update a project-scoped role (legacy ``roles`` table)."""
    conn.execute(
        """
        INSERT INTO roles (role_id, project_id, role_name, role_name_norm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(role_id) DO UPDATE SET
          project_id = excluded.project_id,
          role_name = excluded.role_name,
          role_name_norm = excluded.role_name_norm
        """,
        (role_id, project_id, role_name, role_name_norm),
    )


def upsert_company(
    conn: sqlite3.Connection,
    *,
    company_id: str,
    project_id: str,
    company_name: str,
    company_name_norm: str,
) -> None:
    """Insert or update a project-scoped company (legacy ``companies`` table)."""
    conn.execute(
        """
        INSERT INTO companies (company_id, project_id, company_name, company_name_norm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(company_id) DO UPDATE SET
          project_id = excluded.project_id,
          company_name = excluded.company_name,
          company_name_norm = excluded.company_name_norm
        """,
        (company_id, project_id, company_name, company_name_norm),
    )


def lookup_role_id(conn: sqlite3.Connection, *, project_id: str, role_name_norm: str) -> str | None:
    """Look up a project-level role id by normalized role name."""
    row = conn.execute(
        "SELECT role_id FROM roles WHERE project_id = ? AND role_name_norm = ?",
        (project_id, role_name_norm),
    ).fetchone()
    return None if row is None else str(row["role_id"])


def lookup_company_id(conn: sqlite3.Connection, *, project_id: str, company_name_norm: str) -> str | None:
    """Look up a project-level company id by normalized company name."""
    row = conn.execute(
        "SELECT company_id FROM companies WHERE project_id = ? AND company_name_norm = ?",
        (project_id, company_name_norm),
    ).fetchone()
    return None if row is None else str(row["company_id"])


def upsert_hub_role(
    conn: sqlite3.Connection,
    *,
    hub_id: str,
    role_id: str,
    role_name: str,
    role_name_norm: str,
) -> None:
    """Insert or update an account/hub-level role used for CSV → ``roleIds`` mapping."""
    conn.execute(
        """
        INSERT INTO hub_roles (hub_id, role_id, role_name, role_name_norm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(hub_id, role_id) DO UPDATE SET
          role_name = excluded.role_name,
          role_name_norm = excluded.role_name_norm
        """,
        (hub_id, role_id, role_name, role_name_norm),
    )


def upsert_hub_company(
    conn: sqlite3.Connection,
    *,
    hub_id: str,
    company_id: str,
    company_name: str,
    company_name_norm: str,
) -> None:
    """Insert or update an account/hub-level company used for CSV → ``companyId`` mapping."""
    conn.execute(
        """
        INSERT INTO hub_companies (hub_id, company_id, company_name, company_name_norm)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(hub_id, company_id) DO UPDATE SET
          company_name = excluded.company_name,
          company_name_norm = excluded.company_name_norm
        """,
        (hub_id, company_id, company_name, company_name_norm),
    )


def lookup_hub_role_id(conn: sqlite3.Connection, *, hub_id: str, role_name_norm: str) -> str | None:
    """Return hub-level ``role_id`` for a normalized role display name."""
    row = conn.execute(
        "SELECT role_id FROM hub_roles WHERE hub_id = ? AND role_name_norm = ?",
        (hub_id, role_name_norm),
    ).fetchone()
    return None if row is None else str(row["role_id"])


def lookup_hub_company_id(conn: sqlite3.Connection, *, hub_id: str, company_name_norm: str) -> str | None:
    """Return hub-level ``company_id`` for a normalized company name."""
    row = conn.execute(
        "SELECT company_id FROM hub_companies WHERE hub_id = ? AND company_name_norm = ?",
        (hub_id, company_name_norm),
    ).fetchone()
    return None if row is None else str(row["company_id"])


def hub_roles_count(conn: sqlite3.Connection, *, hub_id: str) -> int:
    """Return number of cached hub roles for ``hub_id``."""
    row = conn.execute(
        "SELECT COUNT(1) AS n FROM hub_roles WHERE hub_id = ?",
        (hub_id,),
    ).fetchone()
    return 0 if row is None else int(row["n"])


def hub_companies_count(conn: sqlite3.Connection, *, hub_id: str) -> int:
    """Return number of cached hub companies for ``hub_id``."""
    row = conn.execute(
        "SELECT COUNT(1) AS n FROM hub_companies WHERE hub_id = ?",
        (hub_id,),
    ).fetchone()
    return 0 if row is None else int(row["n"])


def replace_project_user_cache(
    conn: sqlite3.Connection,
    project_id: str,
    rows: list[tuple[str, str, str, int]],
) -> None:
    """
    Replace cached members for one project.
    Each row: (email_norm, company_id, role_ids_json, company_admin 0|1).
    """
    conn.execute("DELETE FROM project_user_cache WHERE project_id = ?", (project_id,))
    conn.executemany(
        """
        INSERT INTO project_user_cache (project_id, email_norm, company_id, role_ids_json, company_admin)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(project_id, en, cid, rj, adm) for en, cid, rj, adm in rows],
    )


def get_cached_project_member(
    conn: sqlite3.Connection, *, project_id: str, email_norm: str
) -> dict | None:
    """Return cached member fields for diffing import payloads, or ``None`` if absent."""
    row = conn.execute(
        """
        SELECT company_id, role_ids_json, company_admin
        FROM project_user_cache
        WHERE project_id = ? AND email_norm = ?
        """,
        (project_id, email_norm),
    ).fetchone()
    if row is None:
        return None
    return {
        "company_id": str(row["company_id"]),
        "role_ids": tuple(json.loads(row["role_ids_json"])),
        "company_admin": bool(row["company_admin"]),
    }


def projects_for_hub(conn: sqlite3.Connection, *, hub_id: str) -> list[sqlite3.Row]:
    """List ``project_id`` and ``project_name`` rows for a hub, ordered by name."""
    return list(
        conn.execute(
            """
            SELECT project_id, project_name
            FROM projects
            WHERE hub_id = ?
            ORDER BY project_name COLLATE NOCASE
            """,
            (hub_id,),
        )
    )


def project_user_cache_rows(conn: sqlite3.Connection, *, project_id: str) -> list[sqlite3.Row]:
    """Return all cached user rows for one project for reporting or inspection."""
    return list(
        conn.execute(
            """
            SELECT email_norm, company_id, role_ids_json, company_admin
            FROM project_user_cache
            WHERE project_id = ?
            ORDER BY email_norm COLLATE NOCASE
            """,
            (project_id,),
        )
    )

