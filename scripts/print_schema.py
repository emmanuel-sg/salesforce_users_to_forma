"""Print CREATE statements for application tables and indexes in ``data/cache.db`` (SQLite)."""

import sqlite3


def main() -> None:
    """Dump ``sqlite_master`` DDL for non-internal objects to stdout."""
    conn = sqlite3.connect("data/cache.db")
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' "
        "ORDER BY type, name"
    ).fetchall()

    for typ, name, tbl, sql in rows:
        print(f"-- {typ} {name} on {tbl}")
        print((sql or "").strip() + ";")
        print()


if __name__ == "__main__":
    main()
