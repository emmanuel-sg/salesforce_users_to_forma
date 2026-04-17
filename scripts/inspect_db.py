"""List table and index names present in ``data/cache.db`` (quick SQLite inventory)."""

import sqlite3


def main() -> None:
    """Print each table/index name and type from ``sqlite_master``."""
    conn = sqlite3.connect("data/cache.db")
    rows = conn.execute(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table','index') ORDER BY type, name"
    ).fetchall()
    for name, typ in rows:
        print(f"{typ} {name}")


if __name__ == "__main__":
    main()
