import sys
import json
import sqlite3
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from provisioner.db import SCHEMA_SQL, replace_project_user_cache  # noqa: E402
from provisioner.import_plan import apply_import_diff, dedupe_users_per_project  # noqa: E402
from provisioner.normalize import normalize_key  # noqa: E402


class _NullLogger:
    def info(self, *_a, **_k):
        return None

    def warning(self, *_a, **_k):
        return None


class TestImportPlan(unittest.TestCase):
    def test_apply_import_diff_add_update_skip(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA_SQL)

        # Cached user that should be SKIP.
        en_skip = normalize_key("skip@test.com")
        replace_project_user_cache(conn, "proj-a", [(en_skip, "c1", json.dumps(["r1"]), 0)])

        payloads = {
            "proj-a": [
                {"email": "skip@test.com", "companyId": "c1", "roleIds": ["r1"], "companyAdmin": False},
                {"email": "add@test.com", "companyId": "c1", "roleIds": ["r1"], "companyAdmin": False},
            ],
            "proj-b": [
                {"email": "upd@test.com", "companyId": "new", "roleIds": ["r1"], "companyAdmin": False},
            ],
        }

        # Cached user that should UPDATE.
        replace_project_user_cache(conn, "proj-b", [(normalize_key("upd@test.com"), "old", json.dumps(["r1"]), 0)])

        payloads = dedupe_users_per_project(payloads, logger=_NullLogger())
        filtered, summary, plans = apply_import_diff(conn, payloads, logger=_NullLogger())

        self.assertEqual(summary.add, 1)
        self.assertEqual(summary.update, 1)
        self.assertEqual(summary.skip_same, 1)
        self.assertIn("proj-a", filtered)
        self.assertIn("proj-b", filtered)
        self.assertEqual(len(filtered["proj-a"]), 1)  # only add@test.com kept
        self.assertEqual(len(filtered["proj-b"]), 1)
        self.assertEqual(len(plans), 3)
        conn.close()


if __name__ == "__main__":
    unittest.main()

