import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from provisioner.db import connect, init_db, upsert_hub_company, upsert_hub_role, upsert_project  # noqa: E402
from provisioner.normalize import normalize_key  # noqa: E402
from provisioner.payload_build import PRODUCT_KEYS, collect_import_payloads_from_csv  # noqa: E402


class _NullLogger:
    def warning(self, *_a, **_k):
        return None


class TestPayloadBuild(unittest.TestCase):
    def test_collect_payload_builds_products_from_access_level(self) -> None:
        with TemporaryDirectory() as td:
            db_path = Path(td) / "cache.db"
            init_db(db_path)

            hub_id = "hub-1"
            proj_id = "p1"
            proj_name = "SAAA-ProvisionerAAA"

            role_id = "r1"
            role_name = "Swissgrid_intern"
            company_id = "c1"
            company_name = "Swissgrid AG"

            with connect(db_path) as conn:
                upsert_project(
                    conn,
                    project_id=proj_id,
                    hub_id=hub_id,
                    project_name=proj_name,
                    project_name_norm=normalize_key(proj_name),
                )
                upsert_hub_role(
                    conn,
                    hub_id=hub_id,
                    role_id=role_id,
                    role_name=role_name,
                    role_name_norm=normalize_key(role_name),
                )
                upsert_hub_company(
                    conn,
                    hub_id=hub_id,
                    company_id=company_id,
                    company_name=company_name,
                    company_name_norm=normalize_key(company_name),
                )

            csv_path = Path(td) / "in.csv"
            csv_path.write_text(
                "first_name,last_name,email,project_name,roles,company,access_level\n"
                "Alex,Sample,alex.sample@example.com,SAAA-ProvisionerAAA,Swissgrid_intern,Swissgrid AG,Member\n",
                encoding="utf-8",
            )

            payloads, skipped, _skips = collect_import_payloads_from_csv(
                db_path=db_path, hub_id=hub_id, csv_path=csv_path, logger=_NullLogger()
            )
            self.assertEqual(skipped, 0)
            self.assertIn(proj_id, payloads)
            self.assertEqual(len(payloads[proj_id]), 1)
            u = payloads[proj_id][0]
            self.assertEqual(u["companyId"], company_id)
            self.assertEqual(u["roleIds"], [role_id])
            keys = [p["key"] for p in u["products"]]
            self.assertEqual(keys, list(PRODUCT_KEYS))
            self.assertTrue(all(p["access"] == "member" for p in u["products"]))


if __name__ == "__main__":
    unittest.main()

