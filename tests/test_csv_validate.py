import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest


# Allow `import provisioner...` when running tests from repo root.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from provisioner.csv_validate import EXPECTED_HEADER, ValidationSummary, validate_csv_file  # noqa: E402


class TestCsvValidate(unittest.TestCase):
    def test_validate_csv_file_counts_valid_and_skipped(self) -> None:
        with TemporaryDirectory() as td:
            p = Path(td) / "in.csv"
            p.write_text(
                ",".join(EXPECTED_HEADER)
                + "\n"
                + "Alex,Sample,alex@example.com,Proj,RoleA,Company,Member\n"
                + "Bad,Row,bad@example.com,Proj,RoleA,Company,NotARealLevel\n",
                encoding="utf-8",
            )

            calls = {"valid": 0, "row_err": 0, "file_err": 0}

            def on_valid_row(_path: Path, _row: int, _validated) -> None:
                calls["valid"] += 1

            def on_row_error(_path: Path, _row: int, _email, _project_name, _reason: str) -> None:
                calls["row_err"] += 1

            def on_file_error(_path: Path, _reason: str) -> None:
                calls["file_err"] += 1

            summary = validate_csv_file(
                p, on_valid_row=on_valid_row, on_row_error=on_row_error, on_file_error=on_file_error
            )
            self.assertIsInstance(summary, ValidationSummary)
            self.assertEqual(summary.processed, 2)
            self.assertEqual(summary.valid, 1)
            self.assertEqual(summary.skipped, 1)
            self.assertEqual(summary.failed, 0)
            self.assertEqual(calls["valid"], 1)
            self.assertEqual(calls["row_err"], 1)
            self.assertEqual(calls["file_err"], 0)


if __name__ == "__main__":
    unittest.main()

