from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ANALYSIS_DIR = Path(__file__).resolve().parents[1]
ROOT = ANALYSIS_DIR.parents[1]
sys.path.insert(0, str(ANALYSIS_DIR))

import repository_analysis as analysis  # noqa: E402


class RepositoryAnalysisTests(unittest.TestCase):
    def test_comment_and_literal_masking_preserves_offsets(self) -> None:
        source = "-- CREATE TABLE fake.x(a int)\nSELECT 'FROM hidden.x';\nFROM real.x;\n"
        masked = analysis.mask_sql(source)
        self.assertEqual(len(source), len(masked))
        self.assertEqual(source.count("\n"), masked.count("\n"))
        self.assertNotIn("fake.x", masked)
        self.assertNotIn("hidden.x", masked)
        self.assertIn("real.x", masked)

    def test_identifier_normalization(self) -> None:
        self.assertEqual(
            analysis.normalize_identifier("[DataWarehouse].[silver].[crm_cust_info]"),
            "silver.crm_cust_info",
        )
        self.assertEqual(analysis.normalize_identifier("[gold].[fact_sales]"), "gold.fact_sales")

    def test_repository_contract_counts(self) -> None:
        inventory = analysis.build_inventory(ROOT)
        self.assertEqual(analysis.count_inventory(inventory), analysis.EXPECTED_COUNTS)
        self.assertEqual(analysis.check_inventory(inventory, ROOT), [])

    def test_output_is_deterministic(self) -> None:
        first = analysis.serialize(analysis.build_inventory(ROOT), "json")
        second = analysis.serialize(analysis.build_inventory(ROOT), "json")
        self.assertEqual(first, second)
        json.loads(first)
        self.assertNotIn(str(ROOT), first)

    def test_sqlcmd_targets_exist(self) -> None:
        inventory = analysis.build_inventory(ROOT)
        self.assertGreater(len(inventory["sqlcmd_includes"]), 0)
        for include in inventory["sqlcmd_includes"]:
            self.assertTrue((ROOT / include["target"]).is_file(), include)

    def test_legacy_candidates_never_authorize_deletion(self) -> None:
        inventory = analysis.build_inventory(ROOT)
        self.assertGreater(len(inventory["legacy_candidates"]), 0)
        self.assertTrue(
            all(candidate["deletion_authorized"] is False for candidate in inventory["legacy_candidates"])
        )

    def test_empty_csv_fails_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "datasets" / "empty.csv"
            path.parent.mkdir(parents=True)
            path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "CSV source is empty"):
                analysis.scan_csv(root)


if __name__ == "__main__":
    unittest.main()
