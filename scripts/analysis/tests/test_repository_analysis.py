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
import validate_documentation as documentation  # noqa: E402


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

    def test_repository_structured_data_docs_pass(self) -> None:
        self.assertEqual([], documentation.validate_structured_data_docs(ROOT))

    def test_structured_data_docs_require_contract_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            files = {
                "docs/data/data_dictionary.md": "| Object | Column |\n| --- | --- |\n| x | y |\n",
                "docs/data/business_glossary.md": "| Term | Definition |\n| --- | --- |\n| Grain | Row meaning |\n",
                "docs/data/data_quality_rules.md": (
                    "| Rule / code | Severity | Scope | Condition | Disposition / response | Owner |\n"
                    "| --- | --- | --- | --- | --- | --- |\n"
                    "| TEST | Error | Gold | invalid | reject | Data Engineering |\n"
                ),
            }
            for relative, content in files.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            failures = documentation.validate_structured_data_docs(root)
            self.assertTrue(any("data_dictionary.md" in failure for failure in failures), failures)

    def test_inventory_source_contract_requires_temporal_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "datasets/source_inventory/source_contract.json"
            path.parent.mkdir(parents=True)
            contract = {
                "mapping": {
                    "product_rule": {
                        "business_key": "product_id and product_number",
                        "temporal_predicate": "snapshot_date maps to current row",
                        "cardinality": "exactly one gold.dim_products row",
                        "unknown_member_policy": "product_key > 0",
                    }
                }
            }
            path.write_text(json.dumps(contract), encoding="utf-8")
            failures = documentation.validate_inventory_source_contract(root)
            self.assertTrue(any("half-open effective interval" in failure for failure in failures), failures)

    def test_inventory_sql_implements_temporal_product_mapping(self) -> None:
        self.assertEqual([], documentation.validate_inventory_sql_mapping(ROOT))

    def test_markdown_structure_checks_files_outside_required_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "docs/extra.md"
            path.parent.mkdir(parents=True)
            path.write_text("[missing](not-there.md)\n", encoding="utf-8")
            failures = documentation.validate_markdown_structure(root, [path])
            self.assertEqual(["Broken local link in docs/extra.md: not-there.md"], failures)


if __name__ == "__main__":
    unittest.main()
