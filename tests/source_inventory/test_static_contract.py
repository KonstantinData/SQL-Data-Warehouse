import csv
import json
import unittest
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATASET = REPO_ROOT / "datasets" / "source_inventory" / "inventory_snapshots.csv"
CONTRACT = REPO_ROOT / "datasets" / "source_inventory" / "source_contract.json"

HEADER = [
    "source_row_id",
    "source_system",
    "snapshot_date",
    "warehouse_code",
    "warehouse_name",
    "product_id",
    "product_number",
    "on_hand_qty",
    "reserved_qty",
    "reorder_point_qty",
    "unit_cost",
    "currency_code",
    "extracted_at_utc",
]

WAREHOUSES = {"WH-BER-01", "WH-HAM-01"}
PRODUCTS = {
    (210, "FR-R92B-58"),
    (211, "FR-R92R-58"),
    (218, "SO-B909-M"),
    (219, "SO-B909-L"),
}
EXPECTED_REJECTS = {
    "INV-0010": "DUPLICATE_SUPERSEDED",
    "INV-0011": "NEGATIVE_ON_HAND_QTY",
    "INV-0012": "RESERVED_EXCEEDS_ON_HAND_QTY",
    "INV-0013": "PRODUCT_MAPPING_NOT_FOUND",
}


def normalized(row):
    return {key: (value.strip() if value is not None else "") for key, value in row.items()}


def classify(rows):
    prepared = []
    for raw in rows:
        row = normalized(raw)
        row["source_system"] = row["source_system"].upper()
        row["warehouse_code"] = row["warehouse_code"].upper()
        row["product_number"] = row["product_number"].upper()
        row["currency_code"] = row["currency_code"].upper()
        reason = None
        try:
            row["snapshot_date_typed"] = date.fromisoformat(row["snapshot_date"])
        except ValueError:
            reason = "INVALID_SNAPSHOT_DATE"
            row["snapshot_date_typed"] = None
        try:
            row["product_id_typed"] = int(row["product_id"])
        except ValueError:
            reason = reason or "INVALID_PRODUCT_ID"
            row["product_id_typed"] = None
        try:
            row["on_hand_qty_typed"] = int(row["on_hand_qty"])
            row["reserved_qty_typed"] = int(row["reserved_qty"])
            row["reorder_point_qty_typed"] = int(row["reorder_point_qty"])
            row["unit_cost_typed"] = Decimal(row["unit_cost"])
        except (ValueError, InvalidOperation):
            reason = reason or "INVALID_NUMERIC_VALUE"
        row["extracted_at_typed"] = datetime.fromisoformat(row["extracted_at_utc"].replace("Z", "+00:00"))
        if reason is None and row["warehouse_code"] not in WAREHOUSES:
            reason = "WAREHOUSE_MAPPING_NOT_FOUND"
        if reason is None and (row["product_id_typed"], row["product_number"]) not in PRODUCTS:
            reason = "PRODUCT_MAPPING_NOT_FOUND"
        if reason is None and row["on_hand_qty_typed"] < 0:
            reason = "NEGATIVE_ON_HAND_QTY"
        if reason is None and row["reserved_qty_typed"] > row["on_hand_qty_typed"]:
            reason = "RESERVED_EXCEEDS_ON_HAND_QTY"
        row["reason"] = reason
        prepared.append(row)

    groups = defaultdict(list)
    for row in prepared:
        if row["reason"] is None:
            key = (row["snapshot_date_typed"], row["warehouse_code"], row["product_id_typed"])
            groups[key].append(row)
    for group in groups.values():
        group.sort(key=lambda row: (row["extracted_at_typed"], row["source_row_id"]), reverse=True)
        for duplicate in group[1:]:
            duplicate["reason"] = "DUPLICATE_SUPERSEDED"
    return prepared


class SourceInventoryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with DATASET.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            cls.fieldnames = reader.fieldnames
            cls.rows = list(reader)
        cls.classified = classify(cls.rows)

    def test_csv_shape_and_identity_are_deterministic(self):
        self.assertEqual(self.fieldnames, HEADER)
        self.assertEqual(len(self.rows), 14)
        source_ids = [row["source_row_id"].strip() for row in self.rows]
        self.assertEqual(len(source_ids), len(set(source_ids)))

    def test_expected_acceptance_and_reject_partition(self):
        rejects = {row["source_row_id"]: row["reason"] for row in self.classified if row["reason"]}
        self.assertEqual(rejects, EXPECTED_REJECTS)
        self.assertEqual(sum(row["reason"] is None for row in self.classified), 10)
        self.assertEqual(Counter(rejects.values()), Counter(EXPECTED_REJECTS.values()))

    def test_normalization_case_is_accepted(self):
        row = next(item for item in self.classified if item["source_row_id"] == "INV-0009")
        self.assertIsNone(row["reason"])
        self.assertEqual(row["source_system"], "SYNTHETIC_WMS")
        self.assertEqual(row["warehouse_code"], "WH-BER-01")
        self.assertEqual(row["product_number"], "FR-R92B-58")
        self.assertEqual(row["currency_code"], "EUR")

    def test_json_contract_matches_fixture_expectations(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        outcomes = contract["expected_fixture_outcomes"]
        self.assertEqual(outcomes["bronze_rows"], 14)
        self.assertEqual(outcomes["silver_rows"], 10)
        self.assertEqual(outcomes["reject_rows"], 4)
        self.assertEqual(outcomes["gold_rows"], 10)
        self.assertEqual(outcomes["reject_reason_counts"], dict(Counter(EXPECTED_REJECTS.values())))

    def test_sqlcmd_entrypoint_order(self):
        runner = (REPO_ROOT / "scripts" / "source_inventory" / "run_source_inventory.sql").read_text(encoding="utf-8")
        includes = [line.strip() for line in runner.splitlines() if line.strip().lower().startswith(":r")]
        self.assertEqual(
            includes,
            [
                r":r .\scripts\source_inventory\00_create_objects.sql",
                r":r .\scripts\source_inventory\10_load_bronze.sql",
                r":r .\scripts\source_inventory\20_transform_silver.sql",
                r":r .\scripts\source_inventory\30_create_gold_views.sql",
            ],
        )
        self.assertIn(":on error exit", runner.lower())

    def test_documentation_exposes_reserved_integration_hooks(self):
        documentation = (REPO_ROOT / "docs" / "data" / "source_inventory.md").read_text(encoding="utf-8")
        self.assertIn(r":r .\scripts\source_inventory\run_source_inventory.sql", documentation)
        self.assertIn("gold.fact_inventory_snapshots", documentation)
        self.assertIn("semi-additive", documentation)
        self.assertIn("not a production deployment", documentation.lower())


if __name__ == "__main__":
    unittest.main()
