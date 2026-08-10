#!/usr/bin/env python3
"""Fail closed when CI wiring drifts from reviewed repository contracts."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "scripts" / "ci" / "run_ci_pipeline.sql"
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RUNNER = ROOT / "scripts" / "ci" / "run_ci_checks.sh"
FULL_RUNTIME = ROOT / "scripts" / "operations" / "run_full_pipeline.sql"
PUBLIC_PIPELINE = ROOT / "scripts" / "run_pipeline.sql"
OPERATIONAL_RUNNER = ROOT / "scripts" / "operations" / "run_operational_pipeline.sql"

REQUIRED_PIPELINE_INCLUDES = [
    "scripts/init.database.sql",
    "scripts/bronze_layer/create_table_bronze_layer.sql",
    "scripts/bronze_layer/bulk_insert_crm_cust_info.sql",
    "scripts/silver_layer/create_silver_table_structure.sql",
    "scripts/silver_layer/load_silver.sql",
    "scripts/gold_layer/00_create_gold_tables.sql",
    "scripts/gold_layer/10_load_gold.sql",
    "scripts/gold_layer/20_create_gold_indexes.sql",
    "scripts/source_inventory/00_create_objects.sql",
    "scripts/source_inventory/10_load_bronze.sql",
    "scripts/source_inventory/20_transform_silver.sql",
    "scripts/source_inventory/30_create_gold_views.sql",
    "scripts/operations/run_full_pipeline.sql",
    "tests/ci_bronze_load_contract.sql",
]


def fail(message: str) -> None:
    print(f"CI wiring contract failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    pipeline_text = PIPELINE.read_text(encoding="utf-8")
    workflow_text = WORKFLOW.read_text(encoding="utf-8")
    runner_text = RUNNER.read_text(encoding="utf-8")
    full_runtime_text = FULL_RUNTIME.read_text(encoding="utf-8")
    public_pipeline_text = PUBLIC_PIPELINE.read_text(encoding="utf-8")
    operational_runner_text = OPERATIONAL_RUNNER.read_text(encoding="utf-8")

    includes = re.findall(r"^:r\s+/workspace/(\S+)\s*$", pipeline_text, re.MULTILINE)
    if includes != REQUIRED_PIPELINE_INCLUDES:
        fail(
            "pipeline includes changed; reconcile the authoritative runtime/model "
            "order and REQUIRED_PIPELINE_INCLUDES together"
        )

    for relative_path in includes:
        if not (ROOT / relative_path).is_file():
            fail(f"SQLCMD include does not exist: {relative_path}")

    def relative_includes(text: str) -> list[str]:
        values = re.findall(r"^:r\s+(.+?)\s*$", text, re.MULTILINE)
        return [value.strip().replace("\\", "/").removeprefix("./").removeprefix("/workspace/") for value in values]

    expected_public = REQUIRED_PIPELINE_INCLUDES[:-1]
    if relative_includes(public_pipeline_text) != expected_public:
        fail("public pipeline module order differs from the reviewed CI pipeline")
    if relative_includes(operational_runner_text) != ["scripts/operations/run_full_pipeline.sql"]:
        fail("operational runner must delegate only to the canonical full runtime")

    if re.search(r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE)\s+(?:DataWarehouse\.)?silver\.", pipeline_text, re.IGNORECASE):
        fail("CI pipeline contains substitute Silver transformation logic")

    required_runtime_calls = (
        "EXEC control.run_pipeline",
        "@defer_completion = 1",
        "$(SnapshotAsOf)",
        "EXEC gold.usp_load_gold",
        "EXEC bronze.load_inventory_snapshot",
        "SET status = 'SUCCEEDED'",
        "SET status = 'FAILED'",
        "sys.sp_releaseapplock",
    )
    for marker in required_runtime_calls:
        if marker not in full_runtime_text:
            fail(f"canonical runtime marker is missing: {marker}")

    required_runner_tests = (
        "tests/runtime_contract.sql",
        "tests/runtime_silver_coverage.sql",
        "tests/runtime_fail_closed.sql",
        "tests/runtime_run_downstream_failure.sql",
        "tests/runtime_restore_downstream_contract.sql",
        "tests/runtime_restart_downstream.sql",
        "tests/runtime_verify_downstream_restart.sql",
        "tests/runtime_idempotency.sql",
        "tests/runtime_atomicity.sql",
        "tests/runtime_full_success_invariant.sql",
        "tests/model_reproducibility.sql",
        "tests/model_sentinel_contract.sql",
        "tests/model_decimal_arithmetic.sql",
        "tests/model_scd2_reconciliation.sql",
        "tests/source_inventory/run_tests_ci.sql",
    )
    for marker in required_runner_tests:
        if marker not in runner_text:
            fail(f"required end-to-end test is missing from the CI runner: {marker}")

    combined = "\n".join((workflow_text, runner_text, pipeline_text, full_runtime_text, public_pipeline_text, operational_runner_text))
    forbidden_patterns = {
        "floating latest tag": r":latest\b|ubuntu-latest",
        "legacy password variable": r"(?<!MSSQL_)\bSA_PASSWORD\b",
        "password command-line argument": r"(?:^|\s)-P(?:\s|$)",
        "known example password": r"YourStrong!Passw0rd1",
        "retired CI substitute loader": r"load_ci_silver\.sql",
    }
    for label, pattern in forbidden_patterns.items():
        if re.search(pattern, combined, re.IGNORECASE | re.MULTILINE):
            fail(f"{label} is present")

    checkout = re.search(r"uses:\s*actions/checkout@([0-9a-f]{40})\b", workflow_text)
    if checkout is None:
        fail("actions/checkout is not pinned to a full commit SHA")
    if "permissions:\n  contents: read" not in workflow_text:
        fail("workflow permissions are not explicitly read-only")
    if "persist-credentials: false" not in workflow_text:
        fail("checkout credentials are persisted")

    for action_reference in re.findall(r"^\s*uses:\s*([^\s#]+)(?:\s+#.*)?$", workflow_text, re.MULTILINE):
        if action_reference.startswith("./"):
            continue
        if re.fullmatch(r"[^@]+@[0-9a-f]{40}", action_reference) is None:
            fail(f"workflow action is not pinned to a full commit SHA: {action_reference}")

    image = re.search(r'^readonly MSSQL_IMAGE="([^"]+)"$', runner_text, re.MULTILINE)
    if image is None or re.fullmatch(
        r"mcr\.microsoft\.com/mssql/server:2022-CU[0-9]+-ubuntu-22\.04@sha256:[0-9a-f]{64}",
        image.group(1) if image else "",
    ) is None:
        fail("SQL Server image is not pinned by reviewed version tag and SHA-256 digest")

    print("CI wiring contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
