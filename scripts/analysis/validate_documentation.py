#!/usr/bin/env python3
"""Validate the durable documentation contract for this repository."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from repository_analysis import build_inventory, mask_sql


ROOT = Path(__file__).resolve().parents[2]
REQUIRED_FILES = (
    "NOTICE.md",
    "CONTRIBUTING.md",
    "docs/project/project_overview.md",
    "docs/project/attribution.md",
    "docs/project/change_requests.md",
    "docs/project/implementation_proposals.md",
    "docs/architecture/system_architecture.md",
    "docs/architecture/data_lineage.md",
    "docs/data/source_to_target_mapping.md",
    "docs/data/data_catalog.md",
    "docs/data/dependency_analysis.md",
    "docs/data/data_dictionary.md",
    "docs/data/business_glossary.md",
    "docs/data/data_quality_rules.md",
    "docs/legacy/legacy_object_inventory.md",
    "docs/legacy/deprecation_and_removal.md",
)
PRIMARY_SOURCE_URLS = (
    "https://github.com/DataWithBaraa/sql-data-warehouse-project",
    "https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/LICENSE",
    "https://www.datawithbaraa.com/wiki/sql",
)
LOCAL_LINK_RE = re.compile(r"\[[^\]]+\]\((?!https?://|#)([^)]+)\)")
MERMAID_RE = re.compile(r"```mermaid\s*\n(.*?)```", re.DOTALL)


def documentation_files(root: Path) -> list[Path]:
    """Return tracked and non-ignored Markdown artifacts, including unstaged additions."""

    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.md"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        paths = [root / value.decode("utf-8", errors="surrogateescape") for value in result.stdout.split(b"\0") if value]
        return sorted(path for path in paths if path.is_file())
    except (OSError, subprocess.CalledProcessError):
        excluded = {".git", ".pytest_cache", "venv", "sqlvenv", "__pycache__"}
        return sorted(
            path for path in root.rglob("*.md") if path.is_file() and not excluded.intersection(path.parts)
        )


def markdown_tables(text: str) -> list[tuple[list[str], list[dict[str, str]]]]:
    """Parse well-formed pipe tables without coupling validation to prose wording."""

    lines = text.splitlines()
    tables: list[tuple[list[str], list[dict[str, str]]]] = []
    index = 0
    while index + 1 < len(lines):
        if not lines[index].strip().startswith("|") or not lines[index + 1].strip().startswith("|"):
            index += 1
            continue

        def cells(line: str) -> list[str]:
            return [cell.strip() for cell in line.strip().strip("|").split("|")]

        headers = cells(lines[index])
        separator = cells(lines[index + 1])
        if len(headers) != len(separator) or not all(
            re.fullmatch(r":?-{3,}:?", cell) for cell in separator
        ):
            index += 1
            continue
        index += 2
        rows: list[dict[str, str]] = []
        while index < len(lines) and lines[index].strip().startswith("|"):
            values = cells(lines[index])
            if len(values) == len(headers):
                rows.append({header: value for header, value in zip(headers, values)})
            index += 1
        tables.append((headers, rows))
    return tables


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def validate_markdown_structure(root: Path, paths: list[Path]) -> list[str]:
    failures: list[str] = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8")
        for match in LOCAL_LINK_RE.finditer(text):
            target_text = match.group(1).split("#", 1)[0]
            if not target_text:
                continue
            target = (path.parent / target_text).resolve()
            if not target.exists():
                failures.append(f"Broken local link in {relative}: {match.group(1)}")
        for block in MERMAID_RE.findall(text):
            if not re.search(r"\b(flowchart|graph|sequenceDiagram|erDiagram)\b", block):
                failures.append(f"Unsupported or missing Mermaid diagram type in {relative}")
            if block.count("[") != block.count("]"):
                failures.append(f"Unbalanced Mermaid brackets in {relative}")
    return failures


def validate_structured_data_docs(root: Path) -> list[str]:
    failures: list[str] = []
    data_dictionary_rows: list[dict[str, str]] = []
    contracts = {
        "docs/data/data_dictionary.md": {
            "object",
            "column",
            "datatype",
            "nullable",
            "businessmeaning",
            "sourcederivation",
            "dqrules",
            "sensitivity",
            "owner",
        },
        "docs/data/business_glossary.md": {"term", "definition"},
        "docs/data/data_quality_rules.md": {
            "rulecode",
            "severity",
            "scope",
            "condition",
            "dispositionresponse",
            "owner",
        },
    }
    for relative, required_headers in contracts.items():
        path = root / relative
        if not path.is_file():
            failures.append(f"Missing required artifact: {relative}")
            continue
        candidates = markdown_tables(path.read_text(encoding="utf-8"))
        matching = [
            (headers, rows)
            for headers, rows in candidates
            if required_headers <= {normalize_header(header) for header in headers}
        ]
        if not matching:
            failures.append(
                f"Structured documentation table in {relative} lacks required fields: "
                f"{sorted(required_headers)}"
            )
            continue
        _, rows = matching[0]
        if relative == "docs/data/data_dictionary.md":
            data_dictionary_rows = rows
        if not rows:
            failures.append(f"Structured documentation table has no data rows: {relative}")
            continue
        for row_number, row in enumerate(rows, start=1):
            normalized_row = {normalize_header(header): value for header, value in row.items()}
            missing_values = sorted(header for header in required_headers if not normalized_row.get(header))
            if missing_values:
                failures.append(
                    f"Structured documentation row {row_number} in {relative} has empty fields: {missing_values}"
                )
    if data_dictionary_rows:
        documented_source_columns = {
            (
                row.get("Object", "").strip().strip("`").lower(),
                row.get("Column", "").strip().strip("`").lower(),
            )
            for row in data_dictionary_rows
        }
        inventory = build_inventory(root)
        for source in inventory["csv_sources"]:
            for column in source["columns"]:
                key = (source["path"].lower(), column.lower())
                if key not in documented_source_columns:
                    failures.append(f"CSV source column is missing from data dictionary: {source['path']}.{column}")
    return failures


def validate_inventory_source_contract(root: Path) -> list[str]:
    path = root / "datasets" / "source_inventory" / "source_contract.json"
    if not path.is_file():
        return ["Inventory source contract is missing: datasets/source_inventory/source_contract.json"]
    try:
        contract = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"Inventory source contract is invalid JSON: {exc}"]
    product_rule = contract.get("mapping", {}).get("product_rule")
    if not isinstance(product_rule, dict):
        return ["Inventory product_rule must be a structured object"]
    required_fields = {"business_key", "temporal_predicate", "cardinality", "unknown_member_policy"}
    missing = sorted(field for field in required_fields if not product_rule.get(field))
    failures = [f"Inventory product_rule missing fields: {missing}"] if missing else []
    business_key = str(product_rule.get("business_key", "")).lower()
    temporal = " ".join(str(product_rule.get("temporal_predicate", "")).lower().split())
    cardinality = str(product_rule.get("cardinality", "")).lower()
    unknown_policy = str(product_rule.get("unknown_member_policy", "")).lower()
    if "product_id" not in business_key or "product_number" not in business_key:
        failures.append("Inventory product_rule business_key must include product_id and product_number")
    if not all(token in temporal for token in ("snapshot_date", "effective_from", "effective_to", ">=", "<")):
        failures.append("Inventory product_rule temporal_predicate must define the half-open effective interval")
    if "exactly one" not in cardinality or "gold.dim_products" not in cardinality:
        failures.append("Inventory product_rule cardinality must require exactly one gold.dim_products row")
    if "product_key" not in unknown_policy or not re.search(r"(?:>|greater than)\s*(?:0|zero)", unknown_policy):
        failures.append("Inventory product_rule unknown_member_policy must require product_key > 0")
    return failures


def validate_inventory_sql_mapping(root: Path) -> list[str]:
    """Keep both Inventory mapping stages aligned with the structured product rule."""

    failures: list[str] = []
    for relative in (
        "scripts/source_inventory/20_transform_silver.sql",
        "scripts/source_inventory/30_create_gold_views.sql",
    ):
        path = root / relative
        if not path.is_file():
            failures.append(f"Inventory mapping SQL is missing: {relative}")
            continue
        sql = " ".join(mask_sql(path.read_text(encoding="utf-8-sig")).split())
        product_key = re.search(r"\b(?P<product>[A-Za-z_]\w*)\.product_key\s*>\s*0\b", sql, re.IGNORECASE)
        if not product_key:
            failures.append(f"Inventory mapping does not exclude the unknown Product member: {relative}")
            continue
        product = re.escape(product_key.group("product"))
        id_match = re.search(
            rf"\b{product}\.product_id\s*=\s*[A-Za-z_]\w*\.product_id(?:_typed)?\b",
            sql,
            re.IGNORECASE,
        )
        number_match = re.search(
            rf"\bUPPER\s*\(\s*{product}\.product_number\s*\)\s*=\s*[A-Za-z_]\w*\.product_number(?:_clean)?\b",
            sql,
            re.IGNORECASE,
        )
        date_match = re.search(
            rf"\b(?P<date>[A-Za-z_]\w*\.snapshot_date(?:_typed)?)\s*>=\s*{product}\.effective_from\b",
            sql,
            re.IGNORECASE,
        )
        if not id_match or not number_match:
            failures.append(f"Inventory mapping lacks the complete Product business key: {relative}")
        if not date_match:
            failures.append(f"Inventory mapping lacks the effective_from predicate: {relative}")
            continue
        date = re.escape(date_match.group("date"))
        effective_to = re.search(
            rf"{product}\.effective_to\s+IS\s+NULL\s+OR\s+{date}\s*<\s*{product}\.effective_to",
            sql,
            re.IGNORECASE,
        )
        if not effective_to:
            failures.append(f"Inventory mapping lacks the half-open effective_to predicate: {relative}")
    return failures


def validate(render_mermaid: bool = False) -> list[str]:
    failures: list[str] = []
    for relative in REQUIRED_FILES:
        if not (ROOT / relative).is_file():
            failures.append(f"Missing required artifact: {relative}")

    if failures:
        return failures

    failures.extend(validate_structured_data_docs(ROOT))
    failures.extend(validate_inventory_source_contract(ROOT))
    failures.extend(validate_inventory_sql_mapping(ROOT))

    all_text = "\n".join((ROOT / path).read_text(encoding="utf-8") for path in REQUIRED_FILES)
    required_statements = (
        "production-oriented reference implementation",
        "synthetic data",
        "not a production deployment",
        "Baraa Khatib Salkini",
        "Data With Baraa",
    )
    for statement in required_statements:
        if statement.lower() not in all_text.lower():
            failures.append(f"Required transparency statement is missing: {statement}")

    provenance_text = (ROOT / "docs/project/attribution.md").read_text(encoding="utf-8")
    notice_text = (ROOT / "NOTICE.md").read_text(encoding="utf-8")
    for url in PRIMARY_SOURCE_URLS:
        if url not in provenance_text and url not in notice_text:
            failures.append(f"Missing primary-source citation: {url}")

    markdown_files = documentation_files(ROOT)
    failures.extend(validate_markdown_structure(ROOT, markdown_files))

    if render_mermaid:
        renderer = shutil.which("mmdc")
        if renderer is None:
            print("Mermaid renderer unavailable; structural checks only.")
        else:
            with tempfile.TemporaryDirectory(prefix="sql-dw-mermaid-") as temporary:
                temp_root = Path(temporary)
                block_number = 0
                for path in markdown_files:
                    relative = path.relative_to(ROOT).as_posix()
                    text = path.read_text(encoding="utf-8")
                    for block in MERMAID_RE.findall(text):
                        block_number += 1
                        source = temp_root / f"diagram-{block_number}.mmd"
                        target = temp_root / f"diagram-{block_number}.svg"
                        source.write_text(block.strip() + "\n", encoding="utf-8")
                        result = subprocess.run(
                            [renderer, "--input", str(source), "--output", str(target)],
                            check=False,
                            capture_output=True,
                            text=True,
                        )
                        if result.returncode != 0 or not target.is_file():
                            detail = (result.stderr or result.stdout).strip()
                            failures.append(f"Mermaid render failed in {relative}: {detail}")

    inventory = build_inventory(ROOT)
    mapping_text = (ROOT / "docs/data/source_to_target_mapping.md").read_text(encoding="utf-8")
    documented_total = f"{sum(source['data_rows'] for source in inventory['csv_sources']):,}"
    if documented_total not in mapping_text:
        failures.append(f"Source-to-target mapping does not contain current total row count {documented_total}.")
    data_docs = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in (
            "docs/data/source_to_target_mapping.md",
            "docs/data/data_catalog.md",
            "docs/data/dependency_analysis.md",
        )
    ).lower()
    for source in inventory["csv_sources"]:
        if source["path"].lower() not in data_docs:
            failures.append(f"CSV source is undocumented: {source['path']}")
    for item in inventory["objects"]:
        if item["kind"] in {"table", "procedure", "view"} and item["id"] not in data_docs:
            failures.append(f"SQL object is undocumented: {item['id']}")

    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--render-mermaid",
        action="store_true",
        help="Render every Mermaid block when the mmdc executable is available.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures = validate(render_mermaid=args.render_mermaid)
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print("Documentation contract validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
