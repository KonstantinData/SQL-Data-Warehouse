#!/usr/bin/env python3
"""Validate the source-controlled Power BI reference without Power BI Desktop.

This intentionally conservative validator checks repository contracts and
cross-file references. It is not a complete PBIR schema, TMDL, DAX, or M parser.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from validate_performance_evidence import validate_performance_evidence

ALLOWED_PREFIXES = (
    ".gitignore",
    "README.md",
    "powerbi/",
    "docs/kpi/",
    "docs/powerbi/",
    "docs/data/",
    "datasets/source_inventory/",
    "scripts/powerbi_validation/",
    "scripts/source_inventory/",
    "scripts/ci/run_ci_checks.sh",
    "tests/source_inventory/",
    "tests/powerbi_rls_data_contract.sql",
    "tests/runtime_silver_coverage.sql",
)
SECRET_SCANNER_IMPLEMENTATIONS = {
    "validate_powerbi_project.py",
    "powerbi_service_contract.py",
}
TRANSIENT_NAMES = {"cache.abf", "localSettings.json", "unappliedChanges.json", "editorSettings.json"}
EVIDENCE_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg"}
SECRET_PATTERNS = {
    "password assignment": re.compile(r"(?i)(password|pwd)\s*[:=]\s*['\"]?[^\s,'\"]+"),
    "connection secret": re.compile(r"(?i)(accountkey|clientsecret|accesskey)\s*="),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "bearer token": re.compile(r"(?i)authorization\s*[:=]\s*bearer\s+"),
}
ABSOLUTE_PATH = re.compile(r"(?i)(?:[a-z]:\\|file://|/users/|/home/)")
TABLE_RE = re.compile(r"^table\s+(?P<name>.+?)\s*$", re.MULTILINE)
COLUMN_RE = re.compile(r"^\tcolumn\s+(?P<name>'[^']+'|[^=\r\n]+?)(?:\s*=.*)?$", re.MULTILINE)
MEASURE_RE = re.compile(r"^\tmeasure\s+(?P<name>'[^']+'|[^=\r\n]+?)\s*=", re.MULTILINE)
RELATION_ENDPOINT_RE = re.compile(r"^\t(?:fromColumn|toColumn):\s+(?P<ref>.+?)\s*$", re.MULTILINE)


def unquote(value: str) -> str:
    value = value.strip()
    return value[1:-1].replace("''", "'") if value.startswith("'") and value.endswith("'") else value


def load_json(path: Path, errors: list[str]) -> Any | None:
    try:
        raw = path.read_bytes()
        is_performance_export = "performance" in path.parts and "raw" in path.parts
        if raw.startswith(b"\xef\xbb\xbf") and not is_performance_export:
            errors.append(f"UTF-8 BOM is not allowed: {path}")
        encoding = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8"
        return json.loads(raw.decode(encoding))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        errors.append(f"Invalid JSON in {path}: {exc}")
        return None


def resolve_child(base: Path, relative: str, errors: list[str], label: str) -> Path:
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError:
        errors.append(f"{label} escapes its project directory: {relative}")
    return candidate


def iter_json_like(root: Path) -> list[Path]:
    suffixes = {".json", ".pbip", ".pbir", ".pbism"}
    return sorted(path for path in root.rglob("*") if path.is_file() and path.suffix in suffixes)


def validate_platform_files(report_dir: Path, semantic_dir: Path, errors: list[str]) -> None:
    expected = {
        report_dir: "Report",
        semantic_dir: "SemanticModel",
    }
    logical_ids: list[str] = []
    expected_schema = (
        "https://developer.microsoft.com/json-schemas/fabric/"
        "gitIntegration/platformProperties/2.0.0/schema.json"
    )

    for item_dir, expected_type in expected.items():
        path = item_dir / ".platform"
        if not path.is_file():
            errors.append(f"Required Fabric Git integration file is missing: {path}")
            continue
        data = load_json(path, errors)
        if not isinstance(data, dict):
            continue
        if data.get("$schema") != expected_schema:
            errors.append(f"Unsupported .platform schema for {expected_type}")
        metadata = data.get("metadata")
        config = data.get("config")
        if not isinstance(metadata, dict) or metadata.get("type") != expected_type:
            errors.append(f".platform item type must be {expected_type}")
        if not isinstance(metadata, dict) or metadata.get("displayName") != "SQLDataWarehouse":
            errors.append(f".platform displayName must be SQLDataWarehouse for {expected_type}")
        if not isinstance(config, dict) or config.get("version") != "2.0":
            errors.append(f".platform config version must be 2.0 for {expected_type}")
        logical_id = config.get("logicalId") if isinstance(config, dict) else None
        try:
            parsed_id = uuid.UUID(str(logical_id))
            if parsed_id.int == 0:
                raise ValueError("nil UUID")
        except (ValueError, AttributeError):
            errors.append(f".platform logicalId must be a non-nil UUID for {expected_type}")
        else:
            logical_ids.append(str(parsed_id))

    if len(logical_ids) != len(set(logical_ids)):
        errors.append("Report and SemanticModel .platform logicalIds must be distinct")


@dataclass
class ModelInventory:
    columns: dict[str, set[str]] = field(default_factory=dict)
    measures: dict[str, set[str]] = field(default_factory=dict)
    measure_formats: dict[str, str | None] = field(default_factory=dict)
    measure_expressions: dict[str, str] = field(default_factory=dict)

    @property
    def all_measures(self) -> set[str]:
        return {measure for measures in self.measures.values() for measure in measures}


def parse_model(definition: Path, errors: list[str]) -> ModelInventory:
    inventory = ModelInventory()
    table_files = sorted((definition / "tables").glob("*.tmdl"))
    if not table_files:
        errors.append("No TMDL table files found")
        return inventory

    for path in table_files:
        text = path.read_text(encoding="utf-8")
        table_match = TABLE_RE.search(text)
        if not table_match:
            errors.append(f"TMDL table declaration missing: {path}")
            continue
        table = unquote(table_match.group("name"))
        if table in inventory.columns:
            errors.append(f"Duplicate TMDL table: {table}")
            continue
        columns = {unquote(match.group("name").strip()) for match in COLUMN_RE.finditer(text)}
        measure_matches = list(MEASURE_RE.finditer(text))
        measures = {unquote(match.group("name").strip()) for match in measure_matches}
        if len(columns) != len(list(COLUMN_RE.finditer(text))):
            errors.append(f"Duplicate column in table {table}")
        if len(measures) != len(measure_matches):
            errors.append(f"Duplicate measure in table {table}")
        inventory.columns[table] = columns
        inventory.measures[table] = measures
        for match in measure_matches:
            measure = unquote(match.group("name").strip())
            remainder = text[match.end() :]
            next_sibling = re.search(r"^\t(?!\t)(?:measure|column|partition|hierarchy)\s+", remainder, re.MULTILINE)
            block = remainder[: next_sibling.start()] if next_sibling else remainder
            format_match = re.search(r"^\t\tformatString:\s*(.+?)\s*$", block, re.MULTILINE)
            inventory.measure_formats[measure] = format_match.group(1).strip() if format_match else None
            first_property = re.search(
                r"^\t\t(?:formatString|displayFolder|description|isHidden|lineageTag):",
                block,
                re.MULTILINE,
            )
            expression = block[: first_property.start()] if first_property else block
            inventory.measure_expressions[measure] = " ".join(expression.split())

    required_tables = {
        "Customers",
        "Products",
        "Sales",
        "Date",
        "Data Quality Checks",
        "Refresh Metadata",
        "Security User Country",
        "_Measures",
    }
    missing = required_tables - inventory.columns.keys()
    if missing:
        errors.append(f"Required TMDL tables missing: {sorted(missing)}")
    return inventory


def pbir_literal(value: Any) -> str | None:
    """Return a PBIR Literal.Value as user-facing text without TMDL quoting."""

    current = value
    for key in ("expr", "Literal", "Value"):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    if not isinstance(current, str):
        return None
    return unquote(current)


def normalized_text(value: str | None) -> str:
    return " ".join((value or "").split())


def hex_color(value: str | None) -> str | None:
    """Return a normalized six-digit hex color or None."""

    if not isinstance(value, str) or not re.fullmatch(r"#[0-9A-Fa-f]{6}", value.strip()):
        return None
    return value.strip().upper()


def relative_luminance(color: str) -> float:
    channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4 for channel in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast_ratio(foreground: str, background: str) -> float:
    first = relative_luminance(foreground)
    second = relative_luminance(background)
    return (max(first, second) + 0.05) / (min(first, second) + 0.05)


def pbir_color(value: Any) -> str | None:
    current = value
    for key in ("solid", "color"):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return hex_color(pbir_literal(current))


def markdown_table(text: str, heading: str) -> list[dict[str, str]]:
    """Parse the first Markdown table below an exact level-two heading."""

    section_match = re.search(
        rf"^##\s+{re.escape(heading)}\s*$\n(?P<body>.*?)(?=^##\s+|\Z)",
        text,
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    if not section_match:
        return []
    lines = [line.strip() for line in section_match.group("body").splitlines() if line.strip().startswith("|")]
    if len(lines) < 3:
        return []

    def cells(line: str) -> list[str]:
        return [cell.strip() for cell in line.strip("|").split("|")]

    headers = cells(lines[0])
    separator = cells(lines[1])
    if len(headers) != len(separator) or not all(re.fullmatch(r":?-{3,}:?", cell) for cell in separator):
        return []
    rows: list[dict[str, str]] = []
    for line in lines[2:]:
        values = cells(line)
        if len(values) == len(headers):
            rows.append({header: value for header, value in zip(headers, values)})
    return rows


def validate_rls_documentation(root: Path, errors: list[str]) -> None:
    security_path = root / "docs" / "powerbi" / "security-and-refresh.md"
    validation_path = root / "docs" / "powerbi" / "validation.md"
    quality_path = root / "docs" / "powerbi" / "data-quality-reporting.md"
    acceptance_path = root / "docs" / "powerbi" / "rls-acceptance.md"
    missing = [
        path
        for path in (security_path, validation_path, quality_path, acceptance_path)
        if not path.is_file()
    ]
    if missing:
        errors.extend(f"Required RLS documentation is missing: {path}" for path in missing)
        return
    security_text = security_path.read_text(encoding="utf-8")
    validation_text = validation_path.read_text(encoding="utf-8")
    quality_text = quality_path.read_text(encoding="utf-8")
    acceptance_text = acceptance_path.read_text(encoding="utf-8")

    protection_rows = markdown_table(security_text, "RLS protection matrix")
    expected_boundaries = {
        "Customers": "Protected",
        "Sales": "Protected",
        "Inventory Locations": "Protected",
        "Inventory Snapshots": "Protected",
        "Products": "Global",
        "Date": "Global",
        "Data Quality Checks": "Global",
        "Refresh Metadata": "Global",
    }
    actual_boundaries = {
        row.get("Semantic table", ""): row.get("RLS boundary", "") for row in protection_rows
    }
    if actual_boundaries != expected_boundaries:
        errors.append(
            "RLS protection matrix must classify every required semantic table as Protected or Global"
        )

    validation_rows = markdown_table(validation_text, "RLS validation matrix")
    actual_cases = {row.get("Identity case", "").lower(): row for row in validation_rows}
    required_cases = {"allowed", "multiple", "inactive", "expired", "unknown", "blank"}
    if set(actual_cases) != required_cases:
        errors.append(f"RLS validation matrix identity cases differ: {sorted(actual_cases)}")
    for identity_case, row in actual_cases.items():
        if not row.get("Expected Sales") or not row.get("Expected Inventory") or not row.get("Required evidence"):
            errors.append(f"RLS validation matrix lacks Sales, Inventory, or evidence for {identity_case}")
    for required in (
        "analyst.multiple@example.invalid",
        "blank.country@example.invalid",
        "Inventory Snapshot Lines",
        "Desktop `View as` is a separate Tabular-engine gate",
        "15 = 11 accepted + 4 rejected",
    ):
        if required not in acceptance_text:
            errors.append(f"RLS acceptance documentation lacks {required}")

    evidence_rows = markdown_table(quality_text, "Global and selected-scope evidence")
    evidence_scope = {
        row.get("Evidence", "").lower(): row.get("Scope under `CountrySalesViewer`", "").lower()
        for row in evidence_rows
    }
    expected_scope_tokens = {
        "future customer records": ("protected", "customer"),
        "nonpositive product cost records": ("global", "product"),
        "data-through date": ("protected", "sales"),
    }
    for evidence, tokens in expected_scope_tokens.items():
        scope = evidence_scope.get(evidence, "")
        if any(token not in scope for token in tokens):
            errors.append(f"Data Quality scope matrix is missing or incorrect for {evidence}")


def validate_rls_acceptance_contract(
    root: Path,
    security_text: str,
    errors: list[str],
) -> None:
    path = root / "powerbi" / "rls-acceptance-matrix.json"
    contract = load_json(path, errors)
    if not isinstance(contract, dict):
        errors.append("RLS acceptance matrix contract is missing or invalid")
        return
    if contract.get("role") != "CountrySalesViewer":
        errors.append("RLS acceptance matrix must target CountrySalesViewer")
    try:
        as_of = datetime.fromisoformat(str(contract["asOfUtc"]).replace("Z", "+00:00"))
        if as_of.tzinfo is None:
            raise ValueError("timezone is required")
        as_of = as_of.astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError) as exc:
        errors.append(f"RLS acceptance matrix has invalid asOfUtc: {exc}")
        return

    fixture_pattern = re.compile(
        r'\{"(?P<upn>[^"]*)",\s*"(?P<country>[^"]*)",\s*'
        r'(?P<active>TRUE|FALSE),\s*dt"(?P<valid_from>\d{4}-\d{2}-\d{2})",\s*'
        r'dt"(?P<valid_to>\d{4}-\d{2}-\d{2})"\}'
    )
    fixtures: list[dict[str, Any]] = []
    for match in fixture_pattern.finditer(security_text):
        fixtures.append(
            {
                "identity": match.group("upn").strip().lower(),
                "country": match.group("country").strip().upper(),
                "active": match.group("active") == "TRUE",
                "valid_from": datetime.fromisoformat(match.group("valid_from")).replace(tzinfo=timezone.utc),
                "valid_to": datetime.fromisoformat(match.group("valid_to")).replace(tzinfo=timezone.utc),
            }
        )
    if len(fixtures) != 7:
        errors.append(f"RLS entitlement fixture must contain seven deterministic rows, found {len(fixtures)}")

    baselines = contract.get("countryBaselines")
    if not isinstance(baselines, dict) or set(baselines) != {"DE", "US"}:
        errors.append("RLS acceptance matrix must define DE and US country baselines")
        return
    metric_names = {
        "salesRows",
        "salesTotal",
        "inventoryRows",
        "availableInventoryQuantity",
        "inventoryValue",
    }
    if any(not isinstance(value, dict) or set(value) != metric_names for value in baselines.values()):
        errors.append("RLS acceptance country baselines have an invalid metric shape")
        return

    cases = contract.get("cases")
    if not isinstance(cases, list):
        errors.append("RLS acceptance matrix cases are missing")
        return
    by_case = {str(case.get("case")): case for case in cases if isinstance(case, dict)}
    required_cases = {"Allowed", "Multiple", "Inactive", "Expired", "Unknown", "Blank"}
    if set(by_case) != required_cases or len(cases) != len(required_cases):
        errors.append("RLS acceptance matrix must contain each required case exactly once")
        return

    expected_identities = {
        "Allowed": "analyst.de@example.invalid",
        "Multiple": "analyst.multiple@example.invalid",
        "Inactive": "inactive@example.invalid",
        "Expired": "expired@example.invalid",
        "Unknown": "unknown@example.invalid",
        "Blank": "blank.country@example.invalid",
    }
    for case_name, expected_identity in expected_identities.items():
        case = by_case[case_name]
        identity = str(case.get("identity", "")).strip().lower()
        if identity != expected_identity:
            errors.append(f"RLS acceptance identity differs for {case_name}")
            continue
        allowed = sorted(
            {
                fixture["country"]
                for fixture in fixtures
                if fixture["identity"] == identity
                and fixture["country"]
                and fixture["active"]
                and fixture["valid_from"] <= as_of < fixture["valid_to"]
            }
        )
        documented = case.get("expectedCountries")
        if documented != allowed:
            errors.append(f"RLS acceptance countries differ for {case_name}: {documented} != {allowed}")
            continue
        expected_metrics: dict[str, int | float | None] = {}
        for metric in metric_names:
            if allowed:
                expected_metrics[metric] = sum(baselines[country][metric] for country in allowed)
            elif metric in {"salesRows", "inventoryRows"}:
                expected_metrics[metric] = 0
            else:
                expected_metrics[metric] = None
        if case.get("expected") != expected_metrics:
            errors.append(f"RLS acceptance metrics differ for {case_name}")

    unknown_rows = [fixture for fixture in fixtures if fixture["identity"] == expected_identities["Unknown"]]
    if unknown_rows:
        errors.append("Unknown RLS acceptance identity must not have an entitlement fixture")
    blank_rows = [fixture for fixture in fixtures if fixture["identity"] == expected_identities["Blank"]]
    if len(blank_rows) != 1 or blank_rows[0]["country"] != "":
        errors.append("Blank RLS acceptance identity must have one empty CountryCode fixture")


def validate_relationships(definition: Path, inventory: ModelInventory, errors: list[str]) -> None:
    path = definition / "relationships.tmdl"
    if not path.is_file():
        errors.append("relationships.tmdl is missing")
        return
    text = path.read_text(encoding="utf-8")
    endpoints = [match.group("ref").strip() for match in RELATION_ENDPOINT_RE.finditer(text)]
    if len(endpoints) != 16:
        errors.append(f"Expected eight relationships (sixteen endpoints), found {len(endpoints)} endpoints")
    for endpoint in endpoints:
        table_ref, separator, column_ref = endpoint.rpartition(".")
        table = unquote(table_ref)
        column = unquote(column_ref)
        if not separator or table not in inventory.columns or column not in inventory.columns[table]:
            errors.append(f"Unknown relationship endpoint: {endpoint}")
    if text.count("\tisActive: false") != 2:
        errors.append("Exactly two inactive Date relationships are required")
    blocks = re.split(r"(?=^relationship\s+)", text, flags=re.MULTILINE)
    actual: set[tuple[str, str, bool]] = set()
    for block in blocks:
        from_match = re.search(r"^\tfromColumn:\s+(.+)$", block, re.MULTILINE)
        to_match = re.search(r"^\ttoColumn:\s+(.+)$", block, re.MULTILINE)
        if from_match and to_match:
            actual.add((from_match.group(1).strip(), to_match.group(1).strip(), "\tisActive: false" not in block))
    expected = {
        ("Sales.customer_id", "Customers.customer_id", True),
        ("Sales.product_key", "Products.product_key", True),
        ("Sales.order_date", "Date.Date", True),
        ("Sales.ship_date", "Date.Date", False),
        ("Sales.due_date", "Date.Date", False),
        ("'Inventory Snapshots'.product_key", "Products.product_key", True),
        ("'Inventory Snapshots'.warehouse_key", "'Inventory Locations'.warehouse_key", True),
        ("'Inventory Snapshots'.snapshot_date", "Date.Date", True),
    }
    if actual != expected:
        errors.append(f"Relationship topology differs from the required star schema: {sorted(actual)}")
    if "crossFilteringBehavior: bothDirections" in text:
        errors.append("Bidirectional relationship filtering is not allowed")


def validate_measure_expressions(definition: Path, inventory: ModelInventory, errors: list[str]) -> None:
    graph: dict[str, set[str]] = {measure: set() for measure in inventory.all_measures}
    table_column = re.compile(r"(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_ ]*))\[([^\]]+)\]")
    bare_measure = re.compile(r"(?<![A-Za-z0-9_'])\[([^\]]+)\]")
    for path in sorted((definition / "tables").glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"^\tmeasure\s+('([^']+)'|([^=\r\n]+?))\s*=\s*(.+)$", text, re.MULTILINE):
            measure = match.group(2) or match.group(3).strip()
            expression = match.group(4)
            for ref in table_column.finditer(expression):
                table = ref.group(1) or ref.group(2).strip()
                column = ref.group(3)
                if table not in inventory.columns or column not in inventory.columns[table]:
                    errors.append(f"Measure {measure} references unknown column {table}[{column}]")
            for referenced in bare_measure.findall(expression):
                if referenced not in inventory.all_measures:
                    errors.append(f"Measure {measure} references unknown measure [{referenced}]")
                elif referenced != measure:
                    graph[measure].add(referenced)

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(measure: str) -> None:
        if measure in visiting:
            errors.append(f"Measure dependency cycle detected at [{measure}]")
            return
        if measure in visited:
            return
        visiting.add(measure)
        for dependency in graph.get(measure, set()):
            visit(dependency)
        visiting.remove(measure)
        visited.add(measure)

    for measure in graph:
        visit(measure)


def validate_dq_status_contract(inventory: ModelInventory, errors: list[str]) -> None:
    required = {"DQ Failed Error Checks", "DQ Failed Warning Checks", "Overall DQ Status"}
    missing = required - inventory.all_measures
    if missing:
        errors.append(f"Required DQ status measures missing: {sorted(missing)}")
        return

    error_expression = inventory.measure_expressions["DQ Failed Error Checks"]
    warning_expression = inventory.measure_expressions["DQ Failed Warning Checks"]
    overall_expression = inventory.measure_expressions["Overall DQ Status"]
    for label, expression, severity in (
        ("DQ Failed Error Checks", error_expression, '"Error"'),
        ("DQ Failed Warning Checks", warning_expression, '"Warning"'),
    ):
        required_tokens = ("Data Quality Checks", "Severity", severity)
        if any(token not in expression for token in required_tokens):
            errors.append(f"{label} must count failed checks for Severity={severity}")
        status_based = "Status" in expression and '"Failed"' in expression
        row_based = (
            "IsEvaluated" in expression
            and "FailedRows" in expression
            and re.search(r"FailedRows\]\s*>\s*0", expression)
        )
        if not status_based and not row_based:
            errors.append(f"{label} must identify failed checks through Status or FailedRows")
    for token in (
        "[DQ Failed Error Checks]",
        "[DQ Failed Warning Checks]",
        "[DQ Not Evaluated Checks]",
        '"Not run"',
        '"Failed"',
        '"Passed with warnings"',
        '"Passed"',
    ):
        if token not in overall_expression:
            errors.append(f"Overall DQ Status expression lacks required state contract token: {token}")
    scope_sources = {
        "Future Customer Records": "Customers",
        "Nonpositive Product Cost Records": "Products",
        "Data Through Date": "Sales",
    }
    for measure, source_table in scope_sources.items():
        expression = inventory.measure_expressions.get(measure, "")
        if source_table not in expression:
            errors.append(f"{measure} must derive from {source_table} to preserve its documented RLS scope")


def validate_visual_reference(reference: str, inventory: ModelInventory, errors: list[str], path: Path) -> None:
    table, separator, property_name = reference.rpartition(".")
    if not separator:
        errors.append(f"Malformed visual queryRef {reference!r} in {path}")
        return
    if table not in inventory.columns:
        errors.append(f"Unknown visual table {table!r} in {path}")
        return
    if property_name not in inventory.columns[table] and property_name not in inventory.measures[table]:
        errors.append(f"Unknown visual property {reference!r} in {path}")


def validate_report(report_dir: Path, inventory: ModelInventory, errors: list[str]) -> None:
    report_metadata = load_json(report_dir / "definition" / "report.json", errors)
    default_page: str | None = None
    if isinstance(report_metadata, dict):
        imported_version = report_metadata.get("themeCollection", {}).get("baseTheme", {}).get("reportVersionAtImport")
        expected_keys = {"visual", "page", "report"}
        if not isinstance(imported_version, dict) or set(imported_version) != expected_keys:
            errors.append("reportVersionAtImport must contain visual, page, and report versions")
        annotations = report_metadata.get("annotations", [])
        default_page = next(
            (
                annotation.get("value")
                for annotation in annotations
                if isinstance(annotation, dict) and annotation.get("name") == "defaultPage"
            ),
            None,
        )
    pages_path = report_dir / "definition" / "pages" / "pages.json"
    pages_data = load_json(pages_path, errors)
    if not isinstance(pages_data, dict):
        return
    page_order = pages_data.get("pageOrder")
    if not isinstance(page_order, list) or not page_order:
        errors.append("pages.json must contain a nonempty pageOrder")
        return
    if len(page_order) != len(set(page_order)):
        errors.append("pages.json contains duplicate page IDs")
    if pages_data.get("activePageName") not in page_order:
        errors.append("activePageName is not present in pageOrder")
    if default_page != page_order[0] or pages_data.get("activePageName") != default_page:
        errors.append("Power BI defaultPage, activePageName, and first pageOrder entry must match")

    seen_visual_ids: set[str] = set()
    visual_metadata: dict[str, tuple[str, str, str, set[str]]] = {}
    visual_layouts: dict[str, dict[str, Any]] = {}
    for page_name in page_order:
        page_dir = pages_path.parent / page_name
        page_data = load_json(page_dir / "page.json", errors)
        if not isinstance(page_data, dict):
            continue
        if page_data.get("name") != page_name:
            errors.append(f"Page folder/name mismatch for {page_name}")
        width = page_data.get("width")
        height = page_data.get("height")
        if width != 1280 or height != 720:
            errors.append(f"Page {page_name} must use the 1280x720 design canvas")
        page_objects = page_data.get("objects", {})
        page_background = None
        if isinstance(page_objects, dict) and page_objects.get("background"):
            page_background = pbir_color(
                page_objects["background"][0].get("properties", {}).get("color")
            )
        if page_background != "#F7F9FC":
            errors.append(f"Page {page_name} must persist the accessible #F7F9FC background")
        page_tab_orders: set[int] = set()
        mobile_tab_orders: set[int] = set()
        mobile_positions: list[tuple[float, float, float, float, str]] = []
        visual_paths = sorted((page_dir / "visuals").glob("*/visual.json"))
        if len(visual_paths) < 3:
            errors.append(f"Page {page_name} must contain at least three authored visuals")
        for visual_path in visual_paths:
            data = load_json(visual_path, errors)
            if not isinstance(data, dict):
                continue
            visual_id = data.get("name")
            if visual_id != visual_path.parent.name:
                errors.append(f"Visual folder/name mismatch: {visual_path}")
            if not isinstance(visual_id, str) or not re.fullmatch(r"[a-z0-9_-]{1,64}", visual_id):
                errors.append(f"Invalid visual ID in {visual_path}")
            if visual_id in seen_visual_ids:
                errors.append(f"Duplicate visual ID across report: {visual_id}")
            seen_visual_ids.add(visual_id)
            position = data.get("position", {})
            required = ("x", "y", "width", "height", "tabOrder")
            if not all(
                isinstance(position.get(key), (int, float)) and not isinstance(position.get(key), bool)
                for key in required
            ):
                errors.append(f"Incomplete visual position in {visual_path}")
            else:
                if position["x"] < 0 or position["y"] < 0 or position["x"] + position["width"] > width or position["y"] + position["height"] > height:
                    errors.append(f"Visual exceeds desktop canvas: {visual_path}")
                if position["width"] <= 0 or position["height"] <= 0:
                    errors.append(f"Visual dimensions must be positive: {visual_path}")
                if position["tabOrder"] < 0 or position["tabOrder"] != int(position["tabOrder"]):
                    errors.append(f"Visual tabOrder must be a nonnegative integer: {visual_path}")
                tab_order = int(position["tabOrder"])
                if tab_order in page_tab_orders:
                    errors.append(f"Duplicate tabOrder {tab_order} on page {page_name}")
                page_tab_orders.add(tab_order)
            for query_ref in re.findall(r'"queryRef"\s*:\s*"([^"]+)"', json.dumps(data)):
                validate_visual_reference(query_ref, inventory, errors, visual_path)
            objects = data.get("visual", {}).get("visualContainerObjects", {})
            title_properties = objects.get("title", [{}])[0].get("properties", {}) if objects.get("title") else {}
            title_value = title_properties.get("text")
            alt_value = objects.get("general", [{}])[0].get("properties", {}).get("altText") if objects.get("general") else None
            title_text = pbir_literal(title_value)
            alt_text = pbir_literal(alt_value)
            if not title_text or not alt_text:
                errors.append(f"PBIR title or alt text is missing: {visual_path}")
            if pbir_literal(title_properties.get("show")) != "true":
                errors.append(f"PBIR title must be visible: {visual_path}")
            if alt_text and not 20 <= len(normalized_text(alt_text)) <= 250:
                errors.append(f"PBIR alt text must contain 20 to 250 characters: {visual_path}")
            title_color = pbir_color(title_properties.get("fontColor"))
            background_color = (
                pbir_color(objects["background"][0].get("properties", {}).get("color"))
                if objects.get("background")
                else None
            )
            border_color = (
                pbir_color(objects["border"][0].get("properties", {}).get("color"))
                if objects.get("border")
                else None
            )
            if title_color is None or background_color is None or contrast_ratio(title_color, background_color) < 4.5:
                errors.append(f"PBIR title/background contrast is below 4.5:1: {visual_path}")
            if border_color is None or background_color is None or contrast_ratio(border_color, background_color) < 3.0:
                errors.append(f"PBIR border/background contrast is below 3:1: {visual_path}")
            if isinstance(visual_id, str):
                query_refs = set(re.findall(r'"queryRef"\s*:\s*"([^"]+)"', json.dumps(data)))
                visual_metadata[visual_id] = (
                    str(page_name),
                    normalized_text(title_text),
                    normalized_text(alt_text),
                    query_refs,
                )
                visual_layouts[visual_id] = {
                    "page": str(page_name),
                    "desktop": dict(position),
                    "mobile": None,
                    "visualType": data.get("visual", {}).get("visualType"),
                    "objects": data.get("visual", {}).get("objects", {}),
                }
            mobile_path = visual_path.parent / "mobile.json"
            if mobile_path.is_file():
                mobile = load_json(mobile_path, errors)
                position = mobile.get("position", {}) if isinstance(mobile, dict) else {}
                required_mobile = ("x", "y", "width", "height", "tabOrder")
                if not all(
                    isinstance(position.get(key), (int, float)) and not isinstance(position.get(key), bool)
                    for key in required_mobile
                ):
                    errors.append(f"Incomplete mobile position in {mobile_path}")
                else:
                    if (
                        position["x"] < 0
                        or position["y"] < 0
                        or position["width"] <= 0
                        or position["height"] <= 0
                        or position["x"] + position["width"] > 320
                    ):
                        errors.append(f"Mobile visual exceeds the 320px baseline: {mobile_path}")
                    if position["height"] < 100:
                        errors.append(f"Mobile visual is below the 100-unit readable-height contract: {mobile_path}")
                    if position["tabOrder"] < 0 or position["tabOrder"] != int(position["tabOrder"]):
                        errors.append(f"Mobile tabOrder must be a nonnegative integer: {mobile_path}")
                    mobile_tab_order = int(position["tabOrder"])
                    if mobile_tab_order in mobile_tab_orders:
                        errors.append(f"Duplicate mobile tabOrder {mobile_tab_order} on page {page_name}")
                    mobile_tab_orders.add(mobile_tab_order)
                    mobile_positions.append(
                        (
                            position["x"],
                            position["x"] + position["width"],
                            position["y"],
                            position["y"] + position["height"],
                            visual_id,
                        )
                    )
                    if isinstance(visual_id, str) and visual_id in visual_layouts:
                        visual_layouts[visual_id]["mobile"] = dict(position)
        mobile_positions.sort(key=lambda item: (item[2], item[0]))
        for previous, current in zip(mobile_positions, mobile_positions[1:]):
            horizontal_overlap = current[0] < previous[1] and previous[0] < current[1]
            vertical_overlap = current[2] < previous[3] and previous[2] < current[3]
            if horizontal_overlap and vertical_overlap:
                errors.append(f"Mobile visuals overlap on {page_name}: {previous[4]} and {current[4]}")
            if horizontal_overlap and current[2] - previous[3] < 8:
                errors.append(f"Mobile visual gap is below 8 units on {page_name}: {previous[4]} and {current[4]}")

    blueprint_path = report_dir.parent / "report-blueprint.json"
    blueprint = load_json(blueprint_path, errors)
    if not isinstance(blueprint, dict):
        return
    blueprint_pages = blueprint.get("pages", [])
    if {page.get("name") for page in blueprint_pages} != set(page_order):
        errors.append("Report blueprint pages do not match PBIR pageOrder")
    blueprint_ids: set[str] = set()
    for page in blueprint_pages:
        page_name = page.get("name")
        page_visuals = page.get("visuals", [])
        desktop_focus_order = page.get("desktopFocusOrder")
        mobile_focus_order = page.get("mobileFocusOrder")
        if not isinstance(desktop_focus_order, list) or not isinstance(mobile_focus_order, list):
            errors.append(f"Explicit desktop/mobile focus order is missing for {page_name}")
            desktop_focus_order = []
            mobile_focus_order = []
        actual_desktop_order = [
            visual_id
            for visual_id in sorted(
                (visual.get("id") for visual in page_visuals if visual.get("id") in visual_layouts),
                key=lambda visual_id: visual_layouts[visual_id]["desktop"].get("tabOrder", -1),
            )
        ]
        actual_mobile_order = [
            visual_id
            for visual_id in sorted(
                (
                    visual.get("id")
                    for visual in page_visuals
                    if visual.get("id") in visual_layouts and visual_layouts[visual.get("id")]["mobile"] is not None
                ),
                key=lambda visual_id: visual_layouts[visual_id]["mobile"].get("tabOrder", -1),
            )
        ]
        if desktop_focus_order != actual_desktop_order:
            errors.append(f"Desktop focus order differs from the explicit contract on {page_name}")
        if mobile_focus_order != actual_mobile_order:
            errors.append(f"Mobile focus order differs from the explicit contract on {page_name}")
        mobile_priorities = [visual.get("mobilePriority") for visual in page_visuals if visual.get("mobilePriority") is not None]
        if sorted(mobile_priorities) != list(range(1, len(mobile_priorities) + 1)):
            errors.append(f"Mobile priorities must be unique and consecutive on {page_name}")
        for index, visual in enumerate(page_visuals, start=1):
            visual_id = visual.get("id")
            if visual_id in blueprint_ids:
                errors.append(f"Duplicate blueprint visual ID: {visual_id}")
            blueprint_ids.add(visual_id)
            if not visual.get("title") or not 20 <= len(str(visual.get("altText", ""))) <= 250:
                errors.append(f"Missing meaningful title/alt text for visual {visual_id}")
            if visual.get("screenReaderName") != visual.get("title"):
                errors.append(f"Screen-reader name must match the visible title for visual {visual_id}")
            if visual.get("desktopOrder") != index:
                errors.append(f"desktopOrder differs from visual inventory order for {visual_id}")
            if not visual.get("nonColorCue"):
                errors.append(f"Non-color encoding contract is missing for visual {visual_id}")
            visual_dir = pages_path.parent / str(page_name) / "visuals" / str(visual_id)
            if not (visual_dir / "visual.json").is_file():
                errors.append(f"Blueprint visual is missing from PBIR: {page_name}/{visual_id}")
            elif visual_id in visual_metadata:
                actual_page, actual_title, actual_alt_text, _ = visual_metadata[visual_id]
                expected_title = normalized_text(str(visual.get("title", "")))
                expected_alt_text = normalized_text(str(visual.get("altText", "")))
                if actual_page != str(page_name):
                    errors.append(f"Blueprint/PBIR page mismatch for visual {visual_id}")
                if actual_title != expected_title:
                    errors.append(
                        f"Blueprint/PBIR title mismatch for visual {visual_id}: "
                        f"blueprint={expected_title!r}, PBIR={actual_title!r}"
                    )
                if actual_alt_text != expected_alt_text:
                    errors.append(
                        f"Blueprint/PBIR alt text mismatch for visual {visual_id}: "
                        f"blueprint={expected_alt_text!r}, PBIR={actual_alt_text!r}"
                    )
            if visual.get("mobilePriority") is not None and not (visual_dir / "mobile.json").is_file():
                errors.append(f"Prioritized mobile visual lacks mobile.json: {page_name}/{visual_id}")
            if visual.get("mobilePriority") is None:
                if visual.get("mobileDisposition") != "landscape-desktop-detail" or not visual.get("mobileReason"):
                    errors.append(f"Mobile omission lacks an accessible landscape alternative for {visual_id}")
            elif visual_id in visual_layouts:
                mobile_position = visual_layouts[visual_id]["mobile"]
                expected_tab_order = int(visual.get("mobilePriority")) * 1000
                if not isinstance(mobile_position, dict) or mobile_position.get("tabOrder") != expected_tab_order:
                    errors.append(f"Mobile tabOrder differs from mobilePriority for visual {visual_id}")
            if visual.get("nonColorCue") == "series-labels-markers-and-show-data" and visual_id in visual_layouts:
                visual_objects = visual_layouts[visual_id].get("objects", {})
                line_styles = visual_objects.get("lineStyles", []) if isinstance(visual_objects, dict) else []
                legend = visual_objects.get("legend", []) if isinstance(visual_objects, dict) else []
                marker_value = (
                    pbir_literal(line_styles[0].get("properties", {}).get("showMarker"))
                    if line_styles
                    else None
                )
                legend_value = (
                    pbir_literal(legend[0].get("properties", {}).get("show"))
                    if legend
                    else None
                )
                if marker_value != "true" or legend_value != "true":
                    errors.append(f"Multi-series non-color marker/legend contract is not persisted for {visual_id}")
    if blueprint_ids != seen_visual_ids:
        errors.append("PBIR visuals and report-blueprint visual inventory differ")
    dq_status_visuals = [
        visual_id
        for visual_id, (page, _, _, query_refs) in visual_metadata.items()
        if page == "DataQuality" and "_Measures.Overall DQ Status" in query_refs
    ]
    if not dq_status_visuals:
        errors.append("Data Quality page must contain a visual bound to _Measures.Overall DQ Status")
    viewports = blueprint.get("responsiveAcceptance", {}).get("viewports", [])
    if viewports != [320, 390, 768, 1280, 1440]:
        errors.append("Responsive acceptance viewports are incomplete")
    responsive = blueprint.get("responsiveAcceptance", {})
    if responsive.get("phoneContentWidth") != 320 or responsive.get("minimumPhoneGap") < 8:
        errors.append("Responsive phone width/gap contract is incomplete")
    if responsive.get("minimumPhoneVisualHeight") < 100:
        errors.append("Responsive minimum phone visual height must be at least 100")
    if responsive.get("minimumTouchTarget") < 44 or responsive.get("horizontalOverflowAllowed") is not False:
        errors.append("Responsive touch-target or overflow contract is unsafe")
    if set(responsive.get("layoutEquivalents", {})) != {"320", "390", "768", "1280", "1440"}:
        errors.append("Responsive layout equivalents are incomplete")
    for visual_id, layout in visual_layouts.items():
        mobile_position = layout.get("mobile")
        if isinstance(mobile_position, dict) and (
            mobile_position.get("x") != 0 or mobile_position.get("width") != responsive.get("phoneContentWidth")
        ):
            errors.append(f"Mobile visual does not use the full authored phone width: {visual_id}")

    accessibility = blueprint.get("accessibilityContract", {})
    if (
        accessibility.get("normalTextContrastMinimum") != 4.5
        or accessibility.get("nonTextContrastMinimum") != 3.0
        or accessibility.get("altTextMaximumCharacters") != 250
        or accessibility.get("statusRequiresText") is not True
    ):
        errors.append("Accessibility threshold contract is incomplete")
    visual_system = blueprint.get("visualSystem", {})
    surface = hex_color(visual_system.get("surface"))
    for role in ("text", "mutedText", "primary", "positive", "warning", "negative"):
        color = hex_color(visual_system.get(role))
        if color is None or surface is None or contrast_ratio(color, surface) < 4.5:
            errors.append(f"Visual-system color {role} is below 4.5:1 against surface")
    for role in ("border", "focus"):
        color = hex_color(visual_system.get(role))
        if color is None or surface is None or contrast_ratio(color, surface) < 3.0:
            errors.append(f"Visual-system color {role} is below 3:1 against surface")


def validate_accessibility_evidence(root: Path, errors: list[str]) -> None:
    manifests = sorted(
        (root / "docs" / "powerbi" / "evidence" / "accessibility-responsive").glob("*/manifest.json")
    )
    if not manifests:
        errors.append("Accessibility/responsive evidence manifest is missing")
        return
    manifest_path = manifests[-1]
    manifest = load_json(manifest_path, errors)
    if not isinstance(manifest, dict):
        return
    if manifest.get("schemaVersion") != "1.0.0" or manifest.get("evidenceBoundary") != "runtime-not-claimed-without-capture":
        errors.append("Accessibility evidence schema or honesty boundary is invalid")
    entries = manifest.get("runtimeChecks")
    if not isinstance(entries, list):
        errors.append("Accessibility evidence runtimeChecks must be a list")
        return
    required = {
        (page, viewport, "default")
        for page in ("ExecutiveOverview", "SalesPerformance", "DataQuality")
        for viewport in (320, 390, 768, 1280, 1440)
    }
    actual: set[tuple[str, int, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("Accessibility evidence contains a non-object runtime check")
            continue
        key = (entry.get("page"), entry.get("viewport"), entry.get("state"))
        if key in actual:
            errors.append(f"Duplicate accessibility evidence row: {key}")
        actual.add(key)
        result = entry.get("result")
        if result not in {"PASS", "FAIL", "NOT_EXECUTED"}:
            errors.append(f"Invalid accessibility evidence result for {key}")
            continue
        if result == "PASS":
            screenshot = entry.get("screenshot")
            digest = entry.get("sha256")
            if not isinstance(screenshot, str) or not re.fullmatch(r"[0-9a-f]{64}", str(digest)):
                errors.append(f"Passing runtime evidence lacks screenshot/hash for {key}")
                continue
            screenshot_path = resolve_child(root, screenshot, errors, "Accessibility screenshot")
            if not screenshot_path.is_file():
                errors.append(f"Passing accessibility screenshot is missing for {key}")
            elif hashlib.sha256(screenshot_path.read_bytes()).hexdigest() != digest:
                errors.append(f"Accessibility screenshot hash differs for {key}")
        elif len(str(entry.get("note", ""))) < 20:
            errors.append(f"Non-passing runtime evidence needs an explanatory note for {key}")
    if actual != required:
        errors.append("Accessibility runtime evidence does not cover every page and viewport in the default state")


def validate_desktop_poc_evidence(root: Path, errors: list[str]) -> None:
    manifests = sorted((root / "docs" / "powerbi" / "evidence" / "desktop-poc").glob("*/manifest.json"))
    if not manifests:
        errors.append("Desktop PoC evidence manifest is missing")
        return
    manifest = load_json(manifests[-1], errors)
    if not isinstance(manifest, dict):
        return
    if manifest.get("schemaVersion") != "1.0.0" or manifest.get("evidenceBoundary") != "desktop-poc-not-production":
        errors.append("Desktop PoC evidence schema or honesty boundary is invalid")
    if not re.fullmatch(r"\d+\.\d+\.\d+\.\d+", str(manifest.get("powerBIDesktopVersion", ""))):
        errors.append("Desktop PoC evidence must record a Power BI Desktop version")
    if manifest.get("source", {}).get("classification") != "synthetic-loopback-acceptance":
        errors.append("Desktop PoC evidence must identify the synthetic loopback source boundary")
    entries = manifest.get("pages")
    if not isinstance(entries, list):
        errors.append("Desktop PoC evidence pages must be a list")
        return
    required_pages = {"ExecutiveOverview", "SalesPerformance", "DataQuality"}
    actual_pages: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("Desktop PoC evidence contains a non-object page")
            continue
        page = str(entry.get("page", ""))
        if page in actual_pages:
            errors.append(f"Duplicate Desktop PoC evidence page: {page}")
        actual_pages.add(page)
        if entry.get("result") != "PASS":
            errors.append(f"Desktop PoC page is not accepted: {page}")
            continue
        screenshot = entry.get("screenshot")
        digest = entry.get("sha256")
        if not isinstance(screenshot, str) or not re.fullmatch(r"[0-9a-f]{64}", str(digest)):
            errors.append(f"Desktop PoC evidence lacks screenshot/hash for {page}")
            continue
        screenshot_path = resolve_child(root, screenshot, errors, "Desktop PoC screenshot")
        if not screenshot_path.is_file():
            errors.append(f"Desktop PoC screenshot is missing for {page}")
        elif hashlib.sha256(screenshot_path.read_bytes()).hexdigest() != digest:
            errors.append(f"Desktop PoC screenshot hash differs for {page}")
    if actual_pages != required_pages:
        errors.append("Desktop PoC evidence does not cover all three report pages")


def validate_kpi_catalog(root: Path, inventory: ModelInventory, errors: list[str]) -> None:
    path = root / "docs" / "kpi" / "kpi-catalog.json"
    catalog = load_json(path, errors)
    if not isinstance(catalog, dict):
        return
    entries = catalog.get("kpis")
    if not isinstance(entries, list) or not entries:
        errors.append("KPI catalog must contain entries")
        return
    ids: set[str] = set()
    measures: set[str] = set()
    excluded_measures: set[str] = set()
    required_fields = {
        "id",
        "measure",
        "businessQuestion",
        "definition",
        "formula",
        "grain",
        "dateContext",
        "filtersAndExclusions",
        "sourceColumns",
        "format",
        "audience",
        "pageUsage",
        "limitations",
    }
    for entry in entries:
        if not isinstance(entry, dict):
            errors.append("KPI catalog entry is not an object")
            continue
        missing = sorted(field for field in required_fields if not entry.get(field))
        if missing:
            errors.append(f"KPI {entry.get('id', '<unknown>')} missing fields: {missing}")
        kpi_id = str(entry.get("id"))
        measure = str(entry.get("measure"))
        if kpi_id in ids:
            errors.append(f"Duplicate KPI ID: {kpi_id}")
        if measure in measures:
            errors.append(f"Duplicate KPI measure mapping: {measure}")
        ids.add(kpi_id)
        measures.add(measure)
        if measure not in inventory.all_measures:
            errors.append(f"KPI references unknown TMDL measure: {measure}")
        expected_format = inventory.measure_formats.get(measure)
        documented_format = entry.get("format")
        if measure in inventory.all_measures:
            normalized_expected = expected_format if expected_format is not None else "Text"
            if documented_format != normalized_expected:
                errors.append(
                    f"KPI format differs from TMDL for {measure}: "
                    f"catalog={documented_format!r}, TMDL={normalized_expected!r}"
                )
        for source_ref in entry.get("sourceColumns", []):
            table, separator, column = str(source_ref).rpartition(".")
            if not separator or table not in inventory.columns or column not in inventory.columns[table]:
                errors.append(f"KPI {kpi_id} references unknown source column: {source_ref}")

    exclusions = catalog.get("measureExclusions", [])
    if not isinstance(exclusions, list):
        errors.append("KPI catalog measureExclusions must be a list")
        exclusions = []
    for exclusion in exclusions:
        if not isinstance(exclusion, dict):
            errors.append("KPI measure exclusion is not an object")
            continue
        measure = exclusion.get("measure")
        rationale = exclusion.get("rationale")
        if not isinstance(measure, str) or not measure.strip():
            errors.append("KPI measure exclusion lacks a measure")
            continue
        if measure in excluded_measures:
            errors.append(f"Duplicate KPI measure exclusion: {measure}")
        excluded_measures.add(measure)
        if measure in measures:
            errors.append(f"Measure is both cataloged and excluded: {measure}")
        if measure not in inventory.all_measures:
            errors.append(f"KPI exclusion references unknown TMDL measure: {measure}")
        if not isinstance(rationale, str) or len(rationale.strip()) < 30:
            errors.append(f"KPI exclusion requires a meaningful rationale: {measure}")

    undocumented = inventory.all_measures - measures - excluded_measures
    if undocumented:
        errors.append(f"TMDL measures missing from KPI catalog: {sorted(undocumented)}")


def validate_files(root: Path, errors: list[str]) -> None:
    scoped_roots = [root / "powerbi", root / "docs" / "kpi", root / "docs" / "powerbi", root / "scripts" / "powerbi_validation"]
    try:
        tracked_result = subprocess.run(
            ["git", "ls-files"], cwd=root, check=True, capture_output=True, text=True
        )
        tracked_paths = {line.replace("\\", "/") for line in tracked_result.stdout.splitlines()}
    except (OSError, subprocess.CalledProcessError):
        tracked_paths = set()
    for scoped_root in scoped_roots:
        for path in scoped_root.rglob("*") if scoped_root.exists() else []:
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                relative = path.relative_to(root).as_posix()
                if relative in tracked_paths:
                    errors.append(f"Python cache must not be committed: {path}")
                continue
            if path.name in TRANSIENT_NAMES or ".pbi" in path.parts:
                errors.append(f"Transient Power BI state must not be committed: {path}")
            if len(str(path.resolve())) >= 260:
                errors.append(f"Path is unsafe for common Windows tooling (>=260 chars): {path}")
            try:
                raw = path.read_bytes()
                is_evidence_image = (
                    path.suffix.lower() in EVIDENCE_IMAGE_SUFFIXES
                    and path.is_relative_to(root / "docs" / "powerbi" / "evidence")
                )
                if is_evidence_image:
                    if not raw:
                        errors.append(f"Empty Power BI evidence image: {path}")
                    elif path.suffix.lower() == ".png" and not raw.startswith(b"\x89PNG\r\n\x1a\n"):
                        errors.append(f"Invalid PNG Power BI evidence image: {path}")
                    elif path.suffix.lower() in {".jpg", ".jpeg"} and not raw.startswith(b"\xff\xd8\xff"):
                        errors.append(f"Invalid JPEG Power BI evidence image: {path}")
                    continue
                is_performance_export = "performance" in path.parts and "raw" in path.parts
                if raw.startswith(b"\xef\xbb\xbf") and not is_performance_export:
                    errors.append(f"UTF-8 BOM is not allowed: {path}")
                encoding = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8"
                text = raw.decode(encoding)
            except UnicodeDecodeError:
                errors.append(f"Non-UTF-8 artifact: {path}")
                continue
            for label, pattern in SECRET_PATTERNS.items():
                if path.name not in SECRET_SCANNER_IMPLEMENTATIONS and pattern.search(text):
                    errors.append(f"Potential {label} in {path}")
            if path.is_relative_to(root / "powerbi"):
                if ABSOLUTE_PATH.search(text):
                    errors.append(f"Machine-specific absolute path in {path}")


def validate_git_scope(root: Path, errors: list[str]) -> None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain=v1", "-z"],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return
    records = result.stdout.split(b"\0")
    index = 0
    while index < len(records):
        record = records[index]
        index += 1
        if not record:
            continue
        status = record[:2]
        path = record[3:].decode("utf-8", errors="surrogateescape").replace("\\", "/")
        if b"R" in status or b"C" in status:
            index += 1  # Porcelain -z emits the second rename/copy path as a separate record.
        if path and not path.startswith(ALLOWED_PREFIXES):
            errors.append(f"Working-tree change is outside the owned slice: {path}")


def validate_service_release_assets(root: Path, errors: list[str]) -> None:
    contract_path = root / "powerbi" / "service" / "service-contract.example.json"
    runbook_path = root / "docs" / "powerbi" / "service-production-runbook.md"
    validator_path = root / "scripts" / "powerbi_validation" / "powerbi_service_contract.py"
    for path in (contract_path, runbook_path, validator_path):
        if not path.is_file():
            errors.append(f"Required Power BI Service release asset is missing: {path}")
    contract = load_json(contract_path, errors) if contract_path.is_file() else None
    if not isinstance(contract, dict):
        return
    if contract.get("schemaVersion") != 1:
        errors.append("Power BI Service example contract schemaVersion must be 1")
    if contract.get("target", {}).get("environment") != "Production":
        errors.append("Power BI Service example contract must explicitly target Production")
    expected_items = {
        "semanticModel": "c8c14294-542b-460d-aec3-f48f0fcd40fc",
        "report": "696c2727-6eb6-4422-86a9-dc951409c8d8",
    }
    artifacts = contract.get("artifacts", {})
    for item_type, logical_id in expected_items.items():
        item = artifacts.get(item_type, {}) if isinstance(artifacts, dict) else {}
        if item.get("expectedDisplayName") != "SQLDataWarehouse":
            errors.append(f"Power BI Service {item_type} expectedDisplayName must be SQLDataWarehouse")
        if item.get("expectedLogicalId") != logical_id:
            errors.append(f"Power BI Service {item_type} logical ID differs from .platform")
        if item.get("observedItemId") is not None:
            errors.append(f"Power BI Service example must not claim an observed {item_type} item ID")
    if contract.get("security", {}).get("approvedGroups"):
        errors.append("Power BI Service example must not contain approved production group IDs")
    if contract.get("target", {}).get("observedTenantId") is not None:
        errors.append("Power BI Service example must not claim an observed tenant ID")
    if contract.get("target", {}).get("workspace", {}).get("observedId") is not None:
        errors.append("Power BI Service example must not claim an observed workspace ID")
    if contract.get("artifacts", {}).get("provenance", {}).get("observedSourceCommit") is not None:
        errors.append("Power BI Service example must not claim a deployed source commit")
    if contract.get("approval", {}).get("releaseOwnerObjectId") is not None:
        errors.append("Power BI Service example must not contain a production release owner ID")


def validate_project(root: Path, check_git: bool = True) -> list[str]:
    root = root.resolve()
    errors: list[str] = []
    powerbi_root = root / "powerbi"
    if not powerbi_root.is_dir():
        return ["powerbi directory is missing"]

    validate_files(root, errors)
    for path in iter_json_like(powerbi_root):
        load_json(path, errors)

    pbip_files = sorted(powerbi_root.glob("*.pbip"))
    if len(pbip_files) != 1:
        errors.append(f"Expected exactly one .pbip file, found {len(pbip_files)}")
        return errors
    pbip = load_json(pbip_files[0], errors)
    if not isinstance(pbip, dict):
        return errors
    if pbip.get("version") != "1.0":
        errors.append("PBIP content version must be 1.0")
    artifacts = pbip.get("artifacts", [])
    if len(artifacts) != 1 or "report" not in artifacts[0]:
        errors.append("PBIP must reference exactly one report")
        return errors
    report_path = artifacts[0]["report"].get("path")
    report_dir = resolve_child(powerbi_root, str(report_path), errors, "PBIP report path")
    if not report_dir.is_dir():
        errors.append(f"PBIP report directory is missing: {report_dir}")
        return errors

    pbir = load_json(report_dir / "definition.pbir", errors)
    if not isinstance(pbir, dict):
        return errors
    if pbir.get("version") != "4.0":
        errors.append("definition.pbir content version must be 4.0")
    by_path = pbir.get("datasetReference", {}).get("byPath", {}).get("path")
    semantic_dir = (report_dir / str(by_path)).resolve()
    try:
        semantic_dir.relative_to(powerbi_root.resolve())
    except ValueError:
        errors.append(f"PBIR semantic-model path escapes the Power BI project: {by_path}")
    if not semantic_dir.is_dir():
        errors.append(f"PBIR semantic-model directory is missing: {semantic_dir}")
        return errors
    validate_platform_files(report_dir, semantic_dir, errors)
    pbism = load_json(semantic_dir / "definition.pbism", errors)
    if not isinstance(pbism, dict) or float(pbism.get("version", 0)) < 4.0:
        errors.append("definition.pbism version must be 4.0 or newer")

    definition = semantic_dir / "definition"
    required_tmdl = ["database.tmdl", "model.tmdl", "expressions.tmdl", "relationships.tmdl"]
    for name in required_tmdl:
        if not (definition / name).is_file():
            errors.append(f"Required TMDL file missing: {name}")
    inventory = parse_model(definition, errors)
    validate_relationships(definition, inventory, errors)
    validate_measure_expressions(definition, inventory, errors)
    validate_dq_status_contract(inventory, errors)

    model_text = (definition / "model.tmdl").read_text(encoding="utf-8")
    indented_refs = re.findall(r"^[ \t]+ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    if indented_refs:
        errors.append("TMDL ref table/ref role declarations must be root-level")
    root_refs = re.findall(r"^ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    if len(root_refs) != 11:
        errors.append(f"Expected eleven root-level TMDL table/role refs, found {len(root_refs)}")
    first_ref = re.search(r"^ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    annotation = re.search(r"^annotation __PBI_TimeIntelligenceEnabled", model_text, re.MULTILINE)
    if not annotation or (first_ref and annotation.start() > first_ref.start()):
        errors.append("Desktop-canonical model annotation must precede root-level refs")

    expression_text = (definition / "expressions.tmdl").read_text(encoding="utf-8")
    table_text = "\n".join(path.read_text(encoding="utf-8") for path in (definition / "tables").glob("*.tmdl"))
    for parameter in ("SqlServerName", "SqlDatabaseName", "EnvironmentName", "CommandTimeoutMinutes"):
        if f"expression {parameter} =" not in expression_text:
            errors.append(f"Required refresh parameter is missing: {parameter}")
    child_parameter_metadata = re.findall(
        r"(?m)^expression\s+([^\s=]+)\s*=.*\r?\n[ \t]+meta\s+\[[^\]\r\n]*"
        r"\bIsParameterQuery\s*=\s*true\b[^\]\r\n]*\]",
        expression_text,
    )
    if child_parameter_metadata:
        errors.append(
            "Power Query parameter metadata must be on the expression declaration line; "
            f"indented child meta is unsupported for: {sorted(child_parameter_metadata)}"
        )
    if "Sql.Database(SqlServerName, SqlDatabaseName" not in table_text:
        errors.append("Refresh parameters are not used by Sql.Database")
    core_source_text = "\n".join(
        (definition / "tables" / name).read_text(encoding="utf-8")
        for name in (
            "Sales.tmdl",
            "Customers.tmdl",
            "Products.tmdl",
            "Inventory Snapshots.tmdl",
            "Inventory Locations.tmdl",
        )
    )
    for required_gold_source in (
        "FROM gold.fact_sales",
        "FROM gold.dim_customers",
        "FROM gold.dim_products",
        "FROM gold.fact_inventory_snapshots",
        "FROM gold.dim_inventory_locations",
    ):
        if required_gold_source not in core_source_text:
            errors.append(f"Curated Gold source is missing: {required_gold_source}")
    if re.search(r"\bFROM\s+silver\.", core_source_text, re.IGNORECASE):
        errors.append("Core semantic tables must not bypass the curated Gold contract")

    dq_source_path = definition / "tables" / "Data Quality Checks.tmdl"
    dq_source_text = dq_source_path.read_text(encoding="utf-8")
    if "Value.NativeQuery" in dq_source_text:
        errors.append("Data Quality Checks must not require native-query approval")
    for dq_source in (
        'Source{[Schema = "gold", Item = "fact_sales"]}[Data]',
        'Source{[Schema = "gold", Item = "dim_customers"]}[Data]',
        'Source{[Schema = "gold", Item = "dim_products"]}[Data]',
    ):
        if dq_source not in dq_source_text:
            errors.append(f"Data Quality Checks curated source is missing: {dq_source}")
    for numeric_column in ("FailedRows", "EvaluatedRows"):
        type_contract = re.compile(
            rf"(?m)^\tcolumn {re.escape(numeric_column)}\r?$\n\t\tdataType: int64\r?$"
        )
        if not type_contract.search(dq_source_text):
            errors.append(f"Data Quality Checks {numeric_column} must use int64")
        if f'{{"{numeric_column}", Int64.Type}}' not in dq_source_text:
            errors.append(f"Data Quality Checks M result must type {numeric_column} as Int64.Type")
    for dq_contract_token in (
        "KnownProductNumbers",
        "GoldSourceOrphans",
        "ProductPreHistoryRows",
        'CheckKey = "gold_source_orphan_coverage"',
        'CheckKey = "product_pre_history_coverage"',
        'Scope = "Source limitation"',
    ):
        if dq_contract_token not in dq_source_text:
            errors.append(
                "Data Quality Checks must distinguish unresolved source references "
                "from known-product pre-history coverage"
            )
            break

    role_path = definition / "roles" / "CountrySalesViewer.tmdl"
    if not role_path.is_file():
        errors.append("CountrySalesViewer role is missing")
    else:
        role_text = role_path.read_text(encoding="utf-8")
        for required in (
            "USERPRINCIPALNAME()",
            "tablePermission 'Security User Country' =",
            "tablePermission Customers =",
            "Customers[country_code]",
            "tablePermission 'Inventory Locations' =",
            "'Inventory Locations'[country_code]",
            "Security User Country",
            "LOWER(TRIM(USERPRINCIPALNAME()))",
            "LEN(CurrentUser) > 0",
            "LEN(TRIM(COALESCE('Security User Country'[CountryCode], \"\"))) > 0",
            "UPPER(TRIM('Security User Country'[CountryCode]))",
            "[IsActive] = TRUE()",
            "[ValidFromUtc] <= CurrentUtc",
            "[ValidToUtc] > CurrentUtc",
        ):
            if required not in role_text:
                errors.append(f"CountrySalesViewer role lacks {required}")
    security_text = (definition / "tables" / "Security User Country.tmdl").read_text(encoding="utf-8")
    identities = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", security_text)
    if any(not identity.endswith(".invalid") for identity in identities):
        errors.append("Only reserved .invalid synthetic identities may be committed")
    validate_rls_acceptance_contract(root, security_text, errors)
    calculated_partition = re.compile(
        r"(?ms)^\tpartition\s+(.+?)\s*=\s*calculated\s*\r?\n"
        r"(.*?)(?=^\t(?:column|hierarchy|measure|partition)\s+|\Z)"
    )
    datatable_source_with_date = re.compile(
        r"(?mi)^[ \t]+source\s*=\s*DATATABLE\([^\r\n]*\bDATE\s*\("
    )
    for table_path in (definition / "tables").glob("*.tmdl"):
        table_source = table_path.read_text(encoding="utf-8")
        for partition in calculated_partition.finditer(table_source):
            if datatable_source_with_date.search(partition.group(2)):
                errors.append(
                    "Calculated DATATABLE partitions must use dt date literals instead of DATE(...): "
                    f"{table_path.name} / {partition.group(1).strip()}"
                )

    validate_report(report_dir, inventory, errors)
    validate_accessibility_evidence(root, errors)
    validate_desktop_poc_evidence(root, errors)
    validate_kpi_catalog(root, inventory, errors)
    validate_rls_documentation(root, errors)
    errors.extend(validate_performance_evidence(root))
    validate_service_release_assets(root, errors)

    validation_doc = (root / "docs" / "powerbi" / "validation.md").read_text(encoding="utf-8")
    architecture_doc = (root / "docs" / "powerbi" / "architecture.md").read_text(encoding="utf-8")
    if "Desktop refresh passed" not in validation_doc:
        errors.append("Dated Power BI Desktop refresh evidence is not documented")
    if "synthetic CRM, ERP, and Inventory data" not in architecture_doc:
        errors.append("Synthetic-data evidence boundary is not documented")
    prohibited_claims = ("deployed to production", "years of experience")
    lower_docs = (validation_doc + architecture_doc).lower()
    for claim in prohibited_claims:
        if claim in lower_docs:
            errors.append(f"Prohibited unsupported claim in documentation: {claim}")

    if check_git:
        validate_git_scope(root, errors)
    return sorted(set(errors))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    parser.add_argument("--no-git-scope", action="store_true", help="Skip working-tree scope validation")
    args = parser.parse_args()
    errors = validate_project(args.root, check_git=not args.no_git_scope)
    if errors:
        print(f"Power BI source validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Power BI source validation passed.")
    print("Validated: PBIP/PBIR structure, Fabric item metadata, TMDL inventory/references, KPI catalog, report blueprint, focus/mobile order, source-level accessibility/contrast contracts, Desktop PoC screenshot hashes, runtime and Service evidence honesty, RLS/refresh contracts, performance evidence, secrets, and owned-scope changes.")
    print("Not validated by this command: Power BI Desktop open/save, full TMDL/DAX/M parsing, refresh, rendered content semantics, RLS enforcement, runtime interactions, screen-reader output, High Contrast, internal touch hitboxes, or exact viewport behavior.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
