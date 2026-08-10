#!/usr/bin/env python3
"""Validate the source-controlled Power BI reference without Power BI Desktop.

This intentionally conservative validator checks repository contracts and
cross-file references. It is not a complete PBIR schema, TMDL, DAX, or M parser.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ALLOWED_PREFIXES = ("powerbi/", "docs/kpi/", "docs/powerbi/", "scripts/powerbi_validation/")
TRANSIENT_NAMES = {"cache.abf", "localSettings.json", "unappliedChanges.json", "editorSettings.json"}
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
        if raw.startswith(b"\xef\xbb\xbf"):
            errors.append(f"UTF-8 BOM is not allowed: {path}")
        return json.loads(raw.decode("utf-8"))
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


@dataclass
class ModelInventory:
    columns: dict[str, set[str]] = field(default_factory=dict)
    measures: dict[str, set[str]] = field(default_factory=dict)

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
        measures = {unquote(match.group("name").strip()) for match in MEASURE_RE.finditer(text)}
        if len(columns) != len(list(COLUMN_RE.finditer(text))):
            errors.append(f"Duplicate column in table {table}")
        if len(measures) != len(list(MEASURE_RE.finditer(text))):
            errors.append(f"Duplicate measure in table {table}")
        inventory.columns[table] = columns
        inventory.measures[table] = measures

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


def validate_relationships(definition: Path, inventory: ModelInventory, errors: list[str]) -> None:
    path = definition / "relationships.tmdl"
    if not path.is_file():
        errors.append("relationships.tmdl is missing")
        return
    text = path.read_text(encoding="utf-8")
    endpoints = [match.group("ref").strip() for match in RELATION_ENDPOINT_RE.finditer(text)]
    if len(endpoints) != 10:
        errors.append(f"Expected five relationships (ten endpoints), found {len(endpoints)} endpoints")
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
        ("Sales.product_number", "Products.product_number", True),
        ("Sales.order_date", "Date.Date", True),
        ("Sales.ship_date", "Date.Date", False),
        ("Sales.due_date", "Date.Date", False),
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
    if isinstance(report_metadata, dict):
        imported_version = report_metadata.get("themeCollection", {}).get("baseTheme", {}).get("reportVersionAtImport")
        expected_keys = {"visual", "page", "report"}
        if not isinstance(imported_version, dict) or set(imported_version) != expected_keys:
            errors.append("reportVersionAtImport must contain visual, page, and report versions")
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

    seen_visual_ids: set[str] = set()
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
        page_tab_orders: set[int] = set()
        mobile_positions: list[tuple[float, float, str]] = []
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
            if not all(isinstance(position.get(key), (int, float)) for key in required):
                errors.append(f"Incomplete visual position in {visual_path}")
            else:
                if position["x"] < 0 or position["y"] < 0 or position["x"] + position["width"] > width or position["y"] + position["height"] > height:
                    errors.append(f"Visual exceeds desktop canvas: {visual_path}")
                tab_order = int(position["tabOrder"])
                if tab_order in page_tab_orders:
                    errors.append(f"Duplicate tabOrder {tab_order} on page {page_name}")
                page_tab_orders.add(tab_order)
            for query_ref in re.findall(r'"queryRef"\s*:\s*"([^"]+)"', json.dumps(data)):
                validate_visual_reference(query_ref, inventory, errors, visual_path)
            objects = data.get("visual", {}).get("visualContainerObjects", {})
            title_text = objects.get("title", [{}])[0].get("properties", {}).get("text") if objects.get("title") else None
            alt_text = objects.get("general", [{}])[0].get("properties", {}).get("altText") if objects.get("general") else None
            if not title_text or not alt_text:
                errors.append(f"PBIR title or alt text is missing: {visual_path}")
            mobile_path = visual_path.parent / "mobile.json"
            if mobile_path.is_file():
                mobile = load_json(mobile_path, errors)
                position = mobile.get("position", {}) if isinstance(mobile, dict) else {}
                required_mobile = ("x", "y", "width", "height", "tabOrder")
                if not all(isinstance(position.get(key), (int, float)) for key in required_mobile):
                    errors.append(f"Incomplete mobile position in {mobile_path}")
                else:
                    if position["x"] < 0 or position["y"] < 0 or position["x"] + position["width"] > 320:
                        errors.append(f"Mobile visual exceeds the 320px baseline: {mobile_path}")
                    mobile_positions.append((position["y"], position["y"] + position["height"], visual_id))
        mobile_positions.sort()
        for previous, current in zip(mobile_positions, mobile_positions[1:]):
            if current[0] < previous[1]:
                errors.append(f"Mobile visuals overlap on {page_name}: {previous[2]} and {current[2]}")

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
        for visual in page.get("visuals", []):
            visual_id = visual.get("id")
            if visual_id in blueprint_ids:
                errors.append(f"Duplicate blueprint visual ID: {visual_id}")
            blueprint_ids.add(visual_id)
            if not visual.get("title") or len(str(visual.get("altText", ""))) < 20:
                errors.append(f"Missing meaningful title/alt text for visual {visual_id}")
            visual_dir = pages_path.parent / str(page_name) / "visuals" / str(visual_id)
            if not (visual_dir / "visual.json").is_file():
                errors.append(f"Blueprint visual is missing from PBIR: {page_name}/{visual_id}")
            if visual.get("mobilePriority") is not None and not (visual_dir / "mobile.json").is_file():
                errors.append(f"Prioritized mobile visual lacks mobile.json: {page_name}/{visual_id}")
    if blueprint_ids != seen_visual_ids:
        errors.append("PBIR visuals and report-blueprint visual inventory differ")
    viewports = blueprint.get("responsiveAcceptance", {}).get("viewports", [])
    if viewports != [320, 390, 768, 1280, 1440]:
        errors.append("Responsive acceptance viewports are incomplete")


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
        for source_ref in entry.get("sourceColumns", []):
            table, separator, column = str(source_ref).rpartition(".")
            if not separator or table not in inventory.columns or column not in inventory.columns[table]:
                errors.append(f"KPI {kpi_id} references unknown source column: {source_ref}")


def validate_files(root: Path, errors: list[str]) -> None:
    scoped_roots = [root / "powerbi", root / "docs" / "kpi", root / "docs" / "powerbi", root / "scripts" / "powerbi_validation"]
    for scoped_root in scoped_roots:
        for path in scoped_root.rglob("*") if scoped_root.exists() else []:
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                errors.append(f"Python cache must not be committed: {path}")
                continue
            if path.name in TRANSIENT_NAMES or ".pbi" in path.parts:
                errors.append(f"Transient Power BI state must not be committed: {path}")
            if len(str(path.resolve())) >= 260:
                errors.append(f"Path is unsafe for common Windows tooling (>=260 chars): {path}")
            try:
                raw = path.read_bytes()
                if raw.startswith(b"\xef\xbb\xbf"):
                    errors.append(f"UTF-8 BOM is not allowed: {path}")
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                errors.append(f"Non-UTF-8 artifact: {path}")
                continue
            for label, pattern in SECRET_PATTERNS.items():
                if path.name != "validate_powerbi_project.py" and pattern.search(text):
                    errors.append(f"Potential {label} in {path}")
            if path.is_relative_to(root / "powerbi"):
                if ABSOLUTE_PATH.search(text):
                    errors.append(f"Machine-specific absolute path in {path}")


def validate_git_scope(root: Path, errors: list[str]) -> None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return
    for line in result.stdout.splitlines():
        path = line[3:].replace("\\", "/")
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        if path and not path.startswith(ALLOWED_PREFIXES):
            errors.append(f"Working-tree change is outside the owned slice: {path}")


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

    model_text = (definition / "model.tmdl").read_text(encoding="utf-8")
    indented_refs = re.findall(r"^[ \t]+ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    if indented_refs:
        errors.append("TMDL ref table/ref role declarations must be root-level")
    root_refs = re.findall(r"^ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    if len(root_refs) != 9:
        errors.append(f"Expected nine root-level TMDL table/role refs, found {len(root_refs)}")
    first_ref = re.search(r"^ref\s+(?:table|role)\s+", model_text, re.MULTILINE)
    annotation = re.search(r"^\tannotation __PBI_TimeIntelligenceEnabled", model_text, re.MULTILINE)
    if not annotation or (first_ref and annotation.start() > first_ref.start()):
        errors.append("Model annotation must remain inside the model block before root-level refs")

    expression_text = (definition / "expressions.tmdl").read_text(encoding="utf-8")
    table_text = "\n".join(path.read_text(encoding="utf-8") for path in (definition / "tables").glob("*.tmdl"))
    for parameter in ("SqlServerName", "SqlDatabaseName", "EnvironmentName", "CommandTimeoutMinutes"):
        if f"expression {parameter} =" not in expression_text:
            errors.append(f"Required refresh parameter is missing: {parameter}")
    if "Sql.Database(SqlServerName, SqlDatabaseName" not in table_text:
        errors.append("Refresh parameters are not used by Sql.Database")
    if "TRY_CONVERT(date" not in table_text:
        errors.append("Sales safety projection must use TRY_CONVERT for dates")
    if "ROW_NUMBER() OVER (PARTITION BY product_number" not in table_text:
        errors.append("Products safety projection must select one row per product_number")

    role_path = definition / "roles" / "CountrySalesViewer.tmdl"
    if not role_path.is_file():
        errors.append("CountrySalesViewer role is missing")
    else:
        role_text = role_path.read_text(encoding="utf-8")
        for required in ("USERPRINCIPALNAME()", "Customers[country_code]", "Security User Country"):
            if required not in role_text:
                errors.append(f"CountrySalesViewer role lacks {required}")
    security_text = (definition / "tables" / "Security User Country.tmdl").read_text(encoding="utf-8")
    identities = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", security_text)
    if any(not identity.endswith(".invalid") for identity in identities):
        errors.append("Only reserved .invalid synthetic identities may be committed")

    validate_report(report_dir, inventory, errors)
    validate_kpi_catalog(root, inventory, errors)

    validation_doc = (root / "docs" / "powerbi" / "validation.md").read_text(encoding="utf-8")
    architecture_doc = (root / "docs" / "powerbi" / "architecture.md").read_text(encoding="utf-8")
    if "Power BI Desktop was not available" not in validation_doc:
        errors.append("Desktop-unavailable limitation is not documented")
    if "synthetic CRM and ERP sample data" not in architecture_doc:
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
    print("Validated: PBIP/PBIR structure, TMDL inventory/references, KPI catalog, report blueprint, layouts, RLS/refresh contracts, secrets, and owned-scope changes.")
    print("Not validated: Power BI Desktop open/save, full TMDL/DAX/M parsing, refresh, rendering, RLS enforcement, interactions, accessibility, or screenshots.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
