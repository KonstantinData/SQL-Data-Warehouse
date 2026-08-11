from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from validate_powerbi_project import validate_git_scope, validate_project  # noqa: E402


class PowerBIProjectValidatorTests(unittest.TestCase):
    def test_repository_project_passes(self) -> None:
        self.assertEqual([], validate_project(REPOSITORY_ROOT, check_git=False))

    def _copy_slice(self, destination: Path) -> None:
        for relative in ("powerbi", "docs/kpi", "docs/powerbi", "scripts/powerbi_validation"):
            source = REPOSITORY_ROOT / relative
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source, target)

    def test_broken_semantic_model_path_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.Report/definition.pbir"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["datasetReference"]["byPath"]["path"] = "../Missing.SemanticModel"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("semantic-model directory is missing" in error for error in errors), errors)

    def test_parameter_meta_on_indented_child_line_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.SemanticModel/definition/expressions.tmdl"
            text = path.read_text(encoding="utf-8").replace(
                'expression SqlServerName = "localhost" meta ',
                'expression SqlServerName = "localhost"\n\tmeta ',
                1,
            )
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(
                any(
                    "parameter metadata must be on the expression declaration line" in error
                    and "SqlServerName" in error
                    for error in errors
                ),
                errors,
            )

    def test_parameter_meta_on_expression_line_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            errors = validate_project(root, check_git=False)
            self.assertFalse(
                any("parameter metadata must be on the expression declaration line" in error for error in errors),
                errors,
            )

    def test_date_function_in_calculated_datatable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.SemanticModel/definition/tables/Security User Country.tmdl"
            text = path.read_text(encoding="utf-8").replace('dt"2020-01-01"', "DATE(2020, 1, 1)", 1)
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(
                any(
                    "Calculated DATATABLE partitions must use dt date literals instead of DATE(...)" in error
                    and "Security User Country.tmdl" in error
                    for error in errors
                ),
                errors,
            )

    def test_dt_literal_in_calculated_datatable_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            errors = validate_project(root, check_git=False)
            self.assertFalse(
                any("Calculated DATATABLE partitions must use dt date literals" in error for error in errors),
                errors,
            )

    def test_duplicate_kpi_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "docs/kpi/kpi-catalog.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["kpis"][1]["id"] = data["kpis"][0]["id"]
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("Duplicate KPI ID" in error for error in errors), errors)

    def test_uncataloged_tmdl_measure_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "docs/kpi/kpi-catalog.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["kpis"] = [entry for entry in data["kpis"] if entry["measure"] != "Total Sales"]
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("TMDL measures missing from KPI catalog" in error for error in errors), errors)

    def test_kpi_format_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "docs/kpi/kpi-catalog.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            next(entry for entry in data["kpis"] if entry["measure"] == "Total Sales")["format"] = "0"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("KPI format differs from TMDL for Total Sales" in error for error in errors), errors)

    def test_blueprint_title_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/report-blueprint.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["pages"][0]["visuals"][0]["title"] = "Drifted title"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("Blueprint/PBIR title mismatch" in error for error in errors), errors)

    def test_missing_overall_dq_status_visual_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            visual_paths = sorted((root / "powerbi/SQLDataWarehouse.Report/definition/pages/DataQuality/visuals").glob("*/visual.json"))
            for path in visual_paths:
                text = path.read_text(encoding="utf-8")
                if "_Measures.Overall DQ Status" in text:
                    path.write_text(text.replace("_Measures.Overall DQ Status", "_Measures.DQ Pass Rate"), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("visual bound to _Measures.Overall DQ Status" in error for error in errors), errors)

    def test_incomplete_rls_validation_matrix_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "docs/powerbi/validation.md"
            text = path.read_text(encoding="utf-8")
            text = "\n".join(line for line in text.splitlines() if not line.startswith("| Expired |")) + "\n"
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("RLS validation matrix identity cases differ" in error for error in errors), errors)

    def test_data_through_scope_misclassification_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "docs/powerbi/data-quality-reporting.md"
            text = path.read_text(encoding="utf-8").replace(
                "| Data-through date | Protected selected Sales scope through the active Customers-to-Sales relationship |",
                "| Data-through date | Global refresh scope |",
            )
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("scope matrix is missing or incorrect for data-through date" in error for error in errors), errors)

    def test_transient_power_bi_state_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            transient = root / "powerbi/SQLDataWarehouse.Report/.pbi/localSettings.json"
            transient.parent.mkdir(parents=True)
            transient.write_text("{}", encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("Transient Power BI state" in error for error in errors), errors)

    def test_invalid_report_theme_version_contract_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.Report/definition/report.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["themeCollection"]["baseTheme"]["reportVersionAtImport"] = "5.55"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("reportVersionAtImport" in error for error in errors), errors)

    def test_unknown_measure_dependency_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.SemanticModel/definition/tables/_Measures.tmdl"
            text = path.read_text(encoding="utf-8")
            text = text.replace("[Total Sales] - [Estimated COGS]", "[Missing Measure] - [Estimated COGS]")
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("unknown measure [Missing Measure]" in error for error in errors), errors)

    def test_relationship_topology_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.SemanticModel/definition/relationships.tmdl"
            text = path.read_text(encoding="utf-8").replace("Sales.customer_id", "Sales.product_number", 1)
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("Relationship topology differs" in error for error in errors), errors)

    def test_missing_inventory_rls_permission_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.SemanticModel/definition/roles/CountrySalesViewer.tmdl"
            text = path.read_text(encoding="utf-8")
            text = text.split("\n\ttablePermission 'Inventory Locations' =", 1)[0] + "\n"
            path.write_text(text, encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("Inventory Locations" in error for error in errors), errors)

    def test_missing_embedded_alt_text_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_slice(root)
            path = root / "powerbi/SQLDataWarehouse.Report/definition/pages/ExecutiveOverview/visuals/11111111111111111111/visual.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            del data["visual"]["visualContainerObjects"]["general"]
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_project(root, check_git=False)
            self.assertTrue(any("title or alt text is missing" in error for error in errors), errors)

    def test_git_scope_handles_space_in_unquoted_porcelain_z_path(self) -> None:
        errors: list[str] = []
        validate_git_scope(REPOSITORY_ROOT, errors)
        self.assertFalse(
            any("Data Quality Checks.tmdl" in error for error in errors),
            errors,
        )


if __name__ == "__main__":
    unittest.main()
