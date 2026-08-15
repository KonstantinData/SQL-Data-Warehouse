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

from validate_performance_evidence import (  # noqa: E402
    VisualSample,
    validate_performance_evidence,
    validate_visual_gate,
)


class PerformanceEvidenceValidatorTests(unittest.TestCase):
    def test_repository_evidence_passes(self) -> None:
        self.assertEqual([], validate_performance_evidence(REPOSITORY_ROOT))

    def _copy_evidence(self, destination: Path) -> None:
        source = REPOSITORY_ROOT / "powerbi" / "performance"
        target = destination / "powerbi" / "performance"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target)

    def test_capture_hash_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_evidence(root)
            path = root / "powerbi/performance/raw/after-data-quality-runs1-5.json"
            path.write_bytes(path.read_bytes() + b"\n")
            errors = validate_performance_evidence(root)
            self.assertTrue(any("SHA-256 differs" in error for error in errors), errors)

    def test_false_improvement_claim_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_evidence(root)
            path = root / "powerbi/performance/performance-evidence.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            sales_page = next(page for page in data["pages"] if page["page"] == "Sales Performance")
            visual = next(item for item in sales_page["visuals"] if item["title"] == "Sales operations summary")
            visual["claim"] = "improved"
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_performance_evidence(root)
            self.assertTrue(any("improvement claim is unsupported" in error for error in errors), errors)

    def test_row_count_contract_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._copy_evidence(root)
            path = root / "powerbi/performance/performance-evidence.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            executive_page = next(page for page in data["pages"] if page["page"] == "Executive Overview")
            visual = next(item for item in executive_page["visuals"] if item["title"] == "Country performance")
            visual["row_count"] = 5
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            errors = validate_performance_evidence(root)
            self.assertTrue(any("row-count contract differs" in error for error in errors), errors)

    def test_non_regression_bound_fails_above_before_maximum(self) -> None:
        before = VisualSample((10, 11, 12, 13, 20), (4,))
        after = VisualSample((18, 19, 21, 22, 23), (4,))
        errors = validate_visual_gate("Synthetic visual", before, after, "no_improvement_claim")
        self.assertTrue(any("non-regression bound failed" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
