#!/usr/bin/env python3
"""Validate the durable documentation contract for this repository."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from repository_analysis import build_inventory


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


def validate(render_mermaid: bool = False) -> list[str]:
    failures: list[str] = []
    for relative in REQUIRED_FILES:
        if not (ROOT / relative).is_file():
            failures.append(f"Missing required artifact: {relative}")

    if failures:
        return failures

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

    for relative in REQUIRED_FILES:
        path = ROOT / relative
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

    if render_mermaid:
        renderer = shutil.which("mmdc")
        if renderer is None:
            print("Mermaid renderer unavailable; structural checks only.")
        else:
            with tempfile.TemporaryDirectory(prefix="sql-dw-mermaid-") as temporary:
                temp_root = Path(temporary)
                block_number = 0
                for relative in REQUIRED_FILES:
                    text = (ROOT / relative).read_text(encoding="utf-8")
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
