#!/usr/bin/env python3
"""Create a deterministic static inventory of this warehouse repository.

The scanner is intentionally conservative. It recognizes the T-SQL and SQLCMD
constructs used by this repository, but it does not claim to be a SQL Server
parser or a substitute for runtime catalog inspection.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0"
SQL_GLOBS = ("scripts/**/*.sql", "tests/**/*.sql")
EXPECTED_COUNTS = {
    "database": 1,
    "schema": 0,
    "table": 34,
    "procedure": 7,
    "view": 2,
    "csv_source": 7,
}

DEFINITION_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?"
    r"(?P<kind>DATABASE|SCHEMA|TABLE|PROCEDURE|PROC|VIEW)\s+"
    r"(?P<name>(?:\[?[A-Za-z_][\w$#@]*\]?\.){0,2}\[?[A-Za-z_][\w$#@]*\]?)",
    re.IGNORECASE,
)
REFERENCE_RE = re.compile(
    r"\b(?P<operation>INSERT\s+INTO|TRUNCATE\s+TABLE|BULK\s+INSERT|"
    r"ALTER\s+TABLE|DROP\s+TABLE|DROP\s+VIEW|FROM|JOIN|EXECUTE|EXEC)\s+"
    r"(?P<name>(?:\[?[A-Za-z_][\w$#@]*\]?\.){0,2}\[?[A-Za-z_][\w$#@]*\]?)",
    re.IGNORECASE,
)
SQLCMD_RE = re.compile(r"^\s*:r\s+(?P<path>.+?)\s*$", re.IGNORECASE | re.MULTILINE)
CSV_RE = re.compile(r"(?P<name>[A-Za-z0-9_\-]+\.csv)", re.IGNORECASE)
CTE_RE = re.compile(r"(?:\bWITH|,)\s*(?P<name>[A-Za-z_][\w$#@]*)\s+AS\s*\(", re.IGNORECASE)
DYNAMIC_BULK_RE = re.compile(
    r"\bBULK\s+INSERT\s+(?P<name>(?:\[?[A-Za-z_][\w$#@]*\]?\.){1,2}\[?[A-Za-z_][\w$#@]*\]?)",
    re.IGNORECASE,
)


def repo_path(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_identifier(value: str) -> str:
    parts = [part.strip("[]") for part in value.split(".")]
    if len(parts) == 3 and parts[0].lower() == "datawarehouse":
        parts = parts[1:]
    return ".".join(parts).lower()


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def mask_sql(text: str) -> str:
    """Mask comments and string contents while preserving length and newlines."""

    output = list(text)
    patterns = (
        re.compile(r"/\*.*?\*/", re.DOTALL),
        re.compile(r"--[^\r\n]*"),
        re.compile(r"N?'(?:''|[^'])*'", re.DOTALL),
    )
    for pattern in patterns:
        current = "".join(output)
        for match in pattern.finditer(current):
            for index in range(match.start(), match.end()):
                if output[index] not in "\r\n":
                    output[index] = " "
    return "".join(output)


def kind_name(raw: str) -> str:
    return "procedure" if raw.lower() == "proc" else raw.lower()


def iter_sql_files(root: Path) -> list[Path]:
    files: set[Path] = set()
    for pattern in SQL_GLOBS:
        files.update(path for path in root.glob(pattern) if path.is_file())
    return sorted(files, key=lambda item: repo_path(item, root).lower())


def scan_sql(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    objects: list[dict[str, Any]] = []
    references: list[dict[str, Any]] = []
    includes: list[dict[str, Any]] = []

    for path in iter_sql_files(root):
        relative = repo_path(path, root)
        text = path.read_text(encoding="utf-8-sig", errors="strict")
        masked = mask_sql(text)
        cte_names = {match.group("name").lower() for match in CTE_RE.finditer(masked)}

        for match in DEFINITION_RE.finditer(masked):
            objects.append(
                {
                    "id": normalize_identifier(match.group("name")),
                    "kind": kind_name(match.group("kind")),
                    "defined_at": f"{relative}:{line_number(text, match.start())}",
                }
            )

        for match in REFERENCE_RE.finditer(masked):
            name = normalize_identifier(match.group("name"))
            if name in {"select", "values", "openrowset"} or name in cte_names:
                continue
            references.append(
                {
                    "subject": relative,
                    "operation": " ".join(match.group("operation").lower().split()),
                    "object": name,
                    "evidence": f"{relative}:{line_number(text, match.start())}",
                    "dynamic": False,
                }
            )

        static_bulk = {
            item["object"]
            for item in references
            if item["subject"] == relative and item["operation"] == "bulk insert"
        }
        for match in DYNAMIC_BULK_RE.finditer(text):
            name = normalize_identifier(match.group("name"))
            if name in static_bulk:
                continue
            references.append(
                {
                    "subject": relative,
                    "operation": "bulk insert",
                    "object": name,
                    "evidence": f"{relative}:{line_number(text, match.start())}",
                    "dynamic": True,
                }
            )

        for match in SQLCMD_RE.finditer(text):
            raw = match.group("path").strip().replace("\\", "/")
            normalized = raw.removeprefix("/workspace/").removeprefix("./")
            includes.append(
                {
                    "subject": relative,
                    "target": normalized,
                    "evidence": f"{relative}:{line_number(text, match.start())}",
                }
            )

        for match in CSV_RE.finditer(text):
            references.append(
                {
                    "subject": relative,
                    "operation": "file reference",
                    "object": match.group("name"),
                    "evidence": f"{relative}:{line_number(text, match.start())}",
                    "dynamic": "@dataset_path" in text,
                }
            )

    objects.sort(key=lambda item: (item["kind"], item["id"], item["defined_at"]))
    references.sort(key=lambda item: (item["subject"], item["operation"], item["object"], item["evidence"]))
    includes.sort(key=lambda item: (item["subject"], item["target"], item["evidence"]))
    return objects, references, includes


def scan_csv(root: Path) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for path in sorted(root.glob("datasets/**/*.csv"), key=lambda item: repo_path(item, root).lower()):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise ValueError(f"CSV source is empty: {repo_path(path, root)}") from exc
            rows = sum(1 for _ in reader)
        sources.append(
            {
                "path": repo_path(path, root),
                "columns": header,
                "data_rows": rows,
                "sha256": sha256(path),
            }
        )
    return sources


def python_sql_files(root: Path) -> list[dict[str, Any]]:
    path = root / "scripts" / "orchestrate_pipeline.py"
    if not path.exists():
        return []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=repo_path(path, root))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "SQL_FILES" for target in node.targets
        ):
            value = ast.literal_eval(node.value)
            return [
                {"position": index + 1, "target": str(item).replace("\\", "/")}
                for index, item in enumerate(value)
            ]
    return []


def legacy_candidates(root: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    ignored = {".git"}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix().lower()):
        if not path.is_file() or any(part in ignored for part in path.parts):
            continue
        relative = repo_path(path, root)
        lowered = path.name.lower()
        signals: list[str] = []
        if "placeholder" in lowered:
            signals.append("placeholder filename")
        if " copy" in lowered:
            signals.append("copy-style filename; content equivalence not implied")
        if relative.startswith("logs/"):
            signals.append("generated-log path is versioned")
        if lowered.endswith(".drawio.pdf"):
            signals.append("export remains after editable Draw.io sources were removed")
        if signals:
            candidates.append(
                {
                    "path": relative,
                    "signals": signals,
                    "disposition": "review_required",
                    "deletion_authorized": False,
                }
            )
    return candidates


def build_inventory(root: Path) -> dict[str, Any]:
    objects, references, includes = scan_sql(root)
    sources = scan_csv(root)
    inputs = [
        {"path": repo_path(path, root), "sha256": sha256(path)}
        for path in iter_sql_files(root)
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "inputs": inputs,
        "objects": objects,
        "references": references,
        "sqlcmd_includes": includes,
        "python_sql_files": python_sql_files(root),
        "csv_sources": sources,
        "legacy_candidates": legacy_candidates(root),
        "limitations": [
            "Static pattern analysis does not execute T-SQL or inspect a live SQL Server catalog.",
            "Dynamic SQL is only partially visible; runtime object names and permissions require catalog inspection.",
            "Legacy signals are review candidates and never authorize deletion.",
        ],
    }


def count_inventory(inventory: dict[str, Any]) -> dict[str, int]:
    counts = {key: 0 for key in EXPECTED_COUNTS}
    for item in inventory["objects"]:
        if item["kind"] in counts:
            counts[item["kind"]] += 1
    counts["csv_source"] = len(inventory["csv_sources"])
    return counts


def check_inventory(inventory: dict[str, Any], root: Path) -> list[str]:
    failures: list[str] = []
    counts = count_inventory(inventory)
    for kind, expected in EXPECTED_COUNTS.items():
        if counts[kind] != expected:
            failures.append(f"Expected {expected} {kind} entries, found {counts[kind]}.")
    for include in inventory["sqlcmd_includes"]:
        target = root / include["target"]
        if not target.is_file():
            failures.append(f"Missing SQLCMD include {include['target']} ({include['evidence']}).")
    for source in inventory["csv_sources"]:
        if source["data_rows"] == 0:
            failures.append(f"CSV source has no data rows: {source['path']}.")
    return failures


def render_markdown(inventory: dict[str, Any]) -> str:
    counts = count_inventory(inventory)
    lines = [
        "# Static repository inventory",
        "",
        "> Generated deterministically; no runtime SQL Server inspection was performed.",
        "",
        "## Counts",
        "",
        "| Kind | Count |",
        "| --- | ---: |",
    ]
    for kind in sorted(counts):
        lines.append(f"| {kind} | {counts[kind]} |")
    lines.extend(["", "## SQL objects", "", "| Kind | Object | Definition |", "| --- | --- | --- |"])
    for item in inventory["objects"]:
        lines.append(f"| {item['kind']} | `{item['id']}` | `{item['defined_at']}` |")
    lines.extend(["", "## CSV sources", "", "| Path | Rows | Columns | SHA-256 |", "| --- | ---: | ---: | --- |"])
    for source in inventory["csv_sources"]:
        lines.append(
            f"| `{source['path']}` | {source['data_rows']} | {len(source['columns'])} | `{source['sha256']}` |"
        )
    lines.extend(["", "## Legacy review candidates", "", "| Path | Signal |", "| --- | --- |"])
    for item in inventory["legacy_candidates"]:
        lines.append(f"| `{item['path']}` | {'; '.join(item['signals'])} |")
    return "\n".join(lines) + "\n"


def serialize(inventory: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(inventory, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    return render_markdown(inventory)


def write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--output", type=Path, help="Write output atomically instead of stdout.")
    parser.add_argument("--check", action="store_true", help="Validate the current expected inventory contract.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    try:
        inventory = build_inventory(root)
    except (OSError, UnicodeError, ValueError, SyntaxError) as exc:
        print(f"scan failed: {exc}", file=sys.stderr)
        return 3

    failures = check_inventory(inventory, root) if args.check else []
    content = serialize(inventory, args.format)
    if args.output:
        try:
            write_atomic(args.output, content)
        except OSError as exc:
            print(f"write failed: {exc}", file=sys.stderr)
            return 3
    else:
        sys.stdout.write(content)

    for failure in failures:
        print(f"CHECK FAILED: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
