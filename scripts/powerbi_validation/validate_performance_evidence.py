#!/usr/bin/env python3
"""Validate reproducible Power BI Performance Analyzer evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class VisualSample:
    durations_ms: tuple[int, ...]
    row_counts: tuple[int, ...]

    @property
    def upper_median_ms(self) -> int:
        ordered = sorted(self.durations_ms)
        return ordered[len(ordered) // 2]


@dataclass(frozen=True)
class Capture:
    sha256: str
    refresh_count: int
    visuals: dict[str, VisualSample]


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _duration_ms(event: dict[str, Any]) -> int:
    duration = (_parse_utc(event["end"]) - _parse_utc(event["start"])).total_seconds() * 1000
    return int(round(duration))


def parse_capture(path: Path) -> Capture:
    raw = path.read_bytes()
    # Power BI Desktop exports JSON with a UTF-8 BOM. Preserve the vendor file
    # byte-for-byte so its hash remains useful evidence, and decode it explicitly.
    payload = json.loads(raw.decode("utf-8-sig"))
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError(f"Performance capture has no event list: {path}")

    by_id = {event["id"]: event for event in events if isinstance(event, dict) and event.get("id")}
    grouped: dict[str, dict[str, list[int]]] = {}
    for event in events:
        if not isinstance(event, dict) or event.get("name") != "Execute DAX Query":
            continue
        ancestor = event
        for _ in range(20):
            parent_id = ancestor.get("parentId")
            if not parent_id or parent_id not in by_id:
                break
            ancestor = by_id[parent_id]
            if ancestor.get("name") == "Visual Container Lifecycle":
                break
        if ancestor.get("name") != "Visual Container Lifecycle":
            raise ValueError(f"DAX event has no visual lifecycle ancestor in {path}: {event.get('id')}")
        metrics = ancestor.get("metrics") or {}
        title = metrics.get("visualTitle")
        row_count = (event.get("metrics") or {}).get("RowCount")
        if not isinstance(title, str) or not title:
            raise ValueError(f"Visual title is missing in {path}: {ancestor.get('id')}")
        if not isinstance(row_count, int):
            raise ValueError(f"DAX row count is missing for {title} in {path}")
        bucket = grouped.setdefault(title, {"durations": [], "rows": []})
        bucket["durations"].append(_duration_ms(event))
        bucket["rows"].append(row_count)

    refresh_count = sum(
        1
        for event in events
        if isinstance(event, dict)
        and event.get("name") == "User Action"
        and (event.get("metrics") or {}).get("sourceLabel") == "UserAction_Refresh"
    )
    visuals = {
        title: VisualSample(
            durations_ms=tuple(sorted(values["durations"])),
            row_counts=tuple(sorted(set(values["rows"]))),
        )
        for title, values in grouped.items()
    }
    return Capture(hashlib.sha256(raw).hexdigest(), refresh_count, visuals)


def validate_visual_gate(
    title: str,
    before: VisualSample,
    after: VisualSample,
    claim: str,
) -> list[str]:
    errors: list[str] = []
    if before.row_counts != after.row_counts:
        errors.append(f"Performance result row-count drift for {title}: {before.row_counts} -> {after.row_counts}")
    if after.upper_median_ms > max(before.durations_ms):
        errors.append(
            f"Performance non-regression bound failed for {title}: after median "
            f"{after.upper_median_ms} ms > before maximum {max(before.durations_ms)} ms"
        )
    if claim == "improved" and after.upper_median_ms >= before.upper_median_ms:
        errors.append(
            f"Performance improvement claim is unsupported for {title}: after median "
            f"{after.upper_median_ms} ms >= before median {before.upper_median_ms} ms"
        )
    if claim not in {"improved", "no_improvement_claim"}:
        errors.append(f"Unsupported performance claim state for {title}: {claim}")
    return errors


def _load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_performance_evidence(root: Path) -> list[str]:
    errors: list[str] = []
    manifest_path = root / "powerbi" / "performance" / "performance-evidence.json"
    if not manifest_path.is_file():
        return [f"Power BI performance evidence manifest is missing: {manifest_path}"]
    try:
        manifest = _load_manifest(manifest_path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"Invalid Power BI performance evidence manifest: {exc}"]

    if manifest.get("schema_version") != 1:
        errors.append("Power BI performance evidence schema_version must be 1")
    if manifest.get("metric") != "Execute DAX Query duration_ms":
        errors.append("Power BI performance evidence must use Execute DAX Query duration_ms")
    pages = manifest.get("pages")
    if not isinstance(pages, list) or len(pages) != 3:
        return errors + ["Power BI performance evidence must contain exactly three pages"]

    raw_dir = manifest_path.parent / "raw"
    for page in pages:
        page_name = page.get("page", "<unknown>")
        captures: dict[str, Capture] = {}
        for phase in ("before", "after"):
            spec = page.get(phase) or {}
            filename = spec.get("file")
            if not isinstance(filename, str) or Path(filename).name != filename:
                errors.append(f"Invalid {phase} capture filename for {page_name}")
                continue
            capture_path = raw_dir / filename
            if not capture_path.is_file():
                errors.append(f"Missing {phase} performance capture for {page_name}: {filename}")
                continue
            try:
                capture = parse_capture(capture_path)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                errors.append(f"Invalid {phase} performance capture for {page_name}: {exc}")
                continue
            captures[phase] = capture
            if capture.sha256 != str(spec.get("sha256", "")).lower():
                errors.append(f"Performance capture SHA-256 differs for {filename}")
            if capture.refresh_count != 5:
                errors.append(f"Performance capture must contain five refreshes for {page_name}/{phase}")

        if set(captures) != {"before", "after"}:
            continue
        visual_specs = page.get("visuals")
        if not isinstance(visual_specs, list) or len(visual_specs) != 5:
            errors.append(f"Performance evidence must cover five visuals for {page_name}")
            continue
        expected_titles = {spec.get("title") for spec in visual_specs}
        for phase, capture in captures.items():
            actual_titles = set(capture.visuals)
            if actual_titles != expected_titles:
                errors.append(
                    f"Performance visual coverage differs for {page_name}/{phase}: "
                    f"expected {sorted(expected_titles)}, got {sorted(actual_titles)}"
                )

        for spec in visual_specs:
            title = spec.get("title")
            if not isinstance(title, str) or title not in captures["before"].visuals or title not in captures["after"].visuals:
                continue
            before = captures["before"].visuals[title]
            after = captures["after"].visuals[title]
            expected_rows = (spec.get("row_count"),)
            if before.row_counts != expected_rows or after.row_counts != expected_rows:
                errors.append(
                    f"Performance evidence row-count contract differs for {title}: "
                    f"expected {expected_rows}, got {before.row_counts}/{after.row_counts}"
                )
            minimum_queries = int(spec.get("minimum_query_count", 5))
            if len(before.durations_ms) < minimum_queries or len(after.durations_ms) < minimum_queries:
                errors.append(f"Insufficient DAX samples for {title}: {len(before.durations_ms)}/{len(after.durations_ms)}")
            if list(before.durations_ms) != spec.get("before_ms"):
                errors.append(f"Recorded before timings differ for {title}")
            if list(after.durations_ms) != spec.get("after_ms"):
                errors.append(f"Recorded after timings differ for {title}")
            errors.extend(validate_visual_gate(title, before, after, str(spec.get("claim", ""))))

    return sorted(set(errors))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    args = parser.parse_args()
    errors = validate_performance_evidence(args.root)
    if errors:
        print(f"Power BI performance evidence validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Power BI performance evidence validation passed.")
    print("Validated: raw hashes, five refreshes per page, visual coverage, DAX timings, row counts, claims, and non-regression bounds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
