#!/usr/bin/env python3
"""Inspect and validate a fail-closed Power BI Service release contract.

The inspector performs read-only Power BI REST calls. Authentication is accepted
only through the POWERBI_ACCESS_TOKEN process environment variable and is never
written to output. The validator treats missing evidence as UNKNOWN and any
contradictory evidence as FAIL; both states block a production release.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


API_ROOT = "https://api.powerbi.com/v1.0/myorg"
REQUIRED_RLS_CASES = {"Allowed", "Multiple", "Inactive", "Expired", "Unknown", "Blank"}
REQUIRED_GLOBAL_SURFACES = {"Products", "Date", "Data Quality Checks", "Refresh Metadata"}
LOOPBACK_NAMES = {"127.0.0.1", "localhost", "::1", "."}
SECRET_KEY_PATTERN = re.compile(
    r"(?i)(password|pwd|client[_-]?secret|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"private[_-]?key|credential[_-]?value|authorization|cookie|session[_-]?(?:id|token)|"
    r"connection[_-]?string|jwt)"
)
SECRET_VALUE_PATTERNS = (
    re.compile(r"(?i)^bearer\s+"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"^[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}$"),
    re.compile(
        r"(?i)(?:^|[?&#;\s])(?:access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret|"
        r"api[_-]?key|apikey|authorization|sig|signature|code|token|key|sv|se|sp|sr|spr|st|sip|si)="
    ),
)
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+")
ALLOWED_DAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
ALLOWED_RLS_GROUP_TYPES = {"SecurityGroup", "MailEnabledGroup", "DistributionGroup"}
ALLOWED_ENTITLEMENT_SOURCE_KINDS = {"WarehouseTable"}


@dataclass(frozen=True)
class Finding:
    status: str
    path: str
    message: str


@dataclass
class Evaluation:
    findings: list[Finding] = field(default_factory=list)

    def fail(self, path: str, message: str) -> None:
        self.findings.append(Finding("FAIL", path, message))

    def unknown(self, path: str, message: str) -> None:
        self.findings.append(Finding("UNKNOWN", path, message))

    @property
    def status(self) -> str:
        if any(item.status == "FAIL" for item in self.findings):
            return "FAIL"
        if self.findings:
            return "UNKNOWN"
        return "PASS"


class ServiceInspectionError(RuntimeError):
    """A redacted error raised for read-only Power BI inspection failures."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read JSON contract {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("The service contract root must be a JSON object")
    return data


def get_path(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def set_path(data: dict[str, Any], path: str, value: Any) -> None:
    segments = path.split(".")
    current: dict[str, Any] = data
    for segment in segments[:-1]:
        child = current.get(segment)
        if not isinstance(child, dict):
            child = {}
            current[segment] = child
        current = child
    current[segments[-1]] = value


def is_uuid(value: Any) -> bool:
    try:
        return uuid.UUID(str(value)).int != 0
    except (ValueError, AttributeError, TypeError):
        return False


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def parse_decimal(value: Any) -> Decimal | None:
    """Parse exact contract totals without accepting bools, floats, NaN, or infinity."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = Decimal(value)
    elif isinstance(value, str) and DECIMAL_RE.fullmatch(value.strip()):
        try:
            parsed = Decimal(value.strip())
        except InvalidOperation:
            return None
    else:
        return None
    return parsed if parsed.is_finite() else None


def require_value(evaluation: Evaluation, data: dict[str, Any], path: str, message: str) -> Any:
    value = get_path(data, path)
    if value is None or value == "" or value == []:
        evaluation.unknown(path, message)
    return value


def require_true(evaluation: Evaluation, data: dict[str, Any], path: str, message: str) -> None:
    value = get_path(data, path)
    if value is None:
        evaluation.unknown(path, message)
    elif value is not True:
        evaluation.fail(path, message)


def require_equal(
    evaluation: Evaluation,
    data: dict[str, Any],
    expected_path: str,
    observed_path: str,
    message: str,
) -> None:
    expected = get_path(data, expected_path)
    observed = get_path(data, observed_path)
    if expected is None or expected == "":
        evaluation.unknown(expected_path, f"Expected value is not authorized: {message}")
    if observed is None or observed == "":
        evaluation.unknown(observed_path, f"Observed value is not verified: {message}")
    if expected not in (None, "") and observed not in (None, "") and expected != observed:
        evaluation.fail(observed_path, message)


def detect_secret_material(value: Any, path: str, evaluation: Evaluation) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else key
            if SECRET_KEY_PATTERN.search(key):
                evaluation.fail(child_path, "Secret-bearing fields are prohibited in service contracts")
            detect_secret_material(child, child_path, evaluation)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            detect_secret_material(child, f"{path}[{index}]", evaluation)
    elif isinstance(value, str) and any(pattern.search(value) for pattern in SECRET_VALUE_PATTERNS):
        evaluation.fail(path, "Secret-like values are prohibited in service contracts")


def validate_target(data: dict[str, Any], evaluation: Evaluation) -> None:
    if data.get("schemaVersion") != 1:
        evaluation.fail("schemaVersion", "Only Power BI service contract schemaVersion 1 is supported")
    if get_path(data, "target.environment") != "Production":
        evaluation.fail("target.environment", "The release contract must target Production explicitly")

    for path, message in (
        ("target.expectedTenantId", "Authorized tenant ID is missing"),
        ("target.observedTenantId", "Token tenant ID is not verified"),
        ("target.workspace.expectedId", "Authorized workspace ID is missing"),
        ("target.workspace.observedId", "Observed workspace ID is not verified"),
    ):
        value = require_value(evaluation, data, path, message)
        if value is not None and not is_uuid(value):
            evaluation.fail(path, f"{message}; expected a non-nil UUID")
    require_equal(
        evaluation,
        data,
        "target.expectedTenantId",
        "target.observedTenantId",
        "Token tenant differs from the authorized tenant",
    )
    require_equal(
        evaluation,
        data,
        "target.workspace.expectedId",
        "target.workspace.observedId",
        "Observed workspace ID differs from the authorized workspace",
    )
    require_equal(
        evaluation,
        data,
        "target.workspace.expectedName",
        "target.workspace.observedName",
        "Observed workspace name differs from the authorized workspace",
    )

    personal = get_path(data, "target.workspace.isPersonal")
    if personal is None:
        evaluation.unknown("target.workspace.isPersonal", "Personal workspace status is not verified")
    elif personal is not False:
        evaluation.fail("target.workspace.isPersonal", "Production release to My workspace is prohibited")

    allowed_capacity_modes = {"Shared", "Fabric", "Premium", "PPU"}
    for path in ("target.workspace.expectedCapacityMode", "target.workspace.observedCapacityMode"):
        mode = require_value(evaluation, data, path, "Workspace capacity mode is not verified")
        if mode is not None and mode not in allowed_capacity_modes:
            evaluation.fail(path, "Unsupported workspace capacity mode")
    require_equal(
        evaluation,
        data,
        "target.workspace.expectedCapacityMode",
        "target.workspace.observedCapacityMode",
        "Observed workspace capacity mode differs from the authorized mode",
    )
    expected_mode = get_path(data, "target.workspace.expectedCapacityMode")
    expected_capacity_id = get_path(data, "target.workspace.expectedCapacityId")
    observed_capacity_id = get_path(data, "target.workspace.observedCapacityId")
    if expected_mode in {"Fabric", "Premium"}:
        for path, value in (
            ("target.workspace.expectedCapacityId", expected_capacity_id),
            ("target.workspace.observedCapacityId", observed_capacity_id),
        ):
            if not is_uuid(value):
                evaluation.unknown(path, "Dedicated capacity ID is not verified")
        if expected_capacity_id and observed_capacity_id and expected_capacity_id != observed_capacity_id:
            evaluation.fail("target.workspace.observedCapacityId", "Observed capacity ID differs from authorized capacity")
    elif expected_mode in {"Shared", "PPU"} and observed_capacity_id not in (None, ""):
        evaluation.fail("target.workspace.observedCapacityId", "Unexpected dedicated capacity ID is present")


def validate_artifacts(data: dict[str, Any], evaluation: Evaluation) -> None:
    semantic = "artifacts.semanticModel"
    report = "artifacts.report"
    required_logical_ids = {
        semantic: "c8c14294-542b-460d-aec3-f48f0fcd40fc",
        report: "696c2727-6eb6-4422-86a9-dc951409c8d8",
    }
    for prefix in (semantic, report):
        expected_name = get_path(data, f"{prefix}.expectedDisplayName")
        if expected_name != "SQLDataWarehouse":
            evaluation.fail(f"{prefix}.expectedDisplayName", "Expected artifact name must be SQLDataWarehouse")
        if not is_uuid(get_path(data, f"{prefix}.expectedLogicalId")):
            evaluation.fail(f"{prefix}.expectedLogicalId", "Source logical ID must be a non-nil UUID")
        elif get_path(data, f"{prefix}.expectedLogicalId") != required_logical_ids[prefix]:
            evaluation.fail(f"{prefix}.expectedLogicalId", "Source logical ID differs from the committed .platform item")
        item_id = require_value(evaluation, data, f"{prefix}.observedItemId", "Service item ID is not verified")
        if item_id is not None and not is_uuid(item_id):
            evaluation.fail(f"{prefix}.observedItemId", "Observed service item ID must be a non-nil UUID")
        require_equal(
            evaluation,
            data,
            f"{prefix}.expectedDisplayName",
            f"{prefix}.observedDisplayName",
            "Observed artifact name differs from the authorized name",
        )

    model_id = get_path(data, f"{semantic}.observedItemId")
    bound_id = require_value(
        evaluation, data, f"{report}.observedSemanticModelId", "Report-to-semantic-model binding is not verified"
    )
    if model_id is not None and bound_id is not None and model_id != bound_id:
        evaluation.fail(
            f"{report}.observedSemanticModelId",
            "The report is not bound to the observed SQLDataWarehouse semantic model",
        )
    duplicate_count = require_value(
        evaluation, data, "artifacts.duplicateNameCount", "Artifact name uniqueness is not verified"
    )
    if isinstance(duplicate_count, int) and duplicate_count != 0:
        evaluation.fail("artifacts.duplicateNameCount", "Duplicate report or semantic-model names are present")

    expected_commit = require_value(
        evaluation, data, "artifacts.provenance.expectedSourceCommit", "Authorized source commit is missing"
    )
    observed_commit = require_value(
        evaluation, data, "artifacts.provenance.observedSourceCommit", "Deployed source commit is not verified"
    )
    for path, value in (
        ("artifacts.provenance.expectedSourceCommit", expected_commit),
        ("artifacts.provenance.observedSourceCommit", observed_commit),
    ):
        if value is not None and not COMMIT_RE.fullmatch(str(value)):
            evaluation.fail(path, "Source commit must be a full lowercase 40-character Git SHA")
    if expected_commit and observed_commit and expected_commit != observed_commit:
        evaluation.fail("artifacts.provenance.observedSourceCommit", "Deployed commit differs from authorized source")
    require_true(
        evaluation,
        data,
        "artifacts.provenance.logicalIdMappingVerified",
        "Logical-ID-to-Service-item mapping is not independently verified",
    )
    if parse_utc(get_path(data, "artifacts.provenance.verifiedAtUtc")) is None:
        evaluation.unknown("artifacts.provenance.verifiedAtUtc", "Provenance timestamp is missing or invalid")
    require_value(
        evaluation,
        data,
        "artifacts.provenance.evidenceReference",
        "Deployment provenance evidence reference is missing",
    )


def validate_connection(data: dict[str, Any], evaluation: Evaluation) -> None:
    expected_server = require_value(
        evaluation, data, "connection.expected.server", "Production SQL Server endpoint is not authorized"
    )
    if isinstance(expected_server, str) and expected_server.strip().lower() in LOOPBACK_NAMES:
        evaluation.fail("connection.expected.server", "Loopback SQL Server endpoints are prohibited in Production")
    if get_path(data, "connection.expected.environmentName") != "Production":
        evaluation.fail("connection.expected.environmentName", "EnvironmentName must be Production")
    timeout = get_path(data, "connection.expected.commandTimeoutMinutes")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 120:
        evaluation.fail("connection.expected.commandTimeoutMinutes", "Command timeout must be 1 to 120 minutes")
    if get_path(data, "connection.expected.privacyLevel") != "Organizational":
        evaluation.fail("connection.expected.privacyLevel", "Expected privacy level must be Organizational")
    require_value(
        evaluation,
        data,
        "connection.expected.authenticationType",
        "Approved gateway authentication type is not recorded",
    )

    for key in ("server", "database", "environmentName", "commandTimeoutMinutes"):
        require_equal(
            evaluation,
            data,
            f"connection.expected.{key}",
            f"connection.observed.{key}",
            f"Observed {key} differs from the authorized semantic-model parameter",
        )

    observed_server = get_path(data, "connection.observed.server")
    if isinstance(observed_server, str) and observed_server.strip().lower() in LOOPBACK_NAMES:
        evaluation.fail("connection.observed.server", "Observed service parameter still uses a loopback endpoint")

    if get_path(data, "connection.expected.gatewayRequired") is not True:
        evaluation.fail("connection.expected.gatewayRequired", "This on-premises SQL Server contract requires a gateway")
    for path, message in (
        ("connection.observed.gatewayId", "Gateway binding is not verified"),
        ("connection.observed.dataSourceId", "Gateway data-source mapping is not verified"),
    ):
        value = require_value(evaluation, data, path, message)
        if value is not None and not is_uuid(value):
            evaluation.fail(path, f"{message}; expected a non-nil UUID")

    gateway_type = require_value(evaluation, data, "connection.observed.gatewayType", "Gateway type is not verified")
    if gateway_type is not None and gateway_type != "Standard":
        evaluation.fail("connection.observed.gatewayType", "Only a Standard enterprise gateway is accepted")
    gateway_status = require_value(
        evaluation, data, "connection.observed.gatewayStatus", "Gateway online status is not verified"
    )
    if gateway_status is not None and gateway_status != "Online":
        evaluation.fail("connection.observed.gatewayStatus", "Gateway is not online")

    require_equal(
        evaluation,
        data,
        "connection.expected.server",
        "connection.observed.dataSourceServer",
        "Gateway data-source server differs from the authorized endpoint",
    )
    require_equal(
        evaluation,
        data,
        "connection.expected.database",
        "connection.observed.dataSourceDatabase",
        "Gateway data-source database differs from the authorized database",
    )
    require_equal(
        evaluation,
        data,
        "connection.expected.privacyLevel",
        "connection.observed.privacyLevel",
        "Observed privacy level differs from Organizational",
    )
    require_equal(
        evaluation,
        data,
        "connection.expected.authenticationType",
        "connection.observed.authenticationType",
        "Observed authentication type differs from the authorized gateway authentication type",
    )
    for path, message in (
        ("connection.observed.tlsVerified", "TLS use is not verified"),
        ("connection.observed.leastPrivilegeVerified", "Least-privilege database access is not verified"),
        ("connection.observed.credentialsConfigured", "Managed credentials are not verified"),
    ):
        require_true(evaluation, data, path, message)
    if parse_utc(get_path(data, "connection.observed.credentialsVerifiedAtUtc")) is None:
        evaluation.unknown(
            "connection.observed.credentialsVerifiedAtUtc", "Credential verification timestamp is missing or invalid"
        )


def validate_refresh(data: dict[str, Any], evaluation: Evaluation) -> None:
    if get_path(data, "refresh.pipelineGate.status") != "Passed":
        value = get_path(data, "refresh.pipelineGate.status")
        if value is None:
            evaluation.unknown("refresh.pipelineGate.status", "Warehouse pipeline gate is not verified")
        else:
            evaluation.fail("refresh.pipelineGate.status", "Warehouse pipeline gate did not pass")
    pipeline_completed = parse_utc(get_path(data, "refresh.pipelineGate.completedAtUtc"))
    if pipeline_completed is None:
        evaluation.unknown("refresh.pipelineGate.completedAtUtc", "Pipeline completion timestamp is missing or invalid")

    require_true(evaluation, data, "refresh.manual.triggerAccepted", "Manual refresh trigger was not accepted")
    refresh_id = require_value(evaluation, data, "refresh.manual.refreshId", "Manual refresh ID is not recorded")
    if refresh_id is not None and not str(refresh_id).strip():
        evaluation.fail("refresh.manual.refreshId", "Manual refresh ID must not be blank")
    manual_status = get_path(data, "refresh.manual.status")
    if manual_status is None:
        evaluation.unknown("refresh.manual.status", "Manual refresh completion is not verified")
    elif manual_status != "Completed":
        evaluation.fail("refresh.manual.status", "Manual refresh did not complete successfully")
    if get_path(data, "refresh.manual.serviceException") not in (None, ""):
        evaluation.fail("refresh.manual.serviceException", "Manual refresh recorded a service exception")

    started = parse_utc(get_path(data, "refresh.manual.startedAtUtc"))
    completed = parse_utc(get_path(data, "refresh.manual.completedAtUtc"))
    model_refreshed = parse_utc(get_path(data, "refresh.manual.modelRefreshedAtUtc"))
    for path, value, message in (
        ("refresh.manual.startedAtUtc", started, "Manual refresh start timestamp is missing or invalid"),
        ("refresh.manual.completedAtUtc", completed, "Manual refresh completion timestamp is missing or invalid"),
        ("refresh.manual.modelRefreshedAtUtc", model_refreshed, "Model refresh timestamp is missing or invalid"),
    ):
        if value is None:
            evaluation.unknown(path, message)
    if started and completed and completed < started:
        evaluation.fail("refresh.manual.completedAtUtc", "Manual refresh completed before it started")
    if pipeline_completed and started and started < pipeline_completed:
        evaluation.fail("refresh.manual.startedAtUtc", "Manual refresh started before the warehouse gate completed")
    if started and model_refreshed and model_refreshed < started:
        evaluation.fail(
            "refresh.manual.modelRefreshedAtUtc",
            "Model refresh evidence predates the current manual refresh",
        )
    if completed and model_refreshed and model_refreshed > completed and (
        model_refreshed - completed
    ).total_seconds() > 900:
        evaluation.fail(
            "refresh.manual.modelRefreshedAtUtc",
            "Model RefreshedAtUtc is more than 15 minutes after the completed service refresh",
        )

    dq_status = get_path(data, "refresh.dataQuality.overallStatus")
    if dq_status is None:
        evaluation.unknown("refresh.dataQuality.overallStatus", "Post-refresh data-quality status is not verified")
    elif dq_status != "Pass":
        evaluation.fail("refresh.dataQuality.overallStatus", "Data quality is not Pass; release must remain on HOLD")
    dq_verified = parse_utc(get_path(data, "refresh.dataQuality.verifiedAtUtc"))
    if dq_verified is None:
        evaluation.unknown("refresh.dataQuality.verifiedAtUtc", "Data-quality verification timestamp is missing or invalid")
    else:
        latest_refresh_evidence = max(
            (timestamp for timestamp in (completed, model_refreshed) if timestamp is not None),
            default=None,
        )
        if latest_refresh_evidence and dq_verified < latest_refresh_evidence:
            evaluation.fail(
                "refresh.dataQuality.verifiedAtUtc",
                "Data-quality evidence predates the current completed model refresh",
            )

    for key in ("enabled", "timeZone", "days", "times", "notifyOwner"):
        require_equal(
            evaluation,
            data,
            f"refresh.schedule.expected.{key}",
            f"refresh.schedule.observed.{key}",
            f"Observed refresh schedule {key} differs from the authorized schedule",
        )
    if get_path(data, "refresh.schedule.expected.enabled") is not True:
        evaluation.fail("refresh.schedule.expected.enabled", "Production scheduled refresh must be enabled")
    if get_path(data, "refresh.schedule.expected.notifyOwner") is not True:
        evaluation.fail("refresh.schedule.expected.notifyOwner", "Owner failure notification must be enabled")
    expected_zone = get_path(data, "refresh.schedule.expected.timeZone")
    if not isinstance(expected_zone, str) or not expected_zone.strip():
        evaluation.fail("refresh.schedule.expected.timeZone", "Authorized refresh time zone must be non-empty")
    expected_days = get_path(data, "refresh.schedule.expected.days")
    if not isinstance(expected_days, list) or not expected_days:
        evaluation.unknown("refresh.schedule.expected.days", "Authorized refresh days are not configured")
    elif len(expected_days) != len(set(expected_days)) or any(day not in ALLOWED_DAYS for day in expected_days):
        evaluation.fail("refresh.schedule.expected.days", "Refresh days must be unique canonical weekday names")
    expected_times = get_path(data, "refresh.schedule.expected.times")
    time_pattern = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    if not isinstance(expected_times, list) or not expected_times:
        evaluation.unknown("refresh.schedule.expected.times", "Authorized refresh times are not configured")
    elif len(expected_times) != len(set(expected_times)) or any(
        not isinstance(value, str) or not time_pattern.fullmatch(value) for value in expected_times
    ):
        evaluation.fail("refresh.schedule.expected.times", "Refresh times must be unique HH:MM values")
    configured_at = parse_utc(get_path(data, "refresh.schedule.configuredAtUtc"))
    if configured_at is None:
        evaluation.unknown("refresh.schedule.configuredAtUtc", "Schedule configuration timestamp is missing or invalid")
    overlap = get_path(data, "refresh.schedule.overlapWithWarehousePipeline")
    if overlap is None:
        evaluation.unknown(
            "refresh.schedule.overlapWithWarehousePipeline", "Schedule overlap with the warehouse pipeline is not verified"
        )
    elif overlap is not False:
        evaluation.fail("refresh.schedule.overlapWithWarehousePipeline", "Refresh schedule overlaps the warehouse pipeline")
    scheduled_status = get_path(data, "refresh.schedule.lastScheduledRunStatus")
    if scheduled_status is None:
        evaluation.unknown("refresh.schedule.lastScheduledRunStatus", "A successful scheduled run is not verified")
    elif scheduled_status != "Completed":
        evaluation.fail("refresh.schedule.lastScheduledRunStatus", "Last scheduled refresh did not complete")
    if parse_utc(get_path(data, "refresh.schedule.lastScheduledRunCompletedAtUtc")) is None:
        evaluation.unknown(
            "refresh.schedule.lastScheduledRunCompletedAtUtc",
            "Scheduled refresh completion timestamp is missing or invalid",
        )
    scheduled_completed = parse_utc(get_path(data, "refresh.schedule.lastScheduledRunCompletedAtUtc"))
    if completed and configured_at and configured_at <= completed:
        evaluation.fail(
            "refresh.schedule.configuredAtUtc",
            "Refresh schedule must be configured after the accepted manual release refresh",
        )
    if configured_at and scheduled_completed and scheduled_completed <= configured_at:
        evaluation.fail(
            "refresh.schedule.lastScheduledRunCompletedAtUtc",
            "Scheduled-run proof must occur after the current schedule configuration",
        )


def validate_security(data: dict[str, Any], evaluation: Evaluation) -> None:
    owner = require_value(
        evaluation, data, "security.semanticModelOwnerObjectId", "Semantic-model owner is not verified"
    )
    if owner is not None and not is_uuid(owner):
        evaluation.fail("security.semanticModelOwnerObjectId", "Semantic-model owner must be a non-nil object UUID")
    if get_path(data, "security.roleName") != "CountrySalesViewer":
        evaluation.fail("security.roleName", "Expected RLS role CountrySalesViewer is missing")
    require_true(evaluation, data, "security.roleExists", "CountrySalesViewer is not verified in the Service")

    kind = require_value(
        evaluation, data, "security.entitlementSource.kind", "Production entitlement source is not identified"
    )
    if isinstance(kind, str) and kind.strip().casefold() == "inlinedatatable":
        evaluation.fail("security.entitlementSource.kind", "Inline DATATABLE entitlements are prohibited in Production")
    elif kind is not None and kind not in ALLOWED_ENTITLEMENT_SOURCE_KINDS:
        evaluation.fail(
            "security.entitlementSource.kind",
            "Entitlement source kind is not an allowlisted governed production source",
        )
    require_true(evaluation, data, "security.entitlementSource.governed", "Entitlement source governance is not verified")
    source_identifier = require_value(
        evaluation,
        data,
        "security.entitlementSource.sourceIdentifier",
        "Governed entitlement source identifier is not recorded",
    )
    if source_identifier is not None and (
        not isinstance(source_identifier, str) or EMAIL_RE.search(source_identifier)
    ):
        evaluation.fail(
            "security.entitlementSource.sourceIdentifier",
            "Entitlement source identifier must be a non-email evidence-safe reference",
        )
    if parse_utc(get_path(data, "security.entitlementSource.verifiedAtUtc")) is None:
        evaluation.unknown(
            "security.entitlementSource.verifiedAtUtc",
            "Entitlement source verification timestamp is missing or invalid",
        )
    require_value(
        evaluation,
        data,
        "security.entitlementSource.evidenceReference",
        "Entitlement source evidence reference is missing",
    )
    synthetic_only = get_path(data, "security.entitlementSource.containsOnlySyntheticInvalidIdentities")
    if synthetic_only is None:
        evaluation.unknown(
            "security.entitlementSource.containsOnlySyntheticInvalidIdentities",
            "Synthetic-only entitlement status is not verified",
        )
    elif synthetic_only is not False:
        evaluation.fail(
            "security.entitlementSource.containsOnlySyntheticInvalidIdentities",
            "The entitlement source still contains only reserved .invalid identities",
        )

    approved_groups = require_value(evaluation, data, "security.approvedGroups", "No approved Entra RLS group is recorded")
    observed_members = require_value(
        evaluation, data, "security.observedRoleMembers", "RLS role membership is not verified"
    )

    def principal_set(path: str, values: Any) -> set[tuple[str, str]]:
        result: set[tuple[str, str]] = set()
        if not isinstance(values, list):
            evaluation.fail(path, "RLS principals must be a list")
            return result
        for index, item in enumerate(values):
            item_path = f"{path}[{index}]"
            if not isinstance(item, dict):
                evaluation.fail(item_path, "RLS principal must be an object")
                continue
            object_id = item.get("objectId")
            principal_type = item.get("principalType")
            if not is_uuid(object_id):
                evaluation.fail(f"{item_path}.objectId", "RLS principal object ID must be a non-nil UUID")
            if principal_type not in ALLOWED_RLS_GROUP_TYPES:
                evaluation.fail(
                    f"{item_path}.principalType",
                    "RLS principal type must be SecurityGroup, MailEnabledGroup, or DistributionGroup",
                )
            key = (str(object_id), str(principal_type))
            if key in result:
                evaluation.fail(item_path, "Duplicate RLS principal is prohibited")
            result.add(key)
        return result

    approved_set = principal_set("security.approvedGroups", approved_groups)
    observed_set = principal_set("security.observedRoleMembers", observed_members)
    if approved_set and observed_set and approved_set != observed_set:
        evaluation.fail("security.observedRoleMembers", "Observed RLS role members differ from approved groups")

    require_true(
        evaluation,
        data,
        "security.groupMembership.verified",
        "Direct and nested Entra group membership is not verified",
    )
    if parse_utc(get_path(data, "security.groupMembership.verifiedAtUtc")) is None:
        evaluation.unknown(
            "security.groupMembership.verifiedAtUtc", "RLS group-membership timestamp is missing or invalid"
        )
    require_value(
        evaluation,
        data,
        "security.groupMembership.evidenceReference",
        "RLS group-membership evidence reference is missing",
    )

    additive = get_path(data, "security.additiveRoleTest")
    if not isinstance(additive, dict):
        evaluation.unknown("security.additiveRoleTest", "Additive role test is not recorded")
    else:
        identity = additive.get("testIdentityReference")
        if identity is None or identity == "":
            evaluation.unknown(
                "security.additiveRoleTest.testIdentityReference",
                "Additive role test identity reference is missing",
            )
        elif not isinstance(identity, str) or EMAIL_RE.search(identity):
            evaluation.fail(
                "security.additiveRoleTest.testIdentityReference",
                "Additive role test requires a non-email evidence identity reference",
            )
        if additive.get("workspaceRole") is None:
            evaluation.unknown("security.additiveRoleTest.workspaceRole", "Additive role workspace role is missing")
        elif additive.get("workspaceRole") != "Viewer":
            evaluation.fail("security.additiveRoleTest.workspaceRole", "Additive role test principal must be Viewer")
        role_names = additive.get("roleNames")
        if role_names in (None, []):
            evaluation.unknown("security.additiveRoleTest.roleNames", "Additive role memberships are missing")
        elif not isinstance(role_names, list) or len(role_names) < 2 or len(role_names) != len(set(role_names)):
            evaluation.fail("security.additiveRoleTest.roleNames", "Additive test must record two or more unique roles")
        elif "CountrySalesViewer" not in role_names:
            evaluation.fail("security.additiveRoleTest.roleNames", "Additive test must include CountrySalesViewer")
        if additive.get("result") is None:
            evaluation.unknown("security.additiveRoleTest.result", "Additive role result is not verified")
        elif additive.get("result") != "Pass":
            evaluation.fail("security.additiveRoleTest.result", "Additive role exposure test did not pass")
        if parse_utc(additive.get("testedAtUtc")) is None:
            evaluation.unknown("security.additiveRoleTest.testedAtUtc", "Additive role timestamp is missing or invalid")
        if not isinstance(additive.get("evidenceReference"), str) or not additive.get("evidenceReference", "").strip():
            evaluation.unknown("security.additiveRoleTest.evidenceReference", "Additive role evidence is missing")

    tests = get_path(data, "security.rlsTests")
    if not isinstance(tests, list):
        evaluation.unknown("security.rlsTests", "RLS Service matrix is not recorded")
    else:
        by_case = {entry.get("case"): entry for entry in tests if isinstance(entry, dict)}
        if len(tests) != len(REQUIRED_RLS_CASES) or set(by_case) != REQUIRED_RLS_CASES:
            evaluation.fail("security.rlsTests", "RLS tests must contain each required identity case exactly once")
        for case in REQUIRED_RLS_CASES:
            entry = by_case.get(case)
            if not entry:
                continue
            identity = entry.get("testIdentityReference")
            if identity is None or identity == "":
                evaluation.unknown(
                    f"security.rlsTests.{case}.testIdentityReference",
                    "RLS test identity reference is missing",
                )
            elif not isinstance(identity, str) or EMAIL_RE.search(identity):
                evaluation.fail(
                    f"security.rlsTests.{case}.testIdentityReference",
                    "RLS test requires a non-email evidence identity reference",
                )
            if entry.get("workspaceRole") is None:
                evaluation.unknown(f"security.rlsTests.{case}.workspaceRole", "RLS workspace role is missing")
            elif entry.get("workspaceRole") != "Viewer":
                evaluation.fail(f"security.rlsTests.{case}.workspaceRole", "Effective RLS principal must be Viewer")
            expected_countries = entry.get("expectedCountries")
            actual_countries = entry.get("actualCountries")
            for label, countries in (("expectedCountries", expected_countries), ("actualCountries", actual_countries)):
                path = f"security.rlsTests.{case}.{label}"
                if not isinstance(countries, list) or len(countries) != len(set(countries)) or any(
                    not isinstance(country, str) or not re.fullmatch(r"[A-Z]{2}", country) for country in countries
                ):
                    evaluation.fail(path, "Country evidence must contain unique ISO alpha-2 codes")
            if isinstance(expected_countries, list) and isinstance(actual_countries, list):
                if set(expected_countries) != set(actual_countries):
                    evaluation.fail(f"security.rlsTests.{case}.actualCountries", "Actual country set differs from expected")
                if case == "Allowed" and not expected_countries:
                    evaluation.unknown(f"security.rlsTests.{case}.expectedCountries", "Allowed country is not recorded")
                elif case == "Allowed" and len(expected_countries) != 1:
                    evaluation.fail(f"security.rlsTests.{case}.expectedCountries", "Allowed requires one country")
                if case == "Multiple" and not expected_countries:
                    evaluation.unknown(f"security.rlsTests.{case}.expectedCountries", "Multiple countries are not recorded")
                elif case == "Multiple" and len(expected_countries) < 2:
                    evaluation.fail(f"security.rlsTests.{case}.expectedCountries", "Multiple requires two or more countries")
                if case not in {"Allowed", "Multiple"} and expected_countries:
                    evaluation.fail(f"security.rlsTests.{case}.expectedCountries", "Denied cases require no countries")
            for surface in ("sales", "inventory"):
                path = f"security.rlsTests.{case}.{surface}"
                result = entry.get(surface)
                if not isinstance(result, dict):
                    evaluation.unknown(path, f"{case} {surface} RLS measurements are not verified")
                    continue
                for field_name in ("expectedRows", "actualRows"):
                    value = result.get(field_name)
                    if value is None:
                        evaluation.unknown(f"{path}.{field_name}", "RLS row-count evidence is missing")
                    elif not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        evaluation.fail(f"{path}.{field_name}", "RLS row counts must be non-negative integers")
                expected_rows = result.get("expectedRows")
                actual_rows = result.get("actualRows")
                if isinstance(expected_rows, int) and isinstance(actual_rows, int) and expected_rows != actual_rows:
                    evaluation.fail(f"{path}.actualRows", "Actual protected row count differs from expected")
                if case in {"Allowed", "Multiple"} and expected_rows == 0:
                    evaluation.fail(f"{path}.expectedRows", "Allowed RLS cases require representative protected rows")
                if case not in {"Allowed", "Multiple"} and isinstance(expected_rows, int) and expected_rows != 0:
                    evaluation.fail(f"{path}.expectedRows", "Denied RLS cases require zero protected rows")
                expected_total_raw = result.get("expectedTotal")
                actual_total_raw = result.get("actualTotal")
                expected_total = parse_decimal(expected_total_raw)
                actual_total = parse_decimal(actual_total_raw)
                if expected_total_raw is None or actual_total_raw is None:
                    evaluation.unknown(path, f"{case} {surface} total reconciliation is not verified")
                else:
                    if expected_total is None:
                        evaluation.fail(
                            f"{path}.expectedTotal",
                            "Expected protected total must be a finite exact decimal value",
                        )
                    if actual_total is None:
                        evaluation.fail(
                            f"{path}.actualTotal",
                            "Actual protected total must be a finite exact decimal value",
                        )
                if expected_total is not None and actual_total is not None and expected_total != actual_total:
                    evaluation.fail(f"{path}.actualTotal", "Actual protected total differs from expected")
                if case not in {"Allowed", "Multiple"} and expected_total is not None and expected_total != 0:
                    evaluation.fail(f"{path}.expectedTotal", "Denied RLS cases require a zero protected total")
            if parse_utc(entry.get("testedAtUtc")) is None:
                evaluation.unknown(f"security.rlsTests.{case}.testedAtUtc", "RLS test timestamp is missing or invalid")
            evidence_reference = entry.get("evidenceReference")
            if not isinstance(evidence_reference, str) or not evidence_reference.strip():
                evaluation.unknown(f"security.rlsTests.{case}.evidenceReference", "RLS evidence reference is missing")

    global_surfaces = get_path(data, "security.globalSurfaceTests")
    if not isinstance(global_surfaces, dict) or set(global_surfaces) != REQUIRED_GLOBAL_SURFACES:
        evaluation.fail("security.globalSurfaceTests", "Global-surface tests must cover all required tables")
    elif isinstance(global_surfaces, dict):
        for surface in REQUIRED_GLOBAL_SURFACES:
            value = global_surfaces.get(surface)
            path = f"security.globalSurfaceTests.{surface}"
            if not isinstance(value, dict):
                evaluation.unknown(path, f"Global exposure test for {surface} is not verified")
                continue
            if value.get("result") is None:
                evaluation.unknown(f"{path}.result", f"Global exposure test for {surface} is not verified")
            elif value.get("result") != "Pass":
                evaluation.fail(f"{path}.result", f"Global exposure test for {surface} did not pass")
            if parse_utc(value.get("testedAtUtc")) is None:
                evaluation.unknown(f"{path}.testedAtUtc", f"Global test time for {surface} is missing or invalid")
            if not isinstance(value.get("evidenceReference"), str) or not value.get("evidenceReference", "").strip():
                evaluation.unknown(f"{path}.evidenceReference", f"Global evidence for {surface} is missing")


def validate_approval(data: dict[str, Any], evaluation: Evaluation) -> None:
    owner = require_value(evaluation, data, "approval.releaseOwnerObjectId", "Release owner is not approved")
    if owner is not None and not is_uuid(owner):
        evaluation.fail("approval.releaseOwnerObjectId", "Release owner must be a non-nil object UUID")
    require_true(evaluation, data, "approval.privacyApproved", "Privacy approval is not recorded")
    require_true(evaluation, data, "approval.capacityApproved", "Capacity approval is not recorded")
    if parse_utc(get_path(data, "approval.evidenceCapturedAtUtc")) is None:
        evaluation.unknown("approval.evidenceCapturedAtUtc", "Evidence capture timestamp is missing or invalid")
    max_age = get_path(data, "approval.maxEvidenceAgeHours")
    if not isinstance(max_age, int) or isinstance(max_age, bool) or not 1 <= max_age <= 168:
        evaluation.fail("approval.maxEvidenceAgeHours", "Maximum evidence age must be 1 to 168 hours")


def validate_evidence_freshness(
    data: dict[str, Any], evaluation: Evaluation, now: datetime | None = None
) -> None:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    max_age = get_path(data, "approval.maxEvidenceAgeHours")
    if not isinstance(max_age, int) or isinstance(max_age, bool) or not 1 <= max_age <= 168:
        return
    captured = parse_utc(get_path(data, "approval.evidenceCapturedAtUtc"))
    timestamp_paths = [
        "artifacts.provenance.verifiedAtUtc",
        "connection.observed.credentialsVerifiedAtUtc",
        "refresh.pipelineGate.completedAtUtc",
        "refresh.manual.startedAtUtc",
        "refresh.manual.completedAtUtc",
        "refresh.manual.modelRefreshedAtUtc",
        "refresh.dataQuality.verifiedAtUtc",
        "refresh.schedule.configuredAtUtc",
        "refresh.schedule.lastScheduledRunCompletedAtUtc",
        "security.entitlementSource.verifiedAtUtc",
        "security.groupMembership.verifiedAtUtc",
        "security.additiveRoleTest.testedAtUtc",
        "approval.evidenceCapturedAtUtc",
    ]
    for entry in get_path(data, "security.rlsTests") or []:
        if isinstance(entry, dict) and entry.get("case"):
            timestamp_paths.append(f"security.rlsTests.{entry['case']}.testedAtUtc")
    for surface in REQUIRED_GLOBAL_SURFACES:
        timestamp_paths.append(f"security.globalSurfaceTests.{surface}.testedAtUtc")

    rls_by_case = {
        entry.get("case"): entry for entry in (get_path(data, "security.rlsTests") or []) if isinstance(entry, dict)
    }

    def timestamp_for(path: str) -> Any:
        match = re.fullmatch(r"security\.rlsTests\.([^.]+)\.testedAtUtc", path)
        if match:
            return (rls_by_case.get(match.group(1)) or {}).get("testedAtUtc")
        return get_path(data, path)

    for path in timestamp_paths:
        timestamp = parse_utc(timestamp_for(path))
        if timestamp is None:
            continue
        if timestamp > now.replace(microsecond=0) and (timestamp - now).total_seconds() > 300:
            evaluation.fail(path, "Evidence timestamp is more than five minutes in the future")
        if (now - timestamp).total_seconds() > max_age * 3600:
            evaluation.fail(path, f"Evidence is older than the allowed {max_age} hours")
        if captured and timestamp > captured and (timestamp - captured).total_seconds() > 300:
            evaluation.fail(path, "Evidence timestamp occurs after the declared capture time")


def validate_contract(data: dict[str, Any], now: datetime | None = None) -> Evaluation:
    evaluation = Evaluation()
    detect_secret_material(data, "", evaluation)
    validate_target(data, evaluation)
    validate_artifacts(data, evaluation)
    validate_connection(data, evaluation)
    validate_refresh(data, evaluation)
    validate_security(data, evaluation)
    validate_approval(data, evaluation)
    validate_evidence_freshness(data, evaluation, now=now)
    return evaluation


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("[REDACTED]" if SECRET_KEY_PATTERN.search(key) else redact(child))
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str) and any(pattern.search(value) for pattern in SECRET_VALUE_PATTERNS):
        return "[REDACTED]"
    return value


def token_tenant_id(token: str) -> str:
    try:
        payload_segment = token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_segment + padding).decode("utf-8"))
        tenant_id = payload.get("tid")
    except (IndexError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("POWERBI_ACCESS_TOKEN is not a readable JWT with a tenant claim") from exc
    if not is_uuid(tenant_id):
        raise ValueError("POWERBI_ACCESS_TOKEN does not contain a valid tenant claim")
    return str(uuid.UUID(str(tenant_id)))


def reset_observations(contract: dict[str, Any]) -> dict[str, Any]:
    """Return a fresh evidence document while preserving authorized expectations."""

    result = copy.deepcopy(contract)
    for path in (
        "target.observedTenantId",
        "target.workspace.observedId",
        "target.workspace.observedName",
        "target.workspace.isPersonal",
        "target.workspace.observedCapacityMode",
        "target.workspace.observedCapacityId",
        "artifacts.provenance.observedSourceCommit",
        "artifacts.provenance.logicalIdMappingVerified",
        "artifacts.provenance.verifiedAtUtc",
        "artifacts.provenance.evidenceReference",
        "artifacts.semanticModel.observedItemId",
        "artifacts.semanticModel.observedDisplayName",
        "artifacts.report.observedItemId",
        "artifacts.report.observedDisplayName",
        "artifacts.report.observedSemanticModelId",
        "artifacts.duplicateNameCount",
    ):
        set_path(result, path, None)
    observed_connection = get_path(result, "connection.observed")
    if isinstance(observed_connection, dict):
        for key in observed_connection:
            observed_connection[key] = None
    set_path(result, "refresh.pipelineGate", {"status": None, "completedAtUtc": None})
    set_path(
        result,
        "refresh.manual",
        {
            "triggerAccepted": None,
            "refreshId": None,
            "status": None,
            "startedAtUtc": None,
            "completedAtUtc": None,
            "serviceException": None,
            "modelRefreshedAtUtc": None,
        },
    )
    set_path(result, "refresh.dataQuality", {"overallStatus": None, "verifiedAtUtc": None})
    schedule_observed = get_path(result, "refresh.schedule.observed")
    if isinstance(schedule_observed, dict):
        for key in schedule_observed:
            schedule_observed[key] = [] if key in {"days", "times"} else None
    for path in (
        "refresh.schedule.configuredAtUtc",
        "refresh.schedule.overlapWithWarehousePipeline",
        "refresh.schedule.lastScheduledRunStatus",
        "refresh.schedule.lastScheduledRunCompletedAtUtc",
        "security.semanticModelOwnerObjectId",
        "security.roleExists",
    ):
        set_path(result, path, None)
    set_path(
        result,
        "security.entitlementSource",
        {
            "kind": None,
            "sourceIdentifier": None,
            "governed": None,
            "containsOnlySyntheticInvalidIdentities": None,
            "verifiedAtUtc": None,
            "evidenceReference": None,
        },
    )
    set_path(result, "security.observedRoleMembers", [])
    set_path(
        result,
        "security.groupMembership",
        {"verified": None, "verifiedAtUtc": None, "evidenceReference": None},
    )
    set_path(
        result,
        "security.additiveRoleTest",
        {
            "testIdentityReference": None,
            "workspaceRole": None,
            "roleNames": [],
            "result": None,
            "testedAtUtc": None,
            "evidenceReference": None,
        },
    )
    empty_measurement = {
        "expectedRows": None,
        "actualRows": None,
        "expectedTotal": None,
        "actualTotal": None,
    }
    set_path(
        result,
        "security.rlsTests",
        [
            {
                "case": case,
                "testIdentityReference": None,
                "workspaceRole": None,
                "expectedCountries": [],
                "actualCountries": [],
                "sales": copy.deepcopy(empty_measurement),
                "inventory": copy.deepcopy(empty_measurement),
                "testedAtUtc": None,
                "evidenceReference": None,
            }
            for case in ("Allowed", "Multiple", "Inactive", "Expired", "Unknown", "Blank")
        ],
    )
    set_path(
        result,
        "security.globalSurfaceTests",
        {
            surface: {"result": None, "testedAtUtc": None, "evidenceReference": None}
            for surface in REQUIRED_GLOBAL_SURFACES
        },
    )
    set_path(result, "approval.privacyApproved", None)
    set_path(result, "approval.capacityApproved", None)
    set_path(result, "approval.evidenceCapturedAtUtc", None)
    return result


def api_get(token: str, relative_path: str) -> dict[str, Any]:
    url = f"{API_ROOT}/{relative_path.lstrip('/')}"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        raise ServiceInspectionError(f"Power BI REST GET failed with HTTP {exc.code} for {relative_path}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ServiceInspectionError(f"Power BI REST GET failed for {relative_path}: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise ServiceInspectionError(f"Power BI REST GET returned a non-object for {relative_path}")
    return redact(payload)


def exactly_one(items: Iterable[dict[str, Any]], name: str) -> tuple[dict[str, Any] | None, int]:
    matches = [item for item in items if item.get("name") == name]
    return (matches[0] if len(matches) == 1 else None, max(0, len(matches) - 1))


def parameter_value(parameters: list[dict[str, Any]], name: str) -> Any:
    match = next((item for item in parameters if item.get("name") == name), None)
    return match.get("currentValue") if match else None


def inspect_service(contract: dict[str, Any], token: str) -> dict[str, Any]:
    secret_evaluation = Evaluation()
    detect_secret_material(contract, "", secret_evaluation)
    secret_failures = [item for item in secret_evaluation.findings if item.status == "FAIL"]
    if secret_failures:
        raise ValueError("Input contract contains prohibited secret-bearing material")
    result = reset_observations(contract)
    expected_tenant_id = get_path(result, "target.expectedTenantId")
    if not is_uuid(expected_tenant_id):
        raise ValueError("target.expectedTenantId must be authorized before inspection")
    observed_tenant_id = token_tenant_id(token)
    if str(uuid.UUID(str(expected_tenant_id))) != observed_tenant_id:
        raise ValueError("Power BI token tenant differs from target.expectedTenantId")
    set_path(result, "target.observedTenantId", observed_tenant_id)

    workspace_id = get_path(result, "target.workspace.expectedId")
    if not is_uuid(workspace_id):
        raise ValueError("target.workspace.expectedId must be authorized before inspection")
    expected_workspace_name = get_path(result, "target.workspace.expectedName")
    if not isinstance(expected_workspace_name, str) or not expected_workspace_name.strip():
        raise ValueError("target.workspace.expectedName must be authorized before inspection")
    workspace_id = str(workspace_id)
    encoded_group = urllib.parse.quote(workspace_id, safe="")

    workspace = api_get(token, f"groups/{encoded_group}")
    set_path(result, "target.workspace.observedId", workspace.get("id"))
    set_path(result, "target.workspace.observedName", workspace.get("name"))
    set_path(result, "target.workspace.isPersonal", False)
    dedicated = workspace.get("isOnDedicatedCapacity")
    capacity_id = workspace.get("capacityId")
    set_path(result, "target.workspace.observedCapacityId", capacity_id)
    if dedicated is False:
        set_path(result, "target.workspace.observedCapacityMode", "Shared")

    reports_payload = api_get(token, f"groups/{encoded_group}/reports")
    datasets_payload = api_get(token, f"groups/{encoded_group}/datasets")
    reports = reports_payload.get("value", [])
    datasets = datasets_payload.get("value", [])
    report_name = get_path(result, "artifacts.report.expectedDisplayName")
    model_name = get_path(result, "artifacts.semanticModel.expectedDisplayName")
    report, report_duplicates = exactly_one(reports, report_name)
    dataset, dataset_duplicates = exactly_one(datasets, model_name)
    set_path(result, "artifacts.duplicateNameCount", report_duplicates + dataset_duplicates)

    if report:
        set_path(result, "artifacts.report.observedItemId", report.get("id"))
        set_path(result, "artifacts.report.observedDisplayName", report.get("name"))
        set_path(result, "artifacts.report.observedSemanticModelId", report.get("datasetId"))
    if not dataset:
        return result
    dataset_id = str(dataset.get("id"))
    encoded_dataset = urllib.parse.quote(dataset_id, safe="")
    set_path(result, "artifacts.semanticModel.observedItemId", dataset.get("id"))
    set_path(result, "artifacts.semanticModel.observedDisplayName", dataset.get("name"))

    parameters = api_get(token, f"groups/{encoded_group}/datasets/{encoded_dataset}/parameters").get("value", [])
    set_path(result, "connection.observed.server", parameter_value(parameters, "SqlServerName"))
    set_path(result, "connection.observed.database", parameter_value(parameters, "SqlDatabaseName"))
    set_path(result, "connection.observed.environmentName", parameter_value(parameters, "EnvironmentName"))
    timeout = parameter_value(parameters, "CommandTimeoutMinutes")
    try:
        timeout = int(timeout) if timeout is not None else None
    except (TypeError, ValueError):
        pass
    set_path(result, "connection.observed.commandTimeoutMinutes", timeout)

    sources = api_get(token, f"groups/{encoded_group}/datasets/{encoded_dataset}/datasources").get("value", [])
    sql_sources = [item for item in sources if str(item.get("datasourceType", "")).lower() == "sql"]
    if len(sql_sources) == 1:
        source = sql_sources[0]
        details = source.get("connectionDetails") or {}
        set_path(result, "connection.observed.gatewayId", source.get("gatewayId"))
        set_path(result, "connection.observed.dataSourceId", source.get("datasourceId"))
        set_path(result, "connection.observed.dataSourceServer", details.get("server"))
        set_path(result, "connection.observed.dataSourceDatabase", details.get("database"))

    schedule = api_get(token, f"groups/{encoded_group}/datasets/{encoded_dataset}/refreshSchedule")
    for target_key, source_key in (
        ("enabled", "enabled"),
        ("timeZone", "localTimeZoneId"),
        ("days", "days"),
        ("times", "times"),
        ("notifyOwner", "notifyOption"),
    ):
        value = schedule.get(source_key)
        if target_key == "notifyOwner" and value is not None:
            value = value in {"MailOnFailure", "MailOnCompletion"}
        set_path(result, f"refresh.schedule.observed.{target_key}", value)

    refreshes = api_get(token, f"groups/{encoded_group}/datasets/{encoded_dataset}/refreshes?$top=20").get(
        "value", []
    )
    scheduled = next((item for item in refreshes if item.get("refreshType") == "Scheduled"), None)
    if scheduled:
        set_path(result, "refresh.schedule.lastScheduledRunStatus", scheduled.get("status"))
        set_path(result, "refresh.schedule.lastScheduledRunCompletedAtUtc", scheduled.get("endTime"))
    manual = next(
        (
            item
            for item in refreshes
            if item.get("refreshType") in {"OnDemand", "ViaApi", "ViaEnhancedApi", "User"}
        ),
        None,
    )
    if manual:
        set_path(result, "refresh.manual.triggerAccepted", True)
        set_path(result, "refresh.manual.refreshId", manual.get("requestId") or manual.get("id"))
        set_path(result, "refresh.manual.status", manual.get("status"))
        set_path(result, "refresh.manual.startedAtUtc", manual.get("startTime"))
        set_path(result, "refresh.manual.completedAtUtc", manual.get("endTime"))
        service_exception = manual.get("serviceExceptionJson")
        set_path(result, "refresh.manual.serviceException", "Present" if service_exception else None)
    return redact(result)


def render_evaluation(evaluation: Evaluation, json_output: bool) -> str:
    payload = {
        "status": evaluation.status,
        "releaseAuthorized": evaluation.status == "PASS",
        "findings": [item.__dict__ for item in evaluation.findings],
    }
    if json_output:
        return json.dumps(payload, indent=2, sort_keys=True)
    lines = [f"Power BI Service release contract: {evaluation.status}"]
    for item in evaluation.findings:
        lines.append(f"- {item.status} {item.path}: {item.message}")
    if evaluation.status != "PASS":
        lines.append("Release remains blocked until every FAIL and UNKNOWN finding is resolved.")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Validate a completed service release contract")
    validate_parser.add_argument("--contract", type=Path, required=True)
    validate_parser.add_argument("--json", action="store_true", help="Emit machine-readable findings")

    inspect_parser = subparsers.add_parser("inspect", help="Collect read-only REST evidence into a contract copy")
    inspect_parser.add_argument("--contract", type=Path, required=True)
    inspect_parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        contract = load_json(args.contract)
        if args.command == "validate":
            evaluation = validate_contract(contract)
            print(render_evaluation(evaluation, args.json))
            return 0 if evaluation.status == "PASS" else 2

        token = os.environ.get("POWERBI_ACCESS_TOKEN")
        if not token:
            raise ValueError("POWERBI_ACCESS_TOKEN is not set in the current process environment")
        inspected = inspect_service(contract, token)
        output = args.output.resolve()
        if output == args.contract.resolve():
            raise ValueError("Inspection output must not overwrite the desired-state input contract")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(redact(inspected), indent=2) + "\n", encoding="utf-8")
        print(f"Read-only Power BI Service evidence written to {output}")
        print("Credentials, RLS memberships, approvals, and effective RLS tests still require explicit evidence.")
        return 0
    except (ValueError, ServiceInspectionError, OSError) as exc:
        print(f"Power BI Service contract operation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
