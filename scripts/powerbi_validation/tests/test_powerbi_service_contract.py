from __future__ import annotations

import base64
import copy
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from powerbi_service_contract import inspect_service, redact, validate_contract  # noqa: E402


EXAMPLE_PATH = REPOSITORY_ROOT / "powerbi/service/service-contract.example.json"
REFERENCE_NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)


def iso(minutes_before: int) -> str:
    return (REFERENCE_NOW - timedelta(minutes=minutes_before)).isoformat().replace("+00:00", "Z")


def fake_jwt(tenant_id: str) -> str:
    def encode(value: dict) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{encode({'alg': 'none'})}.{encode({'tid': tenant_id})}.signature-value"


def passing_contract() -> dict:
    contract = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    contract["target"].update(
        {
            "expectedTenantId": "11111111-1111-4111-8111-111111111111",
            "observedTenantId": "11111111-1111-4111-8111-111111111111",
            "workspace": {
                "expectedId": "22222222-2222-4222-8222-222222222222",
                "observedId": "22222222-2222-4222-8222-222222222222",
                "expectedName": "Analytics Production",
                "observedName": "Analytics Production",
                "isPersonal": False,
                "expectedCapacityMode": "Shared",
                "observedCapacityMode": "Shared",
                "expectedCapacityId": None,
                "observedCapacityId": None,
            },
        }
    )
    model_id = "33333333-3333-4333-8333-333333333333"
    contract["artifacts"]["provenance"] = {
        "expectedSourceCommit": "a" * 40,
        "observedSourceCommit": "a" * 40,
        "logicalIdMappingVerified": True,
        "verifiedAtUtc": iso(90),
        "evidenceReference": "deployment-evidence:release-001",
    }
    contract["artifacts"]["semanticModel"].update(
        {"observedItemId": model_id, "observedDisplayName": "SQLDataWarehouse"}
    )
    contract["artifacts"]["report"].update(
        {
            "observedItemId": "44444444-4444-4444-8444-444444444444",
            "observedDisplayName": "SQLDataWarehouse",
            "observedSemanticModelId": model_id,
        }
    )
    contract["artifacts"]["duplicateNameCount"] = 0
    contract["connection"]["expected"].update(
        {"server": "sql-prod.example.internal", "authenticationType": "Windows"}
    )
    contract["connection"]["observed"].update(
        {
            "server": "sql-prod.example.internal",
            "database": "DataWarehouse",
            "environmentName": "Production",
            "commandTimeoutMinutes": 10,
            "gatewayId": "55555555-5555-4555-8555-555555555555",
            "gatewayType": "Standard",
            "gatewayStatus": "Online",
            "dataSourceId": "66666666-6666-4666-8666-666666666666",
            "dataSourceServer": "sql-prod.example.internal",
            "dataSourceDatabase": "DataWarehouse",
            "privacyLevel": "Organizational",
            "authenticationType": "Windows",
            "tlsVerified": True,
            "leastPrivilegeVerified": True,
            "credentialsConfigured": True,
            "credentialsVerifiedAtUtc": iso(85),
        }
    )
    contract["refresh"]["pipelineGate"] = {"status": "Passed", "completedAtUtc": iso(80)}
    contract["refresh"]["manual"] = {
        "triggerAccepted": True,
        "refreshId": "refresh-20260815-01",
        "status": "Completed",
        "startedAtUtc": iso(75),
        "completedAtUtc": iso(70),
        "serviceException": None,
        "modelRefreshedAtUtc": iso(69),
    }
    contract["refresh"]["dataQuality"] = {"overallStatus": "Pass", "verifiedAtUtc": iso(68)}
    expected_schedule = {
        "enabled": True,
        "timeZone": "W. Europe Standard Time",
        "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "times": ["06:00"],
        "notifyOwner": True,
    }
    contract["refresh"]["schedule"] = {
        "expected": expected_schedule,
        "observed": copy.deepcopy(expected_schedule),
        "configuredAtUtc": iso(60),
        "overlapWithWarehousePipeline": False,
        "lastScheduledRunStatus": "Completed",
        "lastScheduledRunCompletedAtUtc": iso(30),
    }
    group = {
        "objectId": "77777777-7777-4777-8777-777777777777",
        "principalType": "SecurityGroup",
    }

    def rls_case(case: str, countries: list[str], rows: int, total: str) -> dict:
        return {
            "case": case,
            "testIdentityReference": f"evidence:test-principal-{case.lower()}",
            "workspaceRole": "Viewer",
            "expectedCountries": countries,
            "actualCountries": list(countries),
            "sales": {
                "expectedRows": rows,
                "actualRows": rows,
                "expectedTotal": total,
                "actualTotal": total,
            },
            "inventory": {
                "expectedRows": max(1, rows // 2) if rows else 0,
                "actualRows": max(1, rows // 2) if rows else 0,
                "expectedTotal": total,
                "actualTotal": total,
            },
            "testedAtUtc": iso(18),
            "evidenceReference": f"rls-evidence:{case.lower()}",
        }

    contract["security"].update(
        {
            "semanticModelOwnerObjectId": "88888888-8888-4888-8888-888888888888",
            "roleExists": True,
            "entitlementSource": {
                "kind": "WarehouseTable",
                "sourceIdentifier": "warehouse:security.UserCountryEntitlement",
                "governed": True,
                "containsOnlySyntheticInvalidIdentities": False,
                "verifiedAtUtc": iso(26),
                "evidenceReference": "entitlement-evidence:source-001",
            },
            "approvedGroups": [group],
            "observedRoleMembers": [copy.deepcopy(group)],
            "groupMembership": {
                "verified": True,
                "verifiedAtUtc": iso(25),
                "evidenceReference": "entra-evidence:membership-001",
            },
            "additiveRoleTest": {
                "testIdentityReference": "evidence:test-principal-additive",
                "workspaceRole": "Viewer",
                "roleNames": ["CountrySalesViewer", "RestrictedAuditRole"],
                "result": "Pass",
                "testedAtUtc": iso(20),
                "evidenceReference": "rls-evidence:additive",
            },
            "rlsTests": [
                rls_case("Allowed", ["DE"], 10, "100.00"),
                rls_case("Multiple", ["DE", "US"], 20, "200.00"),
                rls_case("Inactive", [], 0, "0.00"),
                rls_case("Expired", [], 0, "0.00"),
                rls_case("Unknown", [], 0, "0.00"),
                rls_case("Blank", [], 0, "0.00"),
            ],
            "globalSurfaceTests": {
                surface: {
                    "result": "Pass",
                    "testedAtUtc": iso(15),
                    "evidenceReference": f"rls-evidence:global-{surface.lower().replace(' ', '-')}",
                }
                for surface in ("Products", "Date", "Data Quality Checks", "Refresh Metadata")
            },
        }
    )
    contract["approval"] = {
        "releaseOwnerObjectId": "99999999-9999-4999-8999-999999999999",
        "privacyApproved": True,
        "capacityApproved": True,
        "evidenceCapturedAtUtc": iso(0),
        "maxEvidenceAgeHours": 24,
    }
    return contract


class PowerBIServiceContractTests(unittest.TestCase):
    def evaluate(self, contract: dict):
        return validate_contract(contract, now=REFERENCE_NOW)

    def test_example_is_fail_closed_unknown(self) -> None:
        evaluation = self.evaluate(json.loads(EXAMPLE_PATH.read_text(encoding="utf-8")))
        self.assertEqual("UNKNOWN", evaluation.status)
        self.assertTrue(any(item.path == "target.workspace.expectedId" for item in evaluation.findings))

    def test_complete_contract_passes(self) -> None:
        evaluation = self.evaluate(passing_contract())
        self.assertEqual([], evaluation.findings)
        self.assertEqual("PASS", evaluation.status)

    def test_loopback_production_server_fails(self) -> None:
        contract = passing_contract()
        contract["connection"]["expected"]["server"] = "127.0.0.1"
        contract["connection"]["observed"]["server"] = "127.0.0.1"
        contract["connection"]["observed"]["dataSourceServer"] = "127.0.0.1"
        evaluation = self.evaluate(contract)
        self.assertEqual("FAIL", evaluation.status)
        self.assertTrue(any("Loopback" in item.message for item in evaluation.findings))

    def test_report_binding_mismatch_fails(self) -> None:
        contract = passing_contract()
        contract["artifacts"]["report"]["observedSemanticModelId"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("not bound" in item.message for item in evaluation.findings))

    def test_stale_evidence_fails(self) -> None:
        contract = passing_contract()
        contract["approval"]["evidenceCapturedAtUtc"] = "2020-01-01T00:00:00Z"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("older than" in item.message for item in evaluation.findings))

    def test_disabled_schedule_and_notification_fail(self) -> None:
        contract = passing_contract()
        contract["refresh"]["schedule"]["expected"]["enabled"] = False
        contract["refresh"]["schedule"]["observed"]["enabled"] = False
        contract["refresh"]["schedule"]["expected"]["notifyOwner"] = False
        contract["refresh"]["schedule"]["observed"]["notifyOwner"] = False
        evaluation = self.evaluate(contract)
        self.assertTrue(any("must be enabled" in item.message for item in evaluation.findings))

    def test_scheduled_run_before_current_configuration_fails(self) -> None:
        contract = passing_contract()
        contract["refresh"]["schedule"]["lastScheduledRunCompletedAtUtc"] = iso(65)
        evaluation = self.evaluate(contract)
        self.assertTrue(any("after the current schedule" in item.message for item in evaluation.findings))

    def test_duplicate_rls_case_fails(self) -> None:
        contract = passing_contract()
        contract["security"]["rlsTests"][-1] = copy.deepcopy(contract["security"]["rlsTests"][0])
        evaluation = self.evaluate(contract)
        self.assertTrue(any("exactly once" in item.message for item in evaluation.findings))

    def test_rls_measurement_mismatch_fails(self) -> None:
        contract = passing_contract()
        contract["security"]["rlsTests"][0]["sales"]["actualRows"] = 11
        evaluation = self.evaluate(contract)
        self.assertTrue(any("row count differs" in item.message for item in evaluation.findings))

    def test_secret_bearing_field_fails_and_inspection_rejects_it(self) -> None:
        contract = passing_contract()
        contract["connection"]["observed"]["client_secret"] = "must-not-be-stored"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("Secret-bearing fields" in item.message for item in evaluation.findings))
        with self.assertRaisesRegex(ValueError, "secret-bearing"):
            inspect_service(contract, fake_jwt(contract["target"]["expectedTenantId"]))

    def test_inline_entitlements_fail(self) -> None:
        contract = passing_contract()
        contract["security"]["entitlementSource"]["kind"] = "InlineDatatable"
        contract["security"]["entitlementSource"]["containsOnlySyntheticInvalidIdentities"] = True
        evaluation = self.evaluate(contract)
        self.assertTrue(any("Inline DATATABLE" in item.message for item in evaluation.findings))
        self.assertTrue(any(".invalid" in item.message for item in evaluation.findings))

    def test_data_quality_error_holds_release(self) -> None:
        contract = passing_contract()
        contract["refresh"]["dataQuality"]["overallStatus"] = "Error"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("release must remain on HOLD" in item.message for item in evaluation.findings))

    def test_data_quality_evidence_before_current_refresh_fails(self) -> None:
        contract = passing_contract()
        contract["refresh"]["dataQuality"]["verifiedAtUtc"] = iso(76)
        evaluation = self.evaluate(contract)
        self.assertTrue(any("Data-quality evidence predates" in item.message for item in evaluation.findings))

    def test_model_refresh_evidence_before_manual_start_fails(self) -> None:
        contract = passing_contract()
        contract["refresh"]["manual"]["modelRefreshedAtUtc"] = iso(76)
        evaluation = self.evaluate(contract)
        self.assertTrue(any("predates the current manual refresh" in item.message for item in evaluation.findings))

    def test_unallowlisted_entitlement_source_fails(self) -> None:
        contract = passing_contract()
        contract["security"]["entitlementSource"]["kind"] = "UnverifiedMagic"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("not an allowlisted" in item.message for item in evaluation.findings))

    def test_nonnumeric_rls_totals_fail(self) -> None:
        contract = passing_contract()
        contract["security"]["rlsTests"][0]["sales"]["expectedTotal"] = "banana"
        contract["security"]["rlsTests"][0]["sales"]["actualTotal"] = "banana"
        evaluation = self.evaluate(contract)
        self.assertTrue(any("finite exact decimal" in item.message for item in evaluation.findings))

    def test_token_in_evidence_url_fails_and_is_redacted(self) -> None:
        contract = passing_contract()
        unsafe_reference = "https://evidence.invalid/item?access_token=must-not-be-stored"
        contract["artifacts"]["provenance"]["evidenceReference"] = unsafe_reference
        evaluation = self.evaluate(contract)
        self.assertTrue(any("Secret-like values" in item.message for item in evaluation.findings))
        self.assertEqual("[REDACTED]", redact(unsafe_reference))

    def test_read_only_inspector_clears_stale_and_populates_current_evidence(self) -> None:
        contract = passing_contract()
        contract["artifacts"]["report"]["observedItemId"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

        def fake_get(_token: str, path: str) -> dict:
            if path == "groups/22222222-2222-4222-8222-222222222222":
                return {
                    "id": "22222222-2222-4222-8222-222222222222",
                    "name": "Analytics Production",
                    "isOnDedicatedCapacity": False,
                }
            if path.endswith("/reports"):
                return {
                    "value": [
                        {
                            "id": "44444444-4444-4444-8444-444444444444",
                            "name": "SQLDataWarehouse",
                            "datasetId": "33333333-3333-4333-8333-333333333333",
                        }
                    ]
                }
            if path.endswith("/datasets"):
                return {"value": [{"id": "33333333-3333-4333-8333-333333333333", "name": "SQLDataWarehouse"}]}
            if path.endswith("/parameters"):
                return {
                    "value": [
                        {"name": "SqlServerName", "currentValue": "sql-prod.example.internal"},
                        {"name": "SqlDatabaseName", "currentValue": "DataWarehouse"},
                        {"name": "EnvironmentName", "currentValue": "Production"},
                        {"name": "CommandTimeoutMinutes", "currentValue": "10"},
                    ]
                }
            if path.endswith("/datasources"):
                return {
                    "value": [
                        {
                            "datasourceType": "Sql",
                            "gatewayId": "55555555-5555-4555-8555-555555555555",
                            "datasourceId": "66666666-6666-4666-8666-666666666666",
                            "connectionDetails": {"server": "sql-prod.example.internal", "database": "DataWarehouse"},
                        }
                    ]
                }
            if path.endswith("/refreshSchedule"):
                return {
                    "enabled": True,
                    "localTimeZoneId": "W. Europe Standard Time",
                    "days": ["Monday"],
                    "times": ["06:00"],
                    "notifyOption": "MailOnFailure",
                }
            if "/refreshes?" in path:
                return {
                    "value": [
                        {"refreshType": "Scheduled", "status": "Completed", "endTime": iso(30)},
                        {
                            "refreshType": "OnDemand",
                            "requestId": "refresh-current",
                            "status": "Completed",
                            "startTime": iso(75),
                            "endTime": iso(70),
                        },
                    ]
                }
            self.fail(f"Unexpected REST path: {path}")

        token = fake_jwt(contract["target"]["expectedTenantId"])
        with patch("powerbi_service_contract.api_get", side_effect=fake_get):
            inspected = inspect_service(contract, token)

        self.assertEqual(contract["target"]["expectedTenantId"], inspected["target"]["observedTenantId"])
        self.assertEqual("Analytics Production", inspected["target"]["workspace"]["observedName"])
        self.assertEqual("Shared", inspected["target"]["workspace"]["observedCapacityMode"])
        self.assertEqual("44444444-4444-4444-8444-444444444444", inspected["artifacts"]["report"]["observedItemId"])
        self.assertEqual("refresh-current", inspected["refresh"]["manual"]["refreshId"])
        self.assertIsNone(inspected["security"]["roleExists"])
        self.assertIsNone(inspected["approval"]["privacyApproved"])
        self.assertNotIn(token, json.dumps(inspected))

    def test_inspector_rejects_wrong_tenant_before_rest_calls(self) -> None:
        contract = passing_contract()
        with patch("powerbi_service_contract.api_get") as mocked_get:
            with self.assertRaisesRegex(ValueError, "token tenant differs"):
                inspect_service(contract, fake_jwt("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))
        mocked_get.assert_not_called()

    def test_redactor_removes_secret_fields_bearer_values_and_jwts(self) -> None:
        jwt = fake_jwt("11111111-1111-4111-8111-111111111111")
        payload = {
            "apiKey": "key-value",
            "safe": {"authorization": "Bearer token-value", "jwt": jwt, "name": "SQLDataWarehouse"},
        }
        redacted = redact(payload)
        self.assertEqual("[REDACTED]", redacted["apiKey"])
        self.assertEqual("[REDACTED]", redacted["safe"]["authorization"])
        self.assertEqual("[REDACTED]", redacted["safe"]["jwt"])
        self.assertEqual("SQLDataWarehouse", redacted["safe"]["name"])


if __name__ == "__main__":
    unittest.main()
