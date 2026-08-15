# Power BI Service production release runbook

## Release rule

The release is fail-closed. `PASS` means every environment-specific target and observation in the service contract is verified. `FAIL` or `UNKNOWN` means **HOLD**. A successful publish or refresh alone is not a production approval.

Never record passwords, access tokens, refresh tokens, private keys, client secrets, credential values, browser session data, or recovery codes. Tenant, workspace, item, gateway, data-source, group, and owner object IDs are operational identifiers rather than credentials, but local evidence containing them should remain in ignored `*.local.json` files unless its publication is explicitly approved.

## Current source release blockers

The checked-in project is not production-ready without environment-specific remediation:

- `SqlServerName` defaults to `127.0.0.1` and `EnvironmentName` defaults to `Development`.
- `Security User Country` uses an inline synthetic `DATATABLE` with reserved `.invalid` identities. Adding a Service role member does not create a corresponding country entitlement.
- the dated Desktop acceptance reports `gold_source_orphan_coverage` as an Error. Production requires post-refresh data-quality status `Pass`.
- Fabric `.platform` logical IDs identify source-controlled items; they do not prove current Service workspace or item IDs.
- no tenant, workspace, capacity, Service item, gateway, data-source, Entra group, membership, owner, approval, schedule, or refresh-history evidence is committed.

## Prerequisites and authority

Before changing Service state, obtain and record outside source control:

1. the authorized tenant and non-personal target workspace;
2. the accountable release owner and semantic-model owner;
3. permission to publish or synchronize the report and semantic model;
4. gateway-admin support and permission to use the approved gateway data source;
5. the approved production SQL endpoint, database, authentication method, privacy level, TLS policy, and least-privilege database identity;
6. the approved capacity mode and refresh window;
7. the approved Microsoft Entra group object IDs for `CountrySalesViewer` and evidence of effective membership;
8. privacy approval for the intentionally global Product, Date, Data Quality Checks, and Refresh Metadata surfaces.

Semantic-model settings such as credentials and scheduled refresh can require model ownership even when the operator has workspace write permission. RLS protects workspace Viewers; it does not restrict Admin, Member, or Contributor users.

## Prepare a local contract

Copy `powerbi/service/service-contract.example.json` to `powerbi/service/service-contract.local.json`. The local filename is ignored by Git. Fill only authorized non-secret expected values: tenant, workspace ID/name, capacity, production connection, schedule, approved groups, release owner, and the full 40-character source commit. Leave every observed value and approval result as `null`; never guess them.

The committed source logical IDs are fixed:

| Item | Display name | Source logical ID |
|---|---|---|
| Semantic model | `SQLDataWarehouse` | `c8c14294-542b-460d-aec3-f48f0fcd40fc` |
| Report | `SQLDataWarehouse` | `696c2727-6eb6-4422-86a9-dc951409c8d8` |

Validate at any time:

```powershell
python scripts/powerbi_validation/powerbi_service_contract.py validate `
  --contract powerbi/service/service-contract.local.json
```

An untouched example returns `UNKNOWN` by design.

## 1. Confirm workspace and item mapping

1. Sign in to `https://app.powerbi.com` with the authorized work or school account.
2. Confirm the tenant and exact workspace with the release owner. Do not use My workspace.
3. Confirm capacity mode and, for dedicated capacity, the assigned capacity ID.
4. Publish through the approved Desktop or Fabric Git workflow. A source logical ID is not a Service item ID. Record the exact deployed commit separately from the expected commit.
5. In the workspace, require exactly one report and one semantic model named `SQLDataWarehouse`.
6. Record their Service item IDs and confirm that the report's semantic-model binding points to that exact semantic model.
7. Independently map both source logical IDs to those Service item IDs and retain a timestamped deployment evidence reference. The expected and observed commits must match exactly.
8. Stop if similarly named or duplicate items exist; do not delete or overwrite them without separate authorization.

The read-only inspector verifies the token tenant against the authorized tenant and collects the workspace, item mapping, parameters, SQL data-source mapping, refresh schedule, and refresh history. Before every run it clears all old observations, security evidence, and approvals so stale values cannot survive. It cannot prove credentials, logical-ID/build provenance, RLS role members, effective Entra membership, privacy approval, schedule configuration time, or effective RLS behavior.

```powershell
$env:POWERBI_ACCESS_TOKEN = az account get-access-token `
  --resource https://analysis.windows.net/powerbi/api `
  --query accessToken -o tsv

python scripts/powerbi_validation/powerbi_service_contract.py inspect `
  --contract powerbi/service/service-contract.local.json `
  --output powerbi/service/service-observation.local.json

Remove-Item Env:POWERBI_ACCESS_TOKEN
```

The token must remain process-local. Do not pass it as a command argument, print it, paste it into evidence, or commit the generated local file. The REST call is read-only. An authenticated browser session does not automatically authenticate Azure CLI.

Continue all manual verification in `powerbi/service/service-observation.local.json`. Do not return to the pre-inspection input as release evidence. The inspector captures the latest scheduled and latest on-demand/API refresh visible in history, but operators must still prove that those runs belong to the current deployment and warehouse gate.

## 2. Set semantic-model parameters

In the semantic-model settings, update and verify:

- `SqlServerName`: exact approved production endpoint; never loopback or `localhost`;
- `SqlDatabaseName`: approved production database;
- `EnvironmentName`: `Production`;
- `CommandTimeoutMinutes`: approved positive value.

Record observed values in the local contract. Stop if the service reports a dynamic data source or if the observed source tuple differs from the authorized gateway data source.

## 3. Map the gateway and data source

1. Use a Standard enterprise on-premises data gateway. Personal mode is not accepted for this release.
2. Confirm the gateway cluster is online, supported, current, and in the correct tenant/region.
3. A gateway administrator creates or identifies the SQL data source for the exact server/database tuple.
4. Confirm encryption/TLS expectations, authentication method, `Organizational` privacy level, and least-privilege `SELECT` access to every required Gold table and view.
5. Confirm the semantic-model owner is authorized to use that gateway data source.
6. In **Gateway and cloud connections**, map every semantic-model SQL source to the approved data source. A semantic model uses one gateway connection; do not mix similarly named definitions.
7. Record only gateway/data-source IDs and verified status. Never record the stored credential.

Only a gateway administrator can add gateway data sources. Missing gateway visibility can also mean the operator is not authorized to use the matching data source.

## 4. Configure managed credentials

Configure credentials only in the Power BI managed connection or gateway store. Record the approved and observed non-secret authentication type, then verify the successful connection test, privacy level, and verification time. The contract records `credentialsConfigured: true` and a UTC verification timestamp, never a credential value.

If credentials are missing, invalid, expired, or owned by the wrong semantic-model owner, keep the release on HOLD. Do not take over the semantic model unless that ownership change is explicitly authorized.

## 5. Run and verify a manual refresh

1. Complete the canonical warehouse pipeline and all enforced quality/model/Inventory gates first.
2. Record the pipeline completion time and confirm no Power BI refresh overlaps publication.
3. Trigger **Refresh now** only after gateway mapping and credential checks pass.
4. Capture the accepted refresh ID, UTC start/end time, final status, and sanitized service exception summary.
5. Reconcile Sales totals and Inventory `14/10/4/10` source/accepted/rejected/Gold evidence to the same warehouse run.
6. Verify the semantic model's `RefreshedAtUtc` is within 15 minutes of Service completion.
7. Require overall data-quality status `Pass`. The current Desktop `gold_source_orphan_coverage` Error is a production HOLD even if Service reports `Completed`.

Do not enable the schedule after a failed, cancelled, unknown, or unreconciled manual refresh.

## 6. Configure and prove scheduled refresh

1. Select approved days, times, and `W. Europe Standard Time` or another explicitly authorized Service time-zone ID.
2. Keep the schedule outside the warehouse publication window and its expected overrun buffer.
3. Enable owner failure notification and any separately approved operations notification route.
4. Record the observed schedule, its UTC configuration time, and compare it to the authorized contract. Production requires an enabled schedule, owner failure notification, canonical weekday names, valid `HH:MM` times, and an explicit time zone.
5. Wait for and verify at least one real scheduled run. An enabled toggle is not evidence that the gateway, credentials, and schedule operate unattended.

Power BI can pause scheduled refresh after repeated failures. Re-enable it only after the underlying gateway, credential, or source problem is resolved and a new manual refresh passes.

## 7. Configure and verify RLS

Do not assign production principals while the semantic model still uses the inline synthetic entitlement `DATATABLE`. First replace it with the allowlisted `WarehouseTable` source, validate normalization, validity windows, revocation, and country scope, and record its non-secret source identifier, UTC verification time, and evidence reference.

1. Open the semantic model's **Security** page and confirm `CountrySalesViewer` exists.
2. Assign only the approved Microsoft Entra security group, mail-enabled group, or distribution group object IDs and record the principal type for each. Microsoft 365 groups are not supported for RLS role membership.
3. Confirm direct and nested effective group membership in the authorized identity system. Do not infer membership from a display name.
4. Confirm test principals are workspace Viewers. Admin, Member, and Contributor permissions bypass RLS for workspace content.
5. Execute a dedicated additive-role test with a Viewer assigned to `CountrySalesViewer` and at least one additional role; retain the role list, result, UTC time, and evidence reference.
6. Run **Test as role** and effective Viewer tests for Allowed, Multiple, Inactive, Expired, Unknown, and Blank cases against both Sales and Inventory.
7. For every case record a non-email identity evidence reference, Viewer workspace role, expected/actual country codes, exact expected/actual row counts and totals for Sales and Inventory, UTC time, and evidence reference. Allowed must cover one country, Multiple at least two, and denied cases zero protected rows/totals.
8. Separately confirm the intended global exposure of Products, Date, Data Quality Checks, and Refresh Metadata with result, UTC time, and evidence reference.
9. Record sanitized outcomes and object IDs, not user email addresses, in the local contract.

The REST **Get Dataset Users** response is a content-access list, not proof of RLS role membership or effective Entra group expansion. Use the semantic-model Security page or another authorized role-membership source and perform effective Viewer tests.

## 8. Final validation and approval

Run:

```powershell
python scripts/powerbi_validation/powerbi_service_contract.py validate `
  --contract powerbi/service/service-observation.local.json --json
```

Release is authorized only when the result is `PASS`, the release owner approves the exact workspace/item IDs, and every timestamp is within `maxEvidenceAgeHours` (24 by default, maximum 168). The scheduled-run proof must be later than the accepted manual refresh and the current schedule configuration. Preserve sanitized evidence in the approved operations system. Do not commit local tenant or identity evidence without explicit approval.

## Rollback and incident response

- Disable scheduled refresh if it can repeatedly load invalid or unauthorized data.
- Do not delete or overwrite Service items as an implicit rollback.
- Restore the last approved report/semantic-model version through the approved deployment mechanism and retain the failed refresh/evidence identifiers.
- Revoke or correct incorrect RLS group assignments immediately through the authorized identity and Service owners.
- Rotate credentials in the managed store when compromise is suspected; never move credential material into the repository.
- Keep the release on HOLD until a new manual refresh, DQ reconciliation, schedule proof, and RLS matrix all pass.

## Microsoft references

- [Semantic model settings](https://learn.microsoft.com/en-us/power-bi/connect-data/service-semantic-model-settings-pane)
- [Data refresh in Power BI](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-data)
- [Configure scheduled refresh](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-scheduled-refresh)
- [Row-level security in Power BI](https://learn.microsoft.com/en-us/fabric/security/service-admin-row-level-security)
- [Power BI datasets REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets)
