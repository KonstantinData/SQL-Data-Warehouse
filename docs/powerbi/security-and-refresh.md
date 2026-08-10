# Security and Refresh Design

## Refresh contract

Mode: **Import with full refresh**.

Parameters:

- `SqlServerName`: required server endpoint; no credential.
- `SqlDatabaseName`: database name, default `DataWarehouse`.
- `EnvironmentName`: descriptive deployment parameter.
- `CommandTimeoutMinutes`: query timeout.

Credentials are never stored in PBIP/TMDL. Desktop, the Power BI service, and any gateway must bind credentials through their managed connection stores.

The temporary semantic safety projection requires read access to the named Silver tables. This is broader than the preferred production boundary. The target state is a least-privilege principal with `CONNECT` plus `SELECT` on corrected curated Gold views only.

Refresh must run after the warehouse pipeline and enforced quality checks succeed. The current database drop/recreate process is not safe for concurrent refresh; a real deployment needs an atomic publish/staging contract or an explicit orchestration gate.

Incremental refresh is intentionally absent. Enable it only after stable persistent keys, non-destructive loads, an indexed order date, a durable modification watermark, late-arrival/delete rules, and query-folding evidence exist.

`Last Dataset Refresh UTC` is the semantic-model refresh time, not the warehouse pipeline completion time. `Data Through Date` is historical synthetic data coverage within the current access scope and must not be labelled global coverage or operational staleness.

## RLS design

`CountrySalesViewer` filters normalized `Customers[country_code]` using `USERPRINCIPALNAME()` and an entitlement table. Missing, inactive, blank, or unmatched entitlement returns zero rows (fail closed). The Customer-to-Sales relationship propagates the filter.

The checked-in entitlement rows use the reserved `.invalid` domain and exist only to demonstrate the contract. Before any real publication:

1. replace the inline table with a governed entitlement source;
2. normalize all supported country values;
3. test allowed, multi-country, inactive, expired, unknown, and blank identities;
4. assign service role memberships;
5. verify that broad and restricted roles are mutually exclusive because Power BI role memberships are additive.

RLS is not a boundary against semantic-model editors or administrators. Hidden columns are not security. The current customer source includes direct and linkable personal attributes; default report surfaces intentionally avoid names, customer numbers, birth dates, and hashes. Real data requires minimization and privacy review, restricted Build permission, and potentially curated Gold/OLS.

## Gateway and deployment

- SQL privacy level: Organizational.
- Require TLS and least-privilege per-environment credentials.
- Do not combine this source with Public sources without a reviewed privacy design.
- Use deployment rules to replace endpoint parameters across Development, Test, and Production.
- Service membership, gateway binding, refresh schedules, and credentials are operational state, not source-controlled PBIP evidence.
