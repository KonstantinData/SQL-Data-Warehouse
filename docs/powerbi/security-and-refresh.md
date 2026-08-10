# Security and refresh design

## Refresh contract

Mode: **Import with full refresh**.

Parameters: `SqlServerName`, `SqlDatabaseName`, `EnvironmentName`, and `CommandTimeoutMinutes`. Credentials are never stored in PBIP/TMDL; Desktop, Power BI Service, or a gateway must bind them through managed connection stores.

The core semantic model needs least-privilege `SELECT` on curated Gold core tables and Inventory views. The disconnected data-quality table additionally reads the named Gold quality sources. Refresh starts only after the canonical pipeline and enforced quality/model/Inventory gates succeed.

Core CRM/ERP publication is non-destructive and layer-atomic. Gold and Inventory reruns are transactional/idempotent. A deployment still needs an orchestration gate so Power BI never refreshes between layer publications.

Incremental refresh is intentionally absent. Add it only with reviewed range parameters, query-folding evidence, indexed change dates, late-arrival/delete rules, partition tests, retention policy, and an environment-specific capacity assessment.

## RLS design

`CountrySalesViewer` filters both `Customers[country_code]` and `Inventory Locations[country_code]` through `Security User Country` and `USERPRINCIPALNAME()`. The dimension relationships propagate the filter to the protected Sales and Inventory Snapshot facts. Missing, inactive, blank, or unmatched entitlement returns zero protected fact rows. Shared Product, Date, refresh, and global data-quality metadata are intentionally outside this reference role; production owners must approve that boundary or isolate those surfaces. Checked-in identities use the reserved `.invalid` domain.

Before publication:

1. replace inline entitlements with a governed source;
2. confirm normalization for every supported country;
3. test allowed, multiple, inactive, expired, unknown, and blank identities;
4. verify additive role membership cannot broaden restricted users;
5. verify that country entitlement is the correct Inventory boundary or replace it with a dedicated warehouse entitlement model.

RLS does not restrict semantic-model editors or administrators. Hidden columns are not security. Real customer data requires minimization, privacy review, restricted Build permission, database authorization, and potentially object-level security.

## Gateway and deployment

- Require TLS and per-environment least-privilege credentials.
- Treat privacy level as Organizational and review any source combination.
- Replace endpoints through deployment rules.
- Keep gateway binding, refresh schedules, credentials, capacity, role membership, and approvals as audited environment state.
