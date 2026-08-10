# Security and refresh design

## Refresh contract

Mode: **Import with full refresh**.

Parameters: `SqlServerName`, `SqlDatabaseName`, `EnvironmentName`, and `CommandTimeoutMinutes`. Credentials are never stored in PBIP/TMDL; Desktop, Power BI Service, or a gateway must bind them through managed connection stores.

The core semantic model needs least-privilege `SELECT` on curated Gold core tables and Inventory views. The disconnected data-quality table additionally reads the named Gold quality sources. Refresh starts only after the canonical pipeline and enforced quality/model/Inventory gates succeed.

Core CRM/ERP publication is non-destructive and layer-atomic. Gold and Inventory reruns are transactional/idempotent. A deployment still needs an orchestration gate so Power BI never refreshes between layer publications.

Incremental refresh is intentionally absent. Add it only with reviewed range parameters, query-folding evidence, indexed change dates, late-arrival/delete rules, partition tests, retention policy, and an environment-specific capacity assessment.

## RLS design

`CountrySalesViewer` filters `Customers[country_code]` through `Security User Country` and `USERPRINCIPALNAME()`. Missing, inactive, blank, or unmatched entitlement returns zero rows. Checked-in identities use the reserved `.invalid` domain.

Before publication:

1. replace inline entitlements with a governed source;
2. confirm normalization for every supported country;
3. test allowed, multiple, inactive, expired, unknown, and blank identities;
4. verify additive role membership cannot broaden restricted users;
5. decide whether Inventory requires a separate warehouse/country entitlement path.

RLS does not restrict semantic-model editors or administrators. Hidden columns are not security. Real customer data requires minimization, privacy review, restricted Build permission, database authorization, and potentially object-level security.

## Gateway and deployment

- Require TLS and per-environment least-privilege credentials.
- Treat privacy level as Organizational and review any source combination.
- Replace endpoints through deployment rules.
- Keep gateway binding, refresh schedules, credentials, capacity, role membership, and approvals as audited environment state.
