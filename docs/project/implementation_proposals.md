# Implementation proposals and decisions

## Completed in the reference implementation

### A — One authoritative pipeline

Completed: canonical full CRM/ERP Silver logic, shared SQLCMD/Python/CI entry path, audited errors/restarts/watermarks/quarantine, Gold and Inventory integration, and layer/model reconciliation.

### B — Stable analytical contracts

Completed: persisted surrogate keys, physical customer/product/date/sales objects, SCD2 product intervals, Unknown members, trusted constraints, workload indexes, and two-run key stability tests.

### E — Source-controlled reporting

Completed at source level: PBIP/PBIR/TMDL, curated Gold queries, Inventory integration, explicit measures/KPI catalog, RLS/refresh design, report/mobile/accessibility metadata, and automated validators. Power BI Desktop validation remains an external release gate.

## Open proposals

### C — Independently generated core fixture pack

Replace the six inherited course fixtures with deterministically generated repository-owned equivalents after owner/legal review. Acceptance requires scenario parity, new provenance and hashes, all runtime/model/Power BI gates, and explicit licensing.

### D — Versioned production migrations

Introduce migration IDs/checksums, supported upgrade paths, permission preservation, backup/rollback evidence, and clean-install-versus-upgrade catalog equality before claiming upgrade support.

### F — Environment operations

Add a scheduler/orchestrator, monitoring/alerts, secret manager, backup/restore tests, retention, SLOs, ownership, Power BI gateway/deployment pipeline, and audited approvals. None of these should be inferred from local CI.

### G — Incremental CRM/ERP and Inventory processing

Replace full snapshots only after source change semantics, late-arrival/delete policy, durable modification watermark, partition strategy, reconciliation, and performance evidence are specified.

## Priority

Resolve the rights-clear fixture decision first for external/commercial reuse. For a real deployment, deliver versioned migrations and environment operations before incremental processing or scale claims.
