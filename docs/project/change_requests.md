# Change requests

## Implemented and verified

| ID | Outcome | Verification |
| --- | --- | --- |
| CR-DATA-001 | Complete canonical Silver load for Sales and all ERP subjects | runtime coverage, Silver contracts, CI |
| CR-OPS-002 | Converged SQLCMD/Python/CI orchestration on one fail-closed pipeline | static wiring contract and SQL Server CI |
| CR-SCHEMA-003 | Persisted Gold keys, SCD2 product/date/fact model, constraints/indexes | schema/data/reproducibility tests |
| CR-ERR-006 | Errors rethrow; Bronze/Silver publish atomically with durable audit | fail-closed and atomicity tests |
| CR-BI-007 | Added source-controlled Power BI model/report/KPI/RLS/refresh contracts | validator and negative tests; Desktop gate open |
| CR-SRC-008 | Added synthetic Inventory onboarding through Gold and Power BI | static plus double-run SQL tests |
| CR-LEG-005A | Removed tracked generated `logs/dbt.log` and empty Gold placeholder; added ignore rules | repository reference search and full gates |

## Open decisions

### CR-LIC-004 — Core dataset reuse rights

- **State:** owner/legal clarification required.
- **Problem:** the upstream repository carries MIT while a separate course usage notice may create ambiguity for commercial reuse or redistribution of the six upstream-derived fixtures. Four are byte-identical and two contain only the documented rename/final-LF normalization; the repository-specific Inventory fixture is outside this open decision.
- **Decision options:** obtain written clarification or replace them with independently generated fixtures.
- **Acceptance:** recorded rights basis, provenance, hashes, mapping/catalog updates, and equivalent passing runtime/model/Power BI tests.

### CR-LEG-005B — Notebook and diagram artifacts

- **State:** blocked on content/owner decision.
- The copy-named notebook has one additional code cell and is not byte-equivalent; do not delete as a duplicate without consolidating unique content.
- The Draw.io PDF is retained because no editable source is currently present; identify the intended durable source before replacement.

### CR-OPS-009 — Production environment controls

- **State:** proposed.
- Add versioned migrations, scheduler, alerting, backup/restore, retention, secrets, permissions, gateway/deployment, approvals, and operating ownership before production use.
