# Example change requests

These examples are communication-ready proposals grounded in current repository
evidence. Every item remains **proposed, not implemented**, and requires the
listed owner decisions before code or runtime changes.

## CR-DATA-001 — Complete the standard Silver load

- **State:** Proposed.
- **Problem:** The standard SQLCMD path loads only customer and product into
  Silver. Sales and three ERP Silver tables remain empty, although Gold depends
  on them.
- **Evidence:** `scripts/run_pipeline.sql`, dedicated Silver loaders, and
  `scripts/gold_layer/create_gold_views.sql`.
- **Outcome:** Add canonical, reusable transformations for sales, demographics,
  location, and category; make CI call the same logic.
- **Risk:** Row-count drift, join fan-out, invalid integer dates, and semantic
  differences from the CI-only loader.
- **Acceptance:** One execution contract; source/Bronze/Silver reconciliation;
  non-empty fact view; no unmatched customer/product dimension keys; CI green.
- **Rollback:** Restore prior scripts and recreate the disposable warehouse.

## CR-OPS-002 — Converge SQLCMD and Python orchestration

- **State:** Proposed; integration-owned.
- **Problem:** Python launches a new `sqlcmd` process per file, defaults to
  `master`, omits Gold, and runs no quality gate. SQLCMD and CI have different
  coverage.
- **Outcome:** One ordered phase manifest with explicit database context and
  common validation, consumed by every runner.
- **Risk:** Bootstrap connection behavior, secret handling, quoting, and false
  success when the Bronze procedure catches without rethrowing errors.
- **Acceptance:** Fresh disposable-instance tests for every supported runner
  produce the same object and row-count contract.
- **Rollback:** Retain the previous runner until parity evidence is accepted.

## CR-SCHEMA-003 — Stabilize schema and analytical keys

- **State:** Proposed.
- **Problem:** `cust_is_future` is added imperatively after Silver DDL, while Gold
  depends on it. Gold `ROW_NUMBER()` keys are calculated by views and are not
  durable warehouse surrogate keys.
- **Outcome:** Put the column in canonical DDL and choose an explicit key
  strategy appropriate to snapshot-only semantics.
- **Risk:** Consumer key changes and view-result incompatibility.
- **Acceptance:** Deterministic reruns, documented key contract, uniqueness and
  null checks, and consumer migration evidence.
- **Rollback:** Restore views and guarded column addition; rebuild synthetic DB.

## CR-LIC-004 — Resolve bundled dataset reuse rights

- **State:** Proposed; legal/owner clarification required.
- **Problem:** The upstream repository uses MIT, while the official course page
  separately restricts project materials to learning/personal use.
- **Outcome:** Obtain written clarification or replace all six CSVs with
  independently generated and licensed synthetic fixtures.
- **Risk:** Redistribution or commercial-use uncertainty.
- **Acceptance:** Recorded rights basis, provenance, new hashes, mapping and
  catalog updates, and equivalent pipeline tests.
- **Rollback:** Keep the current reference repository non-commercial while the
  question remains unresolved.

## CR-LEG-005 — Review and remove obsolete repository artifacts

- **State:** Proposed; no deletion authorized.
- **Candidates:** Gold placeholder, tracked dbt log, Draw.io PDF without editable
  source, and the copy-named notebook.
- **Risk:** A file may be a training artifact, packaging marker, or the only
  retained representation; the notebooks are materially different.
- **Acceptance:** Named owner, reference search, content comparison, replacement
  or archive decision, and repository checks.
- **Rollback:** Restore the exact Git blobs.

## CR-ERR-006 — Make Bronze failures observable

- **State:** Proposed.
- **Problem:** `bronze.load_bronze` prints caught errors but does not `THROW`, so
  caller exit status may not represent a failed or partial load.
- **Outcome:** Preserve diagnostics and rethrow, with explicit transaction and
  partial-load policy.
- **Risk:** Existing runners may begin failing where they previously continued.
- **Acceptance:** Negative-path test proves nonzero caller status; positive path
  reconciles all six source row counts.
- **Rollback:** Restore prior procedure only with documented monitoring gap.
