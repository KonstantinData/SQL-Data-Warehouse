# SQL Server Runtime Runbook

## Scope and safety boundary

This repository is a production-oriented reference implementation built with
repository-owned synthetic CSV data. It has not been deployed to production and
does not claim production operating experience.

The runtime implements fail-closed SQL Server loading, durable batch/step audit
metadata, explicit snapshot watermarks, idempotent replay handling, linked
restarts, reject evidence, application-lock serialization, and layer-atomic
publication. A real deployment must separately provide backup/restore, HA/DR,
secrets management, least-privilege service accounts, scheduling, alerting,
capacity planning, retention, schema migration governance, and load testing.

The public SQLCMD and Python entrypoints represent the **full pipeline** and do
not report success until Gold and Inventory are published. The internal
`control.run_pipeline` procedure is the reusable CRM/ERP Bronze/Silver core. A
direct core-only call is audited under the distinct
`sql-data-warehouse-core-snapshot` scope and must not be interpreted as a full
pipeline result.

## Entrypoints

| File | Classification | Contract |
|---|---|---|
| `scripts/init.database.sql` | Safe bootstrap | Creates missing database, schemas, and control objects without dropping data. |
| `scripts/run_pipeline.sql` | Canonical install-and-run entrypoint | Installs all modules and keeps the batch `RUNNING` until CRM/ERP Bronze/Silver, physical Gold, and Inventory succeed. |
| `scripts/orchestrate_pipeline.py` | Credential-safe Python wrapper | Calls the canonical SQLCMD entrypoint; SQL authentication uses `SQLCMDPASSWORD`, never a password argument. |
| `scripts/operations/run_operational_pipeline.sql` | Installed-environment routine execution | Delegates to the same end-to-end runtime after all modules are installed. |
| `scripts/operations/reset_development.sql` | Destructive development/test reset | Drops `DataWarehouse` only after the exact confirmation token. Never use for restart or recovery. |

Routine execution must never call the development reset.

## Runtime installation order

The canonical entrypoint installs the following idempotent module groups before
execution:

```text
scripts/init.database.sql
scripts/bronze_layer/create_table_bronze_layer.sql
scripts/bronze_layer/bulk_insert_crm_cust_info.sql
scripts/silver_layer/create_silver_table_structure.sql
scripts/silver_layer/load_silver.sql
scripts/gold_layer/00_create_gold_tables.sql
scripts/gold_layer/10_load_gold.sql
scripts/gold_layer/20_create_gold_indexes.sql
scripts/source_inventory/00_create_objects.sql
scripts/source_inventory/10_load_bronze.sql
scripts/source_inventory/20_transform_silver.sql
scripts/source_inventory/30_create_gold_views.sql
```

Every post-bootstrap script explicitly selects `DataWarehouse`. The canonical
SQLCMD execution retains one session so its session-owned application lock also
covers Gold and Inventory publication.

## Preconditions

- SQL Server 2019 or newer and `sqlcmd` are available.
- The SQL Server service identity can read the absolute `BasePath`. `BULK INSERT`
  reads from the server host, not from the client running `sqlcmd`.
- All six expected CRM/ERP files are present and stable.
- `SourceVersion` identifies the exact immutable bytes/manifest. Never reuse it
  for changed files.
- `SourceWatermark` is a positive, monotonically increasing delivery sequence.
- `SnapshotAsOf` is the immutable ISO business date used to close product
  versions retired by this full snapshot.
- `MaxRejectRows` is an explicit operator decision. `0` is strictest. The
  current repository synthetic snapshot produces 23 quarantined source rows
  under SQL Server 2022 verification: 4 missing required customer keys, 18
  invalid sales order dates, and 1 invalid Sales date sequence. Two missing
  Product costs remain visible as DQ warnings while their valid identities are
  preserved. The documented demo ceiling is therefore `23`.
- Use integrated authentication where possible. Never place passwords in the
  repository, command history, audit metadata, or documentation.

## Run a new snapshot

```powershell
sqlcmd -S "<server>" -d master -E -b `
  -i scripts/run_pipeline.sql `
  -v BasePath="C:\data\SQL-Data-Warehouse\datasets" `
     SourceVersion="synthetic-2026-08-10-v1" `
     SourceWatermark="2026081001" `
     SnapshotAsOf="2024-12-31" `
     MaxRejectRows="23" `
     RestartOfBatchId="0"
```

Success requires both process exit code `0` and terminal audit status
`SUCCEEDED` or the intentional no-op status `SKIPPED`. Console text alone is
not evidence of success. Use `sqlcmd -o <file>` if local policy requires a
separate execution transcript.

## Restart a failed attempt

An identical retry creates a new batch linked to the failed batch. It never
resumes a half-committed transaction.

```powershell
sqlcmd -S "<server>" -d master -E -b `
  -i scripts/run_pipeline.sql `
  -v BasePath="C:\data\SQL-Data-Warehouse\datasets" `
     SourceVersion="synthetic-2026-08-10-v1" `
     SourceWatermark="2026081001" `
     SnapshotAsOf="2024-12-31" `
     MaxRejectRows="23" `
     RestartOfBatchId="<failed_batch_id>"
```

The referenced batch must be `FAILED` and must have the same version and
watermark. If any source bytes changed, allocate a new version and a greater
watermark instead. Replaying an already successful version is audited as
`SKIPPED` and changes no targets, rejects, or watermarks.

## Transaction and failure contract

- One session-owned application lock serializes target-mutating execution. A
  replay of an already successful source version may be audited as `SKIPPED`
  before lock acquisition because it cannot mutate targets.
- The initial `RUNNING` batch is durable before loading begins.
- All files stage as text before Bronze target mutation.
- Bronze validation evidence is durable before its quality gate.
- All six Bronze tables publish in one transaction.
- Silver cleanses CRM customer, product, and sales data plus all three ERP
  entities. Valid Product versions are preserved; Gold resolves Sales to the
  effective version by order date and persists the resulting surrogate key.
- All six Silver tables, six watermarks, and the Silver step publish in one
  transaction. The batch deliberately remains `RUNNING`.
- The same session and application lock then publish physical Gold and
  Inventory. Separate audited steps record both results. Only after both
  succeed does the batch transition to `SUCCEEDED`; a downstream error records
  the same batch and active step as `FAILED` and is rethrown.
- Every `CATCH` rolls back an active transaction, records full SQL error
  metadata, and rethrows the original error.
- A Silver failure can occur after Bronze committed. This is deliberate
  layer-atomicity: Silver and watermarks remain on the previous snapshot, while
  a linked retry always restages and republishes Bronze.
- Reject counts are validation-rule violations. The explicit ceiling is a
  release gate; rejected rows do not enter the affected published target.

## Audit and evidence queries

```sql
SELECT TOP (20)
    batch_id, pipeline_name, source_version, source_watermark,
    max_reject_rows, status, restart_of_batch_id,
    started_at_utc, completed_at_utc,
    error_number, error_severity, error_state,
    error_procedure, error_line, error_message
FROM control.pipeline_batch
ORDER BY batch_id DESC;

DECLARE @batch_id BIGINT = 123; -- replace with the batch under investigation

SELECT step_id, step_name, attempt_no, status, source_name, target_name,
       rows_read, rows_accepted, rows_rejected, rows_superseded, rows_published,
       watermark_before, watermark_after,
       started_at_utc, completed_at_utc,
       error_number, error_procedure, error_line, error_message
FROM control.pipeline_step
WHERE batch_id = @batch_id
ORDER BY step_id;

SELECT source_name, source_file, source_row_number, business_key,
       column_name, rule_code, raw_value, raw_payload,
       error_message, rejected_at_utc
FROM control.load_reject
WHERE batch_id = @batch_id
ORDER BY reject_id;

SELECT pipeline_name, source_name, watermark_value,
       source_version, batch_id, updated_at_utc
FROM control.load_watermark
ORDER BY pipeline_name, source_name;
```

`source_row_number` is a batch-local staging ordinal assigned after bulk
staging, not a guaranteed physical CSV line or a cross-retry identity.
`source_file`, business key, raw payload, and rule code are the primary
remediation evidence.

Potentially stale client-disconnect evidence is found with an operator-chosen
threshold:

```sql
SELECT batch_id, source_version, started_at_utc
FROM control.pipeline_batch
WHERE status = 'RUNNING'
  AND started_at_utc < DATEADD(MINUTE, -30, SYSUTCDATETIME());
```

Before acting, confirm that no server session/job still owns the pipeline.
Never edit audit rows merely to make an attempt look successful. A conforming
new run reconciles older `RUNNING` rows only after it acquires the exclusive
runtime lock.

## Failure handling

| Symptom | Evidence and action |
|---|---|
| `Cannot bulk load` / access denied | Confirm the path is visible from the SQL Server host and the service account has read access. Retry identical bytes with a linked restart. |
| Missing/header-only file | The batch fails before target publication. Restore a complete snapshot; changed bytes require a new version/watermark. |
| Conversion or quality rejects above ceiling | Inspect `control.load_reject`, correct the source or explicitly approve a reviewed ceiling, and use correct version semantics. |
| Application-lock failure | Let the active run finish, inspect its audit state, then retry. Never bypass the lock. |
| Deadlock, timeout, or log-full | The failing publication transaction rolls back. Remediate capacity/contention, then run a linked retry. |
| Silver transformation/constraint failure | Silver and watermarks remain unchanged. Bronze may contain the attempted snapshot; linked retry restages it. |
| Gold or Inventory publication failure | The batch and downstream step are `FAILED`; the caller receives the original error. A linked retry replays the idempotent core and downstream publications. |
| Client disconnect / stale `RUNNING` | Inspect SQL sessions, application lock, targets, and watermarks. Do not infer server failure from client loss alone. |
| Missing runtime object | Treat as an installation/migration defect, not a data retry. Repair the idempotent deployment first. |

## Destructive development reset

Only against a disposable non-production database:

```powershell
sqlcmd -S "<non-production-server>" -d master -E -b `
  -i scripts/operations/reset_development.sql `
  -v ConfirmReset="RESET_DATAWAREHOUSE_FOR_DEVELOPMENT"
```

The reset destroys warehouse data plus all control, watermark, reject, and audit
evidence. It is irreversible within this repository. Create an external backup
first if anything must be retained. A missing, unresolved, or different token
fails before mutation.

## Verification

Run only against a disposable database initialized with synthetic data:

```powershell
sqlcmd -S "<server>" -d DataWarehouse -E -b -i tests/runtime_contract.sql
sqlcmd -S "<server>" -d DataWarehouse -E -b -i tests/runtime_fail_closed.sql
sqlcmd -S "<server>" -d DataWarehouse -E -b -i tests/runtime_idempotency.sql `
  -v TestBasePath="C:\data\SQL-Data-Warehouse\datasets" `
     ConfirmRuntimeTests="RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE"
sqlcmd -S "<server>" -d DataWarehouse -E -b -i tests/runtime_silver_coverage.sql
sqlcmd -S "<server>" -d DataWarehouse -E -b -i tests/runtime_atomicity.sql `
  -v TestBasePath="C:\data\SQL-Data-Warehouse\datasets" `
     ConfirmRuntimeTests="RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE"
```

The idempotency/restart and atomicity tests replace published test data and
therefore require the exact disposable-database confirmation token. The Silver
coverage test inspects the latest successful synthetic runtime batch and should
run immediately after idempotency or another verified synthetic load. Reset
refusal and concurrent-lock behavior require shell/two-session tests; they
cannot be proved by a single success-only SQLCMD script.

## Known limitations

- This is full-snapshot CSV publication, not row-level CDC.
- Snapshot identity and watermark are operator supplied; there is no built-in
  cryptographic manifest or file-arrival protocol.
- `BULK INSERT` depends on SQL Server host filesystem and ACL behavior.
- SQL Server on Linux does not accept the Windows `CODEPAGE` option for
  explicit UTF-8 decoding. The supplied synthetic files are compatible with
  the verified environment; deployments with non-ASCII source data must
  validate platform encoding or add a platform-specific import adapter.
- The two upstream fixtures that lacked a final LF are normalized in this
  repository; attribution records both current and upstream hashes. A real
  delivery must still manifest exact bytes and newline conventions.
- SQLCMD variables are textual substitutions. Scripts accept trusted operator
  inputs; apostrophes in paths/versions are unsupported, and the reset token is
  an accidental-safety guard rather than a malicious-caller boundary.
- The runtime has no scheduler, external alert transport, automatic backfill
  planner, retention purge, backup/restore, HA/DR, vault integration, workload
  sizing, or production certification.

## Integrated handoff contract

SQLCMD, Python, and CI now converge on the same repository-owned runtime,
physical Gold model, and Inventory publication. Changes to module order,
parameters, success boundaries, or quality thresholds must update the three
entrypoints, static parity checks, runtime tests, and this runbook together.
