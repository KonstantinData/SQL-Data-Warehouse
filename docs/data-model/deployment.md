# Gold Deployment and Integration

## Deployment contract

Run Gold only after all six required Silver CRM and ERP tables exist and have been populated. The deployment is idempotent for an existing materialized Gold model: it preserves retained identity keys, updates changed attributes, inserts new identities, synchronizes the fact snapshot, and recreates missing workload indexes without dropping existing Gold tables.

The legacy-named `scripts/gold_layer/create_gold_views.sql` remains the compatibility entry point because existing integration files already reference it. It now deploys physical tables rather than views. Run SQLCMD entry points from the repository root so their `:r` paths resolve consistently.

## Dependency order

1. Remove the legacy fact view, then legacy dimension views, during the one-time migration.
2. Create `dim_customers`, `dim_products`, and `dim_date`.
3. Create `fact_sales` with named PK, alternate keys, checks, and foreign keys.
4. Seed key-`0` Unknown members.
5. Acquire a transaction-owned application lock and start the atomic Gold load.
6. Upsert customers without regenerating retained keys.
7. derive and upsert nonoverlapping product versions.
8. Populate the contiguous date dimension.
9. resolve and synchronize facts, including strict as-of product matching.
10. Create workload-backed indexes.
11. Run model checks.

The loader uses `SET XACT_ABORT ON`, `TRY/CATCH`, an explicit transaction, and `THROW`. Concurrent Gold loads fail closed instead of interleaving.

## Commands

From the repository root, after a complete Silver load:

```powershell
sqlcmd -b -d DataWarehouse -i .\scripts\gold_layer\create_gold_views.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_schema_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_data_quality.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_reproducibility.sql
```

`scripts/gold_layer/deploy_gold_layer.sql` is an explicit alias for the same compatibility entry point.

The `-b` flag is required so SQL errors and test `THROW` statements produce a failing process exit code.

## Integrated execution

The canonical SQLCMD, Python, and CI paths now call the complete audited Silver runtime before this Gold entrypoint. CI executes schema, data-quality, reproducibility, negative, and Inventory gates. The historical CI-only Silver substitute has been removed.

The optional million-row performance fixture must never be added to the standard pipeline or per-commit CI path. It is an isolated benchmark workflow.

## Re-run and rollback behavior

- Re-running Gold in a retained database preserves keys for unchanged customer, product-version, date, and sales-line identities.
- A failed load rolls back the complete Gold data mutation.
- The DDL scripts do not drop materialized Gold tables on repeat execution.
- The repository's destructive database initialization resets identity keys; restoring key continuity across that operation requires an external key-map or database backup and is outside this reference scope.
- Rollback of the schema change should use a database backup or a forward migration. Converting populated physical tables back to views is destructive and is not automated.

## Integration handoff checklist

- Populate all required Silver inputs before Gold.
- Invoke the compatibility entry point from the repository root in SQLCMD mode.
- Propagate SQLCMD failures with `-b` or equivalent.
- Run schema, data-quality, and reproducibility tests in that order.
- Keep the performance suite opt-in.
- Keep README, orchestration, Power BI partitions, and quality contracts synchronized with the physical Gold tables.
- Do not claim a production deployment; the committed data is synthetic.
