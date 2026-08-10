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

## Existing upstream integration limitation

The current reserved standard runners are not a complete CRM/ERP Silver path:

- `scripts/run_pipeline.sql` loads Silver customer and product cleansing but does not populate Silver sales or the three ERP tables;
- `scripts/orchestrate_pipeline.py` also omits Gold and continues to require integration-owned database-routing corrections;
- the existing CI-specific Silver loader does populate the reference inputs used by Gold, but its product-version join multiplies identical sales lines. Gold restores the validated `(order_number, product_number)` source grain and rejects conflicting duplicates.

This Gold subtask does not write into Silver and does not modify those reserved runners. The integration owner must add a production-style Silver sales/ERP route and add the new model tests to automation. Until then, the manual Gold commands require a separately completed Silver load, and the standard quick start may produce an empty fact table with missing ERP enrichment.

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
- Update integration-owned README and orchestration wording from Gold views to physical tables.
- Do not claim a production deployment; the committed data is synthetic.
