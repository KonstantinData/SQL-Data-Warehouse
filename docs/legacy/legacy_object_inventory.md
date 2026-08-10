# Legacy-object and artifact inventory

## Status vocabulary

- **canonical:** defined and referenced by the current repository;
- **retired in Git:** removed or renamed in repository history;
- **suspected runtime residual:** absent from current files but could exist in an
  upgraded database; runtime existence is unverified;
- **review candidate:** a repository artifact with a legacy signal; no deletion
  is authorized.

## Canonical current objects

| Layer | Canonical objects | Status |
| --- | --- | --- |
| Database/schema | `DataWarehouse`; `bronze`, `silver`, `gold` | canonical; bootstrap recreates them |
| Bronze | six `bronze.*` source tables and `bronze.load_bronze` | canonical |
| Silver | `silver.crm_cust_info`, `silver.crm_prd_info`, `silver.crm_sales_details`, `silver.erp_cust_az12`, `silver.erp_loc_a101`, `silver.erp_px_cat_g1v2` | canonical definitions; four have CI-only loaders |
| Gold | `gold.dim_customers`, `gold.dim_products`, `gold.fact_sales` | canonical views |

An object is not legacy merely because another execution profile omits it.

## Confirmed historical names

Commit `87c5604e9bc67bdb9af964424adf75cf13e2349f` standardized local customer
naming. Before that change, repository history contained:

- `bronze.crm_cst_info` and `silver.crm_cst_info`;
- `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, and related `cst_*`
  warehouse columns;
- `bulk_insert_crm_cst_info.sql` and `cleansing_crm_cst_info.sql` filenames.

Current scripts/tests use `crm_cust_info` and `cust_*`. The source file remains
`datasets/source_crm/cst_info.csv`, and its header remains `cst_*`; that is an
upstream source contract and is not a legacy warehouse object.

## Suspected runtime residuals

| Candidate | Why it could remain | Current evidence | Required decision evidence |
| --- | --- | --- | --- |
| `bronze.crm_cst_info` | current DDL drops only canonical names | absent from HEAD | `sys.objects`, row/size/permission capture, dependency search |
| `silver.crm_cst_info` | same | absent from HEAD | catalog and consumer evidence |
| upstream-style `silver.load_silver` | local implementation uses split scripts and CI loader | absent from HEAD | `sys.procedures`, module text, job references |
| modules referencing `cst_*` | external/manual modules are not visible in Git | unverified | `sys.sql_modules`, dependency DMVs, SQL Agent/BI inventory |

A clean run of `scripts/init.database.sql` cannot retain these objects because it
drops and recreates the entire database. Only an in-place or manually modified
environment could contain them. No live environment was inspected for this
inventory.

## Repository artifact candidates

| Path | Signal | Classification | Removal gate |
| --- | --- | --- | --- |
| `scripts/gold_layer/placeholder` | tracked zero-byte marker; real Gold DDL exists | review candidate | confirm no packaging/training reference; integration owner decides |
| `scripts/python_sql_server_connection copy.ipynb` | copy-style filename | review candidate, **not a proven duplicate** | compare code cells and unique learning content; name an authoritative notebook |
| `scripts/python_sql_server_connection.ipynb` | machine-specific server value and stored execution errors | remediation candidate | remove outputs/configure parameters without losing tutorial intent |
| `logs/dbt.log` | generated log is versioned | review candidate | confirm audit need; remove from tracking and ignore only with owner approval |
| `drawio/warehouse-architecture.drawio.pdf` | export remains after editable Draw.io sources were removed | review candidate | confirm PDF is the intended durable diagram or provide editable source |
| `scripts/bronze_layer/bronze-load-bronze.sql` | one-line procedure invocation | active, not legacy | referenced by SQLCMD/Python; do not remove before runner convergence |
| `scripts/bronze_layer/bulk_insert_crm_cust_info.sql` | filename names one entity but procedure loads all six | rename candidate, active | update every runner/reference atomically |

`requirements.txt` declares `dbt-core`, but the repository has no
`dbt_project.yml`; this is dependency/configuration drift rather than proof that
dbt or its log is safe to delete.

## Active lifecycle risks, not legacy objects

- `cust_is_future` is added by the customer transformation but required by Gold;
  move it into canonical DDL before removing the guarded `ALTER TABLE`.
- Gold views are dropped/recreated, which can disrupt grants and dependent
  objects. A later migration may use `CREATE OR ALTER VIEW` after compatibility
  validation.
- Bronze/Silver table DDL and database bootstrap are destructive reset logic,
  not versioned in-place migrations.
- CI Silver logic is active test scaffolding with different product semantics;
  it must not be silently promoted or deleted.
- SQLCMD and Python runners have different coverage; neither is removable until
  an authoritative runner passes parity checks.

## Dependency-aware removal order

For warehouse-object retirement, remove or migrate consumers before providers:

1. external reports/jobs/notebooks;
2. `gold.fact_sales`;
3. `gold.dim_customers` and/or `gold.dim_products`;
4. affected Silver procedures/scripts and tables;
5. affected Bronze procedure/table;
6. source fixture only after no loader or documentation contract requires it.

The order is a planning aid. It does not authorize any drop or file deletion.
