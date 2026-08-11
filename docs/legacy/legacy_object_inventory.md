# Legacy-object and artifact inventory

## Current classification

| Candidate | Classification | Decision/evidence |
| --- | --- | --- |
| `logs/dbt.log` | removed generated artifact | no implemented dbt project/runtime dependency; `logs/*.log` ignored |
| `scripts/gold_layer/placeholder` | removed empty artifact | zero bytes, real Gold DDL present, no references |
| `scripts/python_sql_server_connection copy.ipynb` | retained review candidate | not byte-equivalent; contains an additional code cell |
| `scripts/python_sql_server_connection.ipynb` | retained remediation candidate | parameter/output cleanup should preserve unique tutorial content |
| `drawio/warehouse-architecture.drawio.pdf` | retained review candidate | only durable representation currently present |
| `scripts/bronze_layer/bronze-load-bronze.sql` | retained fail-closed compatibility guard | prevents unversioned legacy execution and points to the operational runner |
| `scripts/gold_layer/create_gold_views.sql` | retained compatibility entrypoint | historical name now deploys the physical Gold model |
| standalone customer/product cleansing scripts | retained compatibility/reference surface | canonical runtime uses `silver.load_silver`; external consumers unverified |

## Canonical objects

- database/schemas: `DataWarehouse`, `bronze`, `silver`, `gold`, `control`; optional benchmark schema `performance`;
- runtime: `control.run_pipeline`, `bronze.load_bronze`, `silver.load_silver` and audit/control tables;
- physical Gold: `gold.dim_customers`, `gold.dim_products`, `gold.dim_date`, `gold.fact_sales`, `gold.usp_load_gold`;
- Inventory: Bronze raw/stage, warehouse map, accepted/reject Silver tables, two load procedures, and two Gold views.

## Historical names and possible runtime residuals

Repository history previously used `bronze.crm_cst_info`, `silver.crm_cst_info`, `cst_*` warehouse columns, `bulk_insert_crm_cst_info.sql`, and `cleansing_crm_cst_info.sql`. Current source headers still use `cst_*`; that is an upstream file contract, not a warehouse residual.

An upgraded or manually modified environment may still contain historical objects or external modules. Verify `sys.objects`, module text, permissions, row/size evidence, SQL Agent jobs, Power BI/gateway lineage, notebooks, and owner confirmation before retirement.

## Safe removal order

1. register owner, replacement, evidence, and rollback;
2. migrate external reports/jobs/notebooks;
3. remove consumer facts/views;
4. remove dimensions/providers;
5. remove Silver procedures/tables;
6. remove Bronze loaders/tables;
7. remove source fixtures only when no executable or documentation contract depends on them.

The completed removal of the log and placeholder does not authorize any SQL object removal. Follow `deprecation_and_removal.md` for shared or production-like environments.
