# Dependency analysis

## Reproducible inventory

Run `python scripts/analysis/repository_analysis.py --check --format markdown` to reproduce the static inventory. At this integrated reference revision it identifies seven CSV sources, the `DataWarehouse` database, seven procedure definitions, 34 table definitions, and two Inventory view definitions. The duplicate `gold.usp_load_gold` and Gold table definitions are intentional: modular scripts and the historical self-contained compatibility entry point implement the same contract.

## Direct runtime dependencies

| Subject | Reads | Writes/defines |
| --- | --- | --- |
| `control.run_pipeline` | batch/watermark state, runtime procedures | `control.pipeline_batch`, `control.pipeline_step`, `control.load_watermark` |
| `bronze.load_bronze` | six CRM/ERP CSV files | six `bronze.*` core tables and `control.load_reject` |
| `silver.load_silver` | six Bronze tables, reject rules | six `silver.*` core tables |
| `gold.usp_load_gold` | six Silver tables | `gold.dim_customers`, `gold.dim_products`, `gold.dim_date`, `gold.fact_sales` |
| `bronze.load_inventory_snapshot` | Inventory CSV | `bronze.inventory_snapshot_stage`, `bronze.inventory_snapshot_raw` |
| `silver.load_inventory_snapshot` | Inventory raw, warehouse map, Gold product | `silver.inventory_snapshot`, `silver.inventory_snapshot_reject` |
| Inventory Gold views | Inventory Silver and shared product dimension | `gold.dim_inventory_locations`, `gold.fact_inventory_snapshots` |
| Power BI semantic model | curated Gold core tables and Inventory views; quality/audit queries | imported semantic tables, measures, RLS, reports |

## Entrypoint matrix

| Phase | SQLCMD | Python wrapper | CI |
| --- | :---: | :---: | :---: |
| non-destructive bootstrap/control | yes | same SQLCMD file | yes |
| audited CRM/ERP Bronze + Silver | yes | yes | yes |
| physical Gold model | yes | yes | yes |
| Inventory onboarding | yes | yes | yes |
| positive quality/model contracts | operator-selectable | operator-selectable | yes |
| targeted negative self-tests | no | no | yes |
| million-row benchmark | opt-in | opt-in | excluded from standard CI |

## External dependencies

| Dependency | Scope |
| --- | --- |
| SQL Server 2022 | reviewed runtime and CI reference |
| SQLCMD | includes, variables, failure exit status |
| Python 3.10+ | orchestration and standard-library validators |
| Docker | isolated local/CI SQL Server |
| Power BI Desktop | final open/save/refresh/render/RLS/accessibility gate |
| `sqlalchemy`, `pandas`, `pyodbc` | optional notebooks only |

## Legacy and removal impact

The generated `logs/dbt.log` and empty Gold placeholder were removed after reference and replacement checks. The copy-named notebook remains because it contains unique content. The standalone cleansing scripts and historical file name `create_gold_views.sql` remain compatibility surfaces; removal requires reference search, runtime catalog evidence, external-consumer confirmation, observation, rollback plan, and owner approval. Follow `docs/legacy/deprecation_and_removal.md`.

Static analysis cannot discover SQL Agent jobs, Power BI Service lineage, gateway bindings, external notebooks, permissions, or ad hoc users. Query `sys.sql_expression_dependencies`, job metadata, and consumer inventories before approving a breaking change.
