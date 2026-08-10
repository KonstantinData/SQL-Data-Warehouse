# System architecture

## Scope and status

The repository implements a resettable SQL Server warehouse reference over
synthetic files. It has three logical layers and three non-equivalent execution
profiles. Only the CI profile currently populates every Silver dependency before
creating Gold. The architecture is production-oriented in structure and
verification intent, but it is not a production deployment.

## System context

```mermaid
flowchart LR
    CRM["Synthetic CRM CSV files"] --> LOAD["bronze.load_bronze"]
    ERP["Synthetic ERP CSV files"] --> LOAD
    LOAD --> B["Bronze raw heap tables"]
    B --> S["Silver heap tables<br>cleaned or copied by profile"]
    S --> G["Gold analytical views"]
    G --> Q["SQL consumers<br>ad hoc analysis or future BI"]
    TEST["SQL quality checks"] --> B
    TEST --> S
    TEST --> G
    STATIC["Static repository analysis"] -.-> LOAD
    STATIC -.-> B
    STATIC -.-> S
    STATIC -.-> G
```

No Power BI artifact, deployed dashboard, machine-learning model, external API,
or live CRM/ERP connection is present. Such consumers are possible future uses,
not implemented components.

## Components and responsibilities

| Component | Responsibility | Current implementation |
| --- | --- | --- |
| Database bootstrap | Recreate `DataWarehouse`; create layer schemas | `scripts/init.database.sql` |
| Bronze DDL | Define six raw tables without declared keys | `scripts/bronze_layer/create_table_bronze_layer.sql` |
| Bronze loader | Truncate and ordinal-load all six CSVs through dynamic `BULK INSERT` | `scripts/bronze_layer/bulk_insert_crm_cust_info.sql` |
| Standard Silver DDL | Define six tables with load timestamps | `scripts/silver_layer/create_silver_table_structure.sql` |
| Standard Silver transforms | Deduplicate/standardize customers and products | two `cleansing_*.sql` files |
| CI Silver loader | Populate product, sales, and three ERP tables for CI | `scripts/ci/load_ci_silver.sql`; explicitly lightweight and CI-only |
| Gold presentation | Join/enrich into two dimension views and one fact view | `scripts/gold_layer/create_gold_views.sql` |
| Runtime validation | Diagnose Bronze and enforce selected Silver/Gold conditions | SQL files in `tests/` |
| Static validation | Inventory repository objects, references, files, and documentation | `scripts/analysis/` |

## Warehouse object model

```mermaid
flowchart TB
    subgraph Bronze["Bronze schema"]
        BC["crm_cust_info"]
        BP["crm_prd_info"]
        BS["crm_sales_details"]
        BA["erp_cust_az12"]
        BL["erp_loc_a101"]
        BX["erp_px_cat_g1v2"]
    end
    subgraph Silver["Silver schema"]
        SC["crm_cust_info"]
        SP["crm_prd_info"]
        SS["crm_sales_details"]
        SA["erp_cust_az12"]
        SL["erp_loc_a101"]
        SX["erp_px_cat_g1v2"]
    end
    subgraph Gold["Gold schema - views"]
        DC["dim_customers"]
        DP["dim_products"]
        FS["fact_sales"]
    end
    BC --> SC
    BP --> SP
    BS --> SS
    BA --> SA
    BL --> SL
    BX --> SX
    SC --> DC
    SA --> DC
    SL --> DC
    SP --> DP
    SX --> DP
    SS --> FS
    DC --> FS
    DP --> FS
```

Bronze and Silver are physical heap tables. Gold objects are views, not
materialized tables. `customer_key` and `product_key` are calculated with
`ROW_NUMBER()` at query time; they are not persisted identity values.

## Execution profiles

| Profile | Session model | Silver coverage | Gold | Enforced gate | Assessment |
| --- | --- | --- | --- | --- | --- |
| SQLCMD include runner | One SQLCMD session via `:r` | customer + product only | created | none inside runner | incomplete data path |
| Python runner | New `sqlcmd` process for each file | customer + product list | omitted | none | prototype; default database context cannot bootstrap the full path |
| CI runner | One SQLCMD include session plus containers | all six tables, with CI-specific semantics | created | `quality_checks_ci.sql` | only current end-to-end profile |

The CI loader is not evidence of canonical production cleansing: its product
cost rule differs from the dedicated product transformation, and it copies ERP
records without the richer upstream normalization rules.

## Failure and data-integrity boundaries

- Bootstrap and table DDL are destructive: the database and layer tables are
  dropped/recreated. Use disposable instances only.
- The Bronze procedure truncates each table before load. A mid-batch failure can
  leave a partially refreshed warehouse.
- Its `CATCH` block prints errors but does not rethrow them, so process exit
  status alone is insufficient evidence of a successful load.
- CSV-to-Bronze mapping is positional because `BULK INSERT` has no format file.
  Header spelling does not bind columns.
- No primary keys, foreign keys, unique constraints, indexes, or row-level
  security are declared by the current warehouse DDL.
- Gold joins can hide unmatched facts because `fact_sales` uses inner joins;
  missing dimension matches remove rows rather than surface null keys.

## Deployment and operations model

There is no evidenced production environment. CI provisions a disposable SQL
Server container and runs a test-specific pipeline. The repository has no
versioned in-place migrations, backup/restore workflow, secrets platform,
monitoring, alerting, retention policy, access-control model, or service-level
objective. These are explicit maturity boundaries, not implicit features.

## Architecture evidence

- field-level rules: [`source_to_target_mapping.md`](../data/source_to_target_mapping.md)
- object contracts: [`data_catalog.md`](../data/data_catalog.md)
- runner/object dependencies: [`dependency_analysis.md`](../data/dependency_analysis.md)
- proposed improvements: [`implementation_proposals.md`](../project/implementation_proposals.md)
