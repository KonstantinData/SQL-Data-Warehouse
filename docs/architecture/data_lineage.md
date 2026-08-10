# Data lineage

## Implemented lineage

```mermaid
flowchart LR
    C["cst_info.csv"] --> BC["bronze.crm_cust_info"] --> SC["silver.crm_cust_info"] --> DC["gold.dim_customers"]
    P["prd_info.csv"] --> BP["bronze.crm_prd_info"] --> SP["silver.crm_prd_info"] --> DP["gold.dim_products"]
    F["sales_details.csv"] --> BF["bronze.crm_sales_details"] --> SF["silver.crm_sales_details"] --> GF["gold.fact_sales"]
    A["CST_AZ12.csv"] --> BA["bronze.erp_cust_az12"] --> SA["silver.erp_cust_az12"] --> DC
    L["LOC_A101.csv"] --> BL["bronze.erp_loc_a101"] --> SL["silver.erp_loc_a101"] --> DC
    X["PX_CAT_G1V2.csv"] --> BX["bronze.erp_px_cat_g1v2"] --> SX["silver.erp_px_cat_g1v2"] --> DP
    DP --> GF
    DC --> GF
    DD["gold.dim_date"] --> GF
    I["inventory_snapshots.csv"] --> IR["bronze.inventory_snapshot_raw"] --> IS["silver.inventory_snapshot"] --> IF["gold.fact_inventory_snapshots"]
    IW["silver.inventory_warehouse_map"] --> IL["gold.dim_inventory_locations"] --> IF
    DP --> IF
    GF --> BI["Power BI"]
    IF --> BI
```

## Processing sequence

```mermaid
sequenceDiagram
    participant Runner
    participant DB as SQL Server
    participant Files as Synthetic CSVs
    participant BI as Power BI
    Runner->>DB: Idempotent bootstrap and procedure deployment
    Runner->>DB: Register audited source version and acquire lock
    DB->>Files: Stage six CRM/ERP files
    DB->>DB: Validate, quarantine, atomically publish Bronze
    DB->>DB: Clean and atomically publish complete Silver
    DB->>DB: Persist Gold dimensions, fact, and indexes
    DB->>Files: Load and validate Inventory snapshot
    DB->>DB: Publish Inventory Silver and Gold views
    Runner->>DB: Run fail-closed quality contracts
    BI->>DB: Import curated Gold datasets after successful gates
```

## Business rules

- Customers: reject missing IDs/keys, select the latest deterministic customer record, standardize domains, enrich from ERP, flag future create dates, and hash last names in Gold.
- Products: validate IDs/keys/cost/dates, preserve product versions, derive SCD2 effective intervals, enrich categories, and enforce one current version per product number.
- Sales: validate required keys, dates, sequence, and measures; preserve the accepted order/product grain; resolve customer/product/date surrogate keys; use Unknown members rather than silently dropping unresolved rows.
- Inventory: normalize source and warehouse/product identifiers, map only controlled warehouses and the product version effective on the snapshot date, reject invalid quantities/currency/mapping, deduplicate by extraction timestamp, and derive availability, stock status, and value.

## Audit lineage

`control.pipeline_batch` identifies source version, watermark, status, restart relation, and error. `control.pipeline_step` records attempts and row metrics. `control.load_reject` records source, row reference, business key, rule, raw evidence, and remediation message. `control.load_watermark` records the last successful delivery per core source.

This documentation is repository-derived. Before deprecation or production change, combine it with SQL Server catalog dependencies, job/gateway inventories, external BI lineage, owner confirmation, and an observation window.
