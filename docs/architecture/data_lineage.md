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
    participant CI
    participant BI as Power BI
    Runner->>DB: Idempotent bootstrap and procedure deployment
    Runner->>DB: Register audited source version and acquire lock
    DB->>Files: Stage six CRM/ERP files
    DB->>DB: Validate, quarantine, atomically publish Bronze
    DB->>DB: Clean and atomically publish complete Silver
    DB->>DB: Persist Gold dimensions, fact, and indexes
    DB->>Files: Load and validate seventh Inventory snapshot file
    DB->>DB: Publish Inventory Silver and Gold views
    CI->>DB: Run fail-closed quality contracts and negative tests
    BI->>DB: Import curated Gold datasets after successful gates
```

The public SQLCMD/Python runner performs publication and reports end-to-end
batch status; it does not execute the repository's full quality-test suite.
Those fail-closed contracts are executed by CI or explicitly by an operator
before a governed Power BI release.

## Business rules

- Customers: reject missing IDs/keys, select the latest deterministic customer record, standardize domains, enrich from ERP, flag future create dates, and hash last names in Gold.
- Products: validate IDs/keys/cost/dates, preserve product versions, derive SCD2 effective intervals, enrich categories, and enforce at most one current version per product number; a retired product may have none.
- Sales: validate required keys, dates, sequence, and measures; normalize positive price and recompute sales as quantity times normalized price; preserve the accepted order/product grain; resolve customer/product/date surrogate keys; use Unknown members rather than silently dropping unresolved rows.
- Inventory: normalize source and warehouse/product identifiers, map only controlled warehouses and the product version effective on the snapshot date, reject invalid quantities/currency/mapping, deduplicate by extraction timestamp, and derive availability, stock status, and value.

## Audit lineage

`control.pipeline_batch` identifies the core source version, core watermark, status, restart relation, and error. It does not persist `SnapshotAsOf` or a separate Inventory source version. `control.pipeline_step` records Gold and Inventory aggregate results on the end-to-end batch. `control.load_reject` records durable CRM/ERP source, row reference, business key, rule, raw evidence, and remediation message. `control.load_watermark` records the last successfully published Silver delivery per core source; it can point to a batch that later failed in Gold or Inventory.

Inventory uses its own generated `load_batch_id` in Bronze/Silver and exposes
the current accepted and rejected snapshot through
`silver.inventory_snapshot` and `silver.inventory_snapshot_reject`. Both are
replaced by the next Inventory load. The control batch retains aggregate
Inventory step counts and errors, but not a durable row-level Inventory reject
history or an independently governed Inventory version/watermark.

This documentation is repository-derived. Before deprecation or production change, combine it with SQL Server catalog dependencies, job/gateway inventories, external BI lineage, owner confirmation, and an observation window.
