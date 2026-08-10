# Data catalog

All committed records are synthetic. Operational metadata uses UTC unless explicitly stated.

## Control plane

| Object | Grain/key | Purpose |
| --- | --- | --- |
| `control.pipeline_batch` | one execution attempt, `batch_id` | source version/watermark, restart relation, status, timestamps, durable error |
| `control.pipeline_step` | one step attempt per batch | source/target, row metrics, watermarks, status, error |
| `control.load_watermark` | pipeline and source | last successful version/watermark/batch |
| `control.load_reject` | rejected rule occurrence | source provenance, row reference, business key, rule, raw evidence, remediation message |
| `control.run_pipeline` | procedure | serialize and coordinate Bronze/Silver publication and audit outcome |

## Core Bronze and Silver

| Bronze object | Silver object | Business grain |
| --- | --- | --- |
| `bronze.crm_cust_info` | `silver.crm_cust_info` | customer version -> latest accepted customer |
| `bronze.crm_prd_info` | `silver.crm_prd_info` | CRM product version |
| `bronze.crm_sales_details` | `silver.crm_sales_details` | order/product sales line |
| `bronze.erp_cust_az12` | `silver.erp_cust_az12` | ERP customer demographic record |
| `bronze.erp_loc_a101` | `silver.erp_loc_a101` | ERP customer location record |
| `bronze.erp_px_cat_g1v2` | `silver.erp_px_cat_g1v2` | ERP category record |

Every core table carries batch/source metadata appropriate to its layer. `bronze.load_bronze` and `silver.load_silver` publish complete snapshots transactionally.

## Physical Gold model

| Object | Grain/key | Important contract |
| --- | --- | --- |
| `gold.dim_customers` | one customer; identity `customer_key` | unique customer ID, Unknown member, hashed last name |
| `gold.dim_products` | one product version; identity `product_key` | unique product ID and product/effective-start, non-overlapping SCD2, one current row |
| `gold.dim_date` | one calendar day; `yyyymmdd` key | contiguous range plus Unknown member |
| `gold.fact_sales` | one accepted order/product line; identity `sales_key` | trusted customer/product/date foreign keys and source-grain uniqueness |
| `gold.usp_load_gold` | procedure | transactional dimensions/fact load with application lock and ambiguity checks |

## Inventory onboarding objects

| Object | Grain/key | Purpose |
| --- | --- | --- |
| `bronze.inventory_snapshot_stage` | transient staged CSV row | positional raw staging |
| `bronze.inventory_snapshot_raw` | one source row ID | persisted normalized raw snapshot |
| `silver.inventory_warehouse_map` | warehouse code | governed warehouse crosswalk |
| `silver.inventory_snapshot` | date, warehouse, product | accepted typed snapshot |
| `silver.inventory_snapshot_reject` | rejected source row | deterministic terminal reason and evidence |
| `gold.dim_inventory_locations` | active warehouse | semantic location view |
| `gold.fact_inventory_snapshots` | accepted snapshot line | product/location keys and inventory measures |
| `bronze.load_inventory_snapshot` | procedure | atomic Inventory Bronze load |
| `silver.load_inventory_snapshot` | procedure | validation, mapping, deduplication, quarantine, publication |

## Isolated performance objects

`performance.fact_sales_benchmark`, `performance.benchmark_metadata`, `performance.benchmark_results`, `performance.benchmark_result_summary`, and `performance.benchmark_run_log` exist only in the opt-in benchmark schema. The standard pipeline does not create or populate them.

## Sensitivity and access

The synthetic customer schema resembles personal data. Gold minimizes surname exposure through hashing, but real deployments still require purpose limitation, lawful basis, minimization, encryption, retention, least privilege, audit review, and accountable approval. Power BI RLS examples do not replace database authorization.
