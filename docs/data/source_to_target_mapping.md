# Source-to-target mapping

## Source contracts

All seven files are synthetic reference fixtures. CRM/ERP loading uses SQL Server CSV parsing and positional mappings; Inventory additionally has `datasets/source_inventory/source_contract.json`. The current committed total is **116,308** data rows.

| Source file | Rows | Bronze target | Silver target | Analytical target |
| --- | ---: | --- | --- | --- |
| `datasets/source_crm/cst_info.csv` | 18,494 | `bronze.crm_cust_info` | `silver.crm_cust_info` | `gold.dim_customers` |
| `datasets/source_crm/prd_info.csv` | 397 | `bronze.crm_prd_info` | `silver.crm_prd_info` | `gold.dim_products` |
| `datasets/source_crm/sales_details.csv` | 60,398 | `bronze.crm_sales_details` | `silver.crm_sales_details` | `gold.fact_sales` |
| `datasets/source_erp/CST_AZ12.csv` | 18,484 | `bronze.erp_cust_az12` | `silver.erp_cust_az12` | customer enrichment |
| `datasets/source_erp/LOC_A101.csv` | 18,484 | `bronze.erp_loc_a101` | `silver.erp_loc_a101` | customer country enrichment |
| `datasets/source_erp/PX_CAT_G1V2.csv` | 37 | `bronze.erp_px_cat_g1v2` | `silver.erp_px_cat_g1v2` | product classification |
| `datasets/source_inventory/inventory_snapshots.csv` | 14 | `bronze.inventory_snapshot_raw` | `silver.inventory_snapshot` or `silver.inventory_snapshot_reject` | `gold.fact_inventory_snapshots` |

## Customer mapping

| Source/Bronze | Silver rule | Gold rule |
| --- | --- | --- |
| `cust_id`, `cust_key` | required, typed, deterministic latest row per ID | persisted `customer_id`; stable identity `customer_key`; Unknown member for unresolved facts |
| names | trim; reject overlength | first name retained; last name stored only as uppercase SHA-256 `last_name_hash` |
| marital/gender | map M/S and M/F to descriptive domains, otherwise `n/a` | CRM gender preferred; ERP fallback; country/birthdate enriched |
| `cust_create_date` | parse date; derive `cust_is_future` | `customer_create_date`, `customer_is_future` |
| ERP `cid` variants | normalize documented keys | 0/1 enrichment chosen deterministically |

## Product mapping

| Source/Bronze | Silver rule | Gold rule |
| --- | --- | --- |
| `prd_id`, `prd_key` | required, typed, trimmed | persisted product-version surrogate key; product number derived from CRM key |
| cost | negative values are quarantined; a missing value preserves Product identity, becomes `0` in Gold, and remains a DQ warning | `decimal(18,2)` analytical cost |
| product line | M/R/S/T mapped to descriptive values | descriptive product line |
| start/end dates | typed and validated | SCD2 `effective_from`, derived `effective_to`, `is_current` |
| ERP category code | normalized exact lookup | category, subcategory, maintenance |

The Gold model preserves all accepted product versions and enforces at most one current row per product number. A product retired from a later full snapshot has no current row; a previously closed version is never silently reopened. Sales resolves the product version valid on the order date; ambiguity fails closed.

## Sales mapping

| Source/Bronze | Silver rule | Gold rule |
| --- | --- | --- |
| order/product/customer keys | required and reconciled; duplicate source grain rejected | `order_number`, `sales_order_line_number`, customer/product surrogate keys |
| order/ship/due integers | validate `yyyymmdd` and chronological sequence | typed dates plus conformed date keys |
| sales/quantity/price | require positive quantity; normalize price to a positive value (derive it from amount/quantity when source price is zero or missing); recalculate sales as quantity times normalized price; reject invalid/nonpositive/overflow results | normalized decimal amount/price, positive quantity, `measure_quality_code` |

Accepted Silver sales are reconciled bidirectionally to `gold.fact_sales`. Unknown dimension members preserve explicitly unresolved references; no inner join silently removes an accepted fact.

## Inventory mapping

| Source field | Silver/Gold rule |
| --- | --- |
| `source_row_id`, source system, timestamp | validate source system, ID, and UTC timestamp; use latest extraction for duplicate precedence |
| snapshot date | ISO date to typed `snapshot_date` and shared Power BI Date relationship |
| warehouse code/name | normalize code; require active `silver.inventory_warehouse_map`; expose `gold.dim_inventory_locations` |
| product ID/number | require an exact pair against the `gold.dim_products` version effective on `snapshot_date`; expose `product_key` |
| quantities | nonnegative; reserved cannot exceed on-hand; derive available quantity and status |
| unit cost/currency | nonnegative snapshot unit cost and EUR; derive inventory value as `on_hand_qty * unit_cost` (not available quantity and not Product master cost) |

## Reconciliation and validation

1. Static gates verify file presence, header width/order, fixture counts, and contract drift.
2. Core runtime reconciles each source row to published Bronze or `control.load_reject`.
3. Silver tests enforce domains, unique grains, dates, references, and exact reference-fixture cardinalities.
4. Gold tests enforce keys, trusted relationships, SCD2 non-overlap, date consistency, and bidirectional fact reconciliation.
5. Inventory tests prove `14 = 10 accepted + 4 rejected`, deterministic reject reasons, Gold totals, and identical second-run business rowsets.
6. Power BI source validation proves curated Gold usage, model relationships, KPI mappings, report definitions, RLS references, and refresh parameters.
