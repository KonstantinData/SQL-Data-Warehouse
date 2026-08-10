# Source-to-target mapping

## Mapping rules

- All files are synthetic upstream learning data; provenance is documented in
  [`attribution.md`](../project/attribution.md).
- `BULK INSERT` maps fields by ordinal position, not header name. Column count
  and order are therefore contract-critical.
- Bronze preserves source values but uses local column names for the customer
  extract.
- “Standard” means the SQLCMD/Python-listed transformation. “CI only” means the
  lightweight `scripts/ci/load_ci_silver.sql` behavior and must not be presented
  as canonical production cleansing.

## File-to-Bronze contracts

| Source file | Data rows | Ordinal source columns | Bronze target |
| --- | ---: | --- | --- |
| `datasets/source_crm/cst_info.csv` | 18,494 | `cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_marital_status`, `cst_gndr`, `cst_create_date` | `bronze.crm_cust_info` as `cust_id`, `cust_key`, `cust_firstname`, `cust_lastname`, `cust_marital_status`, `cust_gender`, `cust_create_date` |
| `datasets/source_crm/prd_info.csv` | 397 | `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt` | `bronze.crm_prd_info` |
| `datasets/source_crm/sales_details.csv` | 60,398 | `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price` | `bronze.crm_sales_details` |
| `datasets/source_erp/CST_AZ12.csv` | 18,484 | `CID`, `BDATE`, `GEN` | `bronze.erp_cust_az12` |
| `datasets/source_erp/LOC_A101.csv` | 18,484 | `CID`, `CNTRY` | `bronze.erp_loc_a101` |
| `datasets/source_erp/PX_CAT_G1V2.csv` | 37 | `ID`, `CAT`, `SUBCAT`, `MAINTENANCE` | `bronze.erp_px_cat_g1v2` |

The repository has 116,294 source rows in total. This is a file inventory fact,
not a claim about rows surviving transformation or Gold joins.

## Customer mapping

| Bronze field | Silver field/rule | Gold field/rule |
| --- | --- | --- |
| `cust_id` | Reject null IDs; retain greatest `cust_create_date` per ID | `customer_id`; `customer_key = ROW_NUMBER() OVER (ORDER BY cust_id)` |
| `cust_key` | Pass through | `customer_number`; join key for both ERP sources |
| `cust_firstname` | `TRIM` | `first_name` |
| `cust_lastname` | `TRIM` | `last_name_hash = SHA2_256` hex; plain name not exposed |
| `cust_marital_status` | M→Married, S→Single, otherwise `n/a` | `marital_status` |
| `cust_gender` | M→Male, F→Female, otherwise `n/a` | CRM value unless `n/a`, then raw CI-loaded ERP `gen` |
| `cust_create_date` | Pass through | `customer_create_date` |
| derived | `cust_is_future = 1` when create date is later than current server time; null→0 | `cust_is_future` |
| default | `dwh_create_date = GETDATE()`; not explicitly inserted | not exposed |

`cust_is_future` is absent from the base Silver DDL and is added by the customer
cleansing script before insert. A tie on `(cust_id, cust_create_date)` has no
secondary ordering rule and can select either row.

### Customer ERP enrichment

| Source/Silver object | Silver behavior by profile | Gold rule |
| --- | --- | --- |
| `bronze.erp_cust_az12` → `silver.erp_cust_az12` | CI-only direct copy of `cid`, `bdate`, `gen` | join `RIGHT(cid, 10) = cust_key`; expose `birth_date`; use `gen` only when CRM gender is `n/a` |
| `bronze.erp_loc_a101` → `silver.erp_loc_a101` | CI-only direct copy of `cid`, `cntry` | join `REPLACE(cid, '-', '') = cust_key`; expose `country` |

The local standard path does not populate these Silver tables. Country and ERP
demographic values are therefore absent outside CI unless a user loads them
manually.

## Product mapping

| Bronze field | Standard Silver rule | Gold field/rule |
| --- | --- | --- |
| `prd_id` | Reject null IDs; retain greatest `prd_start_dt` per ID | `product_id`; `product_key = ROW_NUMBER() OVER (ORDER BY prd_id)` |
| `prd_key` | `TRIM` | `product_number = SUBSTRING(prd_key, 7, LEN(prd_key))`; category ID is first five characters with `-`→`_` |
| `prd_nm` | `TRIM` | `product_name` |
| `prd_cost` | null or negative→0; otherwise pass | `product_cost`; Gold also applies `ISNULL(..., 0)` |
| `prd_line` | M→Mountain, R→Road, S→Other Sales, T→Touring, otherwise trimmed source value | `product_line`; same mapping is repeated defensively in Gold |
| `prd_start_dt` | pass | `product_start_date` |
| `prd_end_dt` | if earlier than start→null; otherwise pass | `product_end_date` |
| default | `dwh_create_date = GETDATE()` | not exposed |

`silver.erp_px_cat_g1v2` is a CI-only direct copy from
`bronze.erp_px_cat_g1v2`. Gold left-joins `id` to the derived category ID and
exposes `category`, `subcategory`, and `maintenance`; unmatched products remain
with null classification.

The CI loader does not call the standard product transformation. It loads every
Bronze product and maps null or non-positive cost to `1`, so CI and standard
product semantics are intentionally not equivalent.

## Sales mapping

| Bronze field | Silver rule | Gold field/rule |
| --- | --- | --- |
| `sls_ord_num` | CI-only pass through after customer/product existence filter | `order_number` |
| `sls_prd_key` | CI-only pass through | inner join to `gold.dim_products.product_number`; expose `product_key` |
| `sls_cust_id` | CI-only pass through | inner join to `gold.dim_customers.customer_id`; expose `customer_key` |
| `sls_order_dt` | CI-only integer pass through | nonzero integer text converted to `DATE` as `order_date` |
| `sls_ship_dt` | CI-only integer pass through | converted to `ship_date` |
| `sls_due_dt` | CI-only integer pass through | converted to `due_date` |
| `sls_sales` | CI-only pass through | `sales_amount` |
| `sls_quantity` | CI-only pass through | `quantity` |
| `sls_price` | CI-only pass through | `price` |
| default | `dwh_create_date = GETDATE()` | not exposed |

Gold conversion is not `TRY_CONVERT`; an invalid nonzero eight-character date
can fail view evaluation. Because Gold uses inner joins, rejected or unmatched
sales disappear from the fact view. Meaningful verification must reconcile
Bronze, Silver, and Gold counts rather than rely only on referential checks over
rows that survived.

## Validation expectations

1. Assert each source exists, has the documented header order, and has data.
2. Reconcile source and Bronze row counts after every full refresh.
3. Assert customer/product deduplication is deterministic or report ties.
4. Enforce exact Silver domains and valid date ranges.
5. Preflight integer dates with `TRY_CONVERT` before Gold consumption.
6. Assert 0/1 enrichment joins, report unmatched categories, and detect fan-out.
7. Assert non-empty Gold outputs and reconcile fact rows to accepted Silver
   sales rows.
