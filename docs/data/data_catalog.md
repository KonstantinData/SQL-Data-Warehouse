# Data catalog

## Catalog conventions

All data is synthetic. Bronze and Silver objects are SQL Server heap tables with
no declared primary keys, foreign keys, unique constraints, or indexes. “Logical
key” expresses a quality expectation, not an enforced database constraint.
Gold objects are views. `ROW_NUMBER()` returns `bigint` and its values are
calculated at query time; they are not durable surrogate-key storage.

## Bronze tables

| Object | Grain | Logical key | Columns and SQL types | Purpose |
| --- | --- | --- | --- | --- |
| `bronze.crm_cust_info` | source customer record/version | `cust_id` candidate; duplicates exist in raw input | `cust_id int`; `cust_key nvarchar(50)`; first/last name `nvarchar(50)`; marital `nvarchar(50)`; gender `nvarchar(10)`; create date `date` | ordinal raw customer landing |
| `bronze.crm_prd_info` | source product record/version | `prd_id` candidate | `prd_id int`; key/name/line `nvarchar(50)`; cost `int`; start/end `datetime` | ordinal raw product landing |
| `bronze.crm_sales_details` | source sales-detail row | no enforced or proven unique key | order/product keys `nvarchar(50)`; customer/date/amount/quantity/price fields `int` | ordinal raw sales landing |
| `bronze.erp_cust_az12` | ERP demographic record | `cid` candidate | `cid nvarchar(50)`, `bdate date`, `gen nvarchar(10)` | raw demographic enrichment |
| `bronze.erp_loc_a101` | ERP location record | `cid` candidate | `cid nvarchar(50)`, `cntry nvarchar(50)` | raw country enrichment |
| `bronze.erp_px_cat_g1v2` | ERP category record | `id` candidate | `id`, `cat`, `subcat`, `maintenance` as `nvarchar(50)` | raw product classification |

`bronze.load_bronze(@base_path nvarchar(4000) = N'datasets')` truncates and
loads all six tables. It logs timings and catches errors, but does not rethrow.

## Silver tables

Each Silver table adds `dwh_create_date datetime DEFAULT GETDATE()`. The timestamp
is a load timestamp, not a source event time.

| Object | Grain and logical key | Business content | Current population path |
| --- | --- | --- | --- |
| `silver.crm_cust_info` | latest accepted row per non-null `cust_id` | Bronze customer columns plus `cust_is_future bit` added by the cleansing script | standard customer transform and CI |
| `silver.crm_prd_info` | latest accepted row per non-null `prd_id` in standard path; every Bronze row in CI path | Bronze product columns after cost/line/date rules | standard product transform **or different CI semantics** |
| `silver.crm_sales_details` | accepted source order-line candidate | nine Bronze sales columns | CI-only filtered copy |
| `silver.erp_cust_az12` | one copied ERP demographic row candidate per `cid` | `cid`, `bdate`, `gen` | CI-only direct copy |
| `silver.erp_loc_a101` | one copied ERP location row candidate per `cid` | `cid`, `cntry` | CI-only direct copy |
| `silver.erp_px_cat_g1v2` | one copied category record candidate per `id` | `id`, `cat`, `subcat`, `maintenance` | CI-only direct copy |

## `gold.dim_customers`

**Grain:** one row returned by the latest-record Silver customer selection,
subject to possible enrichment fan-out. **Logical key:** `customer_id`.

| Column | Inferred type | Nullability/meaning |
| --- | --- | --- |
| `customer_key` | `bigint` | query-time row number; unique for one result evaluation, not persisted |
| `customer_id` | `int` | source customer ID; expected non-null after Silver filter |
| `customer_number` | `nvarchar(50)` | cross-system CRM key |
| `first_name` | `nvarchar(50)` | trimmed first name |
| `last_name_hash` | `varchar(64)` | SHA-256 uppercase hex of last name; null when input is null |
| `marital_status` | `nvarchar(50)` | Married, Single, or `n/a` from local rule |
| `gender` | `nvarchar` | cleaned CRM value or ERP fallback; exact length follows expression inference |
| `birth_date` | `date` | ERP value when CI enrichment is loaded |
| `country` | `nvarchar(50)` | ERP country when CI enrichment is loaded |
| `customer_create_date` | `date` | CRM create date |
| `cust_is_future` | `bit` | local data-quality flag |

## `gold.dim_products`

**Grain:** one Silver product row. Standard and CI profiles can produce
different row sets because their Silver loaders differ. **Logical key:**
`product_id` in the standard deduplicated path; `product_number` is the fact join
key but uniqueness is not declared.

| Column | Inferred type | Nullability/meaning |
| --- | --- | --- |
| `product_key` | `bigint` | query-time row number, not persisted |
| `product_id` | `int` | CRM product ID |
| `product_number` | `nvarchar` | substring of CRM product key from character seven |
| `product_name` | `nvarchar(50)` | trimmed standard-path name |
| `product_cost` | `int` | defaulted per active profile |
| `product_line` | `nvarchar` | descriptive mapping or trimmed fallback |
| `product_start_date` | `datetime` | source start timestamp |
| `product_end_date` | `datetime` | source/cleaned end timestamp |
| `category` | `nvarchar(50)` | optional ERP category |
| `subcategory` | `nvarchar(50)` | optional ERP subcategory |
| `maintenance` | `nvarchar(50)` | optional ERP maintenance indicator |

## `gold.fact_sales`

**Grain:** one accepted Silver sales-detail row after inner joins; order number
alone is not proven unique. The calculated dimension keys act as foreign-key
values in the view but no physical constraint exists.

| Column | Inferred type | Meaning |
| --- | --- | --- |
| `order_number` | `nvarchar(50)` | source sales order number |
| `customer_key` | `bigint` | query-time key from `gold.dim_customers` |
| `product_key` | `bigint` | query-time key from `gold.dim_products` |
| `order_date` | `date` | converted nonzero `yyyymmdd` integer |
| `ship_date` | `date` | converted nonzero `yyyymmdd` integer |
| `due_date` | `date` | converted nonzero `yyyymmdd` integer |
| `sales_amount` | `int` | source sales amount |
| `quantity` | `int` | source quantity |
| `price` | `int` | source unit price |

## Sensitivity and use

Names, birth dates, marital status, gender, and country resemble personal-data
attributes, but bundled rows are synthetic learning fixtures. That does not make
the schema safe for real personal data. A real deployment would require a lawful
basis, minimization, access controls, retention, encryption, auditability, and a
review of whether even hashed names are necessary. This repository implements
none of those operational controls.
