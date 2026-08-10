# Gold Analytical Model

## Status and scope

This repository is a production-oriented reference implementation built with synthetic data. It has not been deployed against a production workload. The model demonstrates durable warehouse design patterns; it is not evidence of a live production deployment or multi-year operating history.

Gold is materialized as a constrained star schema in SQL Server. The retained public object names are `gold.dim_customers`, `gold.dim_products`, `gold.dim_date`, and `gold.fact_sales`.

## Source contracts and assumptions

The model consumes the existing Silver CRM and ERP tables.

- Customer identity is the CRM `cust_id`. `cust_key` is retained as the customer number. ERP demographics match through `RIGHT(erp_cust_az12.cid, 10) = crm_cust_info.cust_key`; location matches after removing hyphens from `erp_loc_a101.cid`.
- Product version identity is CRM `prd_id`. The category identifier is the normalized first five characters of `prd_key`; the analytical product number is the substring after character six.
- A sales line is identified by `(sls_ord_num, sls_prd_key)` in the committed synthetic fixture. The fixture contains no duplicate pair, but the source does not provide a durable order-line identifier. Gold collapses exact Silver duplicates created by version joins and fails if the same identity carries conflicting payloads. A real onboarding must supply or independently validate a durable line identifier.
- Integer date fields are parsed with `TRY_CONVERT` using the `YYYYMMDD` convention. Invalid or zero dates resolve to Date Key `0` and do not remove a fact row.

Measured committed-fixture profile:

- 60,398 raw sales lines, of which the canonical runtime accepts 60,379 after 19 documented date rejects;
- 18,484 customers after the existing Silver latest-record rule;
- 397 raw and accepted CRM product versions across 295 product numbers; two missing costs are retained as explicit zero-cost DQ warnings in Gold;
- 19 rejected sales date issues (18 invalid order dates and one invalid sequence);
- 35 positive sales values that differ from `quantity * price`, plus additional null or nonpositive source measures.

These are reproducible fixture observations, not production-volume claims.

## Grain and keys

| Object | Grain | Warehouse key | Alternate identity |
| --- | --- | --- | --- |
| `gold.dim_customers` | One current Silver CRM customer | `customer_key` identity | `customer_id` |
| `gold.dim_products` | One CRM product version | `product_key` identity | `product_id`; `(product_number, effective_from)` |
| `gold.dim_date` | One calendar date plus Unknown | deterministic `YYYYMMDD`; `0` is Unknown | `calendar_date` |
| `gold.fact_sales` | One validated CRM order/product line | `sales_key` identity | `(order_number, product_number)`; derived `(order_number, sales_order_line_number)` |

Identity keys are stable for retained natural identities across repeated Gold loads in the same database. They are not guaranteed to remain equal after `scripts/init.database.sql` destroys and recreates the database. Date keys are deterministic across database recreation.

Every dimension has a key-`0` Unknown member. Unknown members preserve fact grain when source relationships cannot be resolved; they do not prove source completeness.

## Product version semantics

`gold.dim_products` is a Type 2-style version dimension. Versions use a half-open interval:

```text
[effective_from, effective_to)
```

`effective_from` comes from `prd_start_dt`. `effective_to` is the next version's `effective_from` for the same `product_number`; the final version has no end. A source end date of `9999-12-31` is treated as the explicit open-end sentinel, so that final version remains current with `effective_to = NULL`. This derived boundary prevents overlap even where the raw source end date is inconsistent. The raw cleaned end date remains available as `source_end_date` for lineage.

Because Silver is a complete snapshot, disappearance closes a current product
episode at `SnapshotAsOf`. If the same closed `product_id` later reappears as a
current source row, the Gold load fails closed rather than silently reopening
history; the source must provide a new version identity or an explicitly
governed correction must be applied.

A sales line resolves to a product only when its valid order date falls in exactly one interval. Missing, pre-history, post-history, or ambiguous matches use `product_key = 0`. The runtime/model quality gates report Unknown coverage for every verified run. Falling back to an unsupported earliest or latest product version would leak attributes into historical facts, so the load deliberately does not do that.

## Customer and privacy handling

ERP enrichment is optional and cannot remove a CRM customer. Gold hashes the last name with SHA-256 and does not expose the original last name. This unsalted deterministic hash is pseudonymization for a reference model; it is not anonymization and does not establish GDPR compliance. Real deployments require a documented threat model, access controls, retention rules, and an approved tokenization or key-management design.

## Date dimension

The loader creates a contiguous calendar spanning all valid order, ship, and due dates, expanded by optional procedure parameters when requested. Date Key `0` represents invalid or missing dates. Known members include stable numeric year, quarter, month, day, ISO week, ISO weekday, weekend, and month-start attributes. English names are generated with fixed mappings rather than session-language-dependent `DATENAME` output.

## Measures and quality state

`sales_amount`, `quantity`, and `price` preserve the Silver values, including null, negative, or adjustment-like values. No currency, tax, or accounting precision contract exists in the source.

`measure_quality_code` makes interpretation explicit:

- `VALID`: complete nonnegative values and `sales_amount = quantity * price`;
- `SOURCE_ADJUSTMENT`: complete nonnegative values with a source-provided amount difference;
- `INVALID_SOURCE_MEASURE`: null or structurally invalid source values.

The model never silently recomputes source sales. Analytical consumers must decide whether to include adjustments or invalid rows for a specific use case.

## Constraints and workload indexes

Named primary, alternate, foreign-key, and check constraints enforce object grain, relationships, valid line numbering, nonoverlapping effective ranges, and the quality-code domain. Foreign keys use Unknown members, so referential integrity remains trusted without dropping unmatched facts.

The workload-backed indexes are:

- `IX_dim_products_asof_lookup (product_number, effective_from, effective_to) INCLUDE (product_key)`;
- `IX_fact_sales_order_date (order_date_key) INCLUDE (customer_key, product_key, sales_amount, quantity)`;
- `IX_fact_sales_customer_date (customer_key, order_date_key) INCLUDE (sales_amount, quantity)`;
- `IX_fact_sales_product_date (product_key, order_date_key) INCLUDE (sales_amount, quantity)`.

Partitioning is intentionally absent at the reference scale. It should be introduced only when measured volume, retention, and partition-elimination requirements justify its operational cost.

## Acceptance evidence

Run the repository-root SQLCMD checks after deployment:

```powershell
sqlcmd -b -d DataWarehouse -i .\tests\model_schema_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_data_quality.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_reproducibility.sql
```

The gates validate physical schema, trusted relationships, exact index contracts, fact row preservation, temporal resolution, date integrity, bidirectional source reconciliation, and key stability across repeated loads.
