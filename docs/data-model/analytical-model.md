# Gold Analytical Model

## Status and scope

This repository is a production-oriented reference implementation built with synthetic data. It has not been deployed against a production workload. The model demonstrates durable warehouse design patterns; it is not evidence of a live production deployment or multi-year operating history.

The core Gold model is materialized as a constrained star schema in SQL Server. Its retained public table names are `gold.dim_customers`, `gold.dim_products`, `gold.dim_date`, and `gold.fact_sales`. Inventory extends that model through the public views `gold.dim_inventory_locations` and `gold.fact_inventory_snapshots`.

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
| `gold.dim_inventory_locations` | One active controlled warehouse | derived `warehouse_key` in the view | `warehouse_code` |
| `gold.fact_inventory_snapshots` | One accepted snapshot/product/warehouse observation | source `inventory_snapshot_id` | `(snapshot_date, warehouse_key, product_key)` in the committed contract |

Identity keys are stable for retained natural identities across repeated Gold loads in the same database. `scripts/init.database.sql` is a non-destructive bootstrap and does not recreate an existing database. Identity continuity is lost only when the database is deliberately replaced, for example through `scripts/operations/reset_development.sql`; date keys remain deterministic across database recreation.

Every persisted core dimension has a key-`0` Unknown member. Unknown members preserve fact grain when source relationships cannot be resolved; they do not prove source completeness. The Inventory location view contains only active mapped warehouses and has no Unknown row.

## Product version semantics

`gold.dim_products` is a Type 2-style version dimension. Versions use a half-open interval:

```text
[effective_from, effective_to)
```

`effective_from` comes from `prd_start_dt`. For every non-final version, `effective_to` is the next version's `effective_from` for the same `product_number`. A final version with a finite `source_end_date` closes at `source_end_date + 1 day`; a null source end or the `9999-12-31` sentinel remains open with `effective_to = NULL`. The raw cleaned end date remains available as `source_end_date` for lineage.

Because Silver is a complete snapshot, disappearance closes a current product
episode at `SnapshotAsOf` when that date is later than `effective_from`.
Otherwise it closes at `effective_from + 1 day` so the half-open interval stays
valid. If the same closed `product_id` later reappears as a current source row,
the Gold load fails closed rather than silently reopening history; the source
must provide a new version identity or an explicitly governed correction must
be applied. The current batch audit does not persist `SnapshotAsOf`, so an
operator must preserve and reuse it for a linked restart; the runtime cannot
verify that equality by itself.

A sales line resolves to a product only when its valid order date falls in exactly one interval. Missing, pre-history, post-history, or ambiguous matches use `product_key = 0`. The runtime/model quality gates report Unknown coverage for every verified run. Falling back to an unsupported earliest or latest product version would leak attributes into historical facts, so the load deliberately does not do that.

## Customer and privacy handling

ERP enrichment is optional and cannot remove a CRM customer. Gold hashes the last name with SHA-256 and does not expose the original last name. This unsalted deterministic hash is pseudonymization for a reference model; it is not anonymization and does not establish GDPR compliance. Real deployments require a documented threat model, access controls, retention rules, and an approved tokenization or key-management design.

## Date dimension

The loader creates a contiguous calendar spanning all valid order, ship, and due dates, expanded by optional procedure parameters when requested. Date Key `0` represents invalid or missing dates. Known members include stable numeric year, quarter, month, day, ISO week, ISO weekday, weekend, and month-start attributes. English names are generated with fixed mappings rather than session-language-dependent `DATENAME` output.

## Measures and quality state

The canonical Silver runtime accepts only positive quantities and prices. It normalizes price to an absolute value, or derives it as `ABS(source sales) / ABS(quantity)` when source price is null or zero, using widened decimal arithmetic. Silver then recomputes `sales_amount = quantity * normalized price` as `DECIMAL(18,2)` and quarantines invalid, nonpositive, orphaned, date-invalid, or overflowing rows. Gold preserves those accepted normalized Silver values; it does not restore the raw source measures. No currency, tax, or accounting precision contract exists in the source.

`measure_quality_code` retains a defensive schema domain:

- `VALID`: complete nonnegative values and `sales_amount = quantity * price`;
- `SOURCE_ADJUSTMENT`: complete nonnegative values with a source-provided amount difference;
- `INVALID_SOURCE_MEASURE`: null or structurally invalid source values.

Under the canonical Bronze-to-Silver path, published sales satisfy the normalized multiplication rule and therefore reach Gold as `VALID`. `SOURCE_ADJUSTMENT` and `INVALID_SOURCE_MEASURE` protect Gold when it is invoked against noncanonical or manually populated Silver input; they do not describe raw-source anomalies preserved by the current runtime. Raw values and reject reasons belong to Bronze and `control.load_reject`, not the Gold fact.

## Constraints and workload indexes

Named primary, alternate, foreign-key, and check constraints enforce object grain, relationships, valid line numbering, per-row effective-range/current-flag consistency, and the quality-code domain. Source-derived product intervals are constructed from ordered version starts to avoid cross-row overlap, while CI/data-quality queries explicitly detect any overlap in the resulting model. The at-most-one-current-version rule is checked directly by the Gold loader and again by CI/data-quality queries. Neither cross-row rule is a database constraint. Retired products may have no current version. Foreign keys use Unknown members, so referential integrity remains trusted without dropping unmatched facts.

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
sqlcmd -b -d DataWarehouse -i .\tests\model_sentinel_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_decimal_arithmetic.sql
sqlcmd -b -d DataWarehouse -i .\tests\model_scd2_reconciliation.sql
```

The gates validate physical schema, trusted relationships, exact index contracts, fact row preservation, temporal resolution, date integrity, sentinel members, decimal arithmetic, SCD2 retirement/reconciliation, bidirectional source reconciliation, and key stability across repeated loads.
