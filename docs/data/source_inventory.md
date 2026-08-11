# Synthetic inventory snapshot source

## Status and scope

This package is a production-oriented reference implementation built with fictional synthetic data. It demonstrates a complete additional source-onboarding path integrated after the repository's CRM/ERP Gold model. It is not a production deployment, does not process real operational inventory, and does not claim production operating experience.

The source represents full inventory snapshots exported by a fictional warehouse management system. The analytical use case is stock visibility by snapshot date, warehouse, and product. Incremental ingestion, change data capture, scheduling, alerting, retention, and Power BI Service deployment remain outside this package; the source-controlled Power BI model is implemented.

## Source analysis and contract

The authoritative machine-readable contract is `datasets/source_inventory/source_contract.json` (contract version 1.1.0); the deterministic fixture is `datasets/source_inventory/inventory_snapshots.csv`.

| Property | Contract |
|---|---|
| Encoding | UTF-8; the bounded fixture is ASCII-compatible for cross-platform SQL Server `BULK INSERT` |
| Delimiter | Comma |
| Header | Required, exact order from the JSON contract |
| Quoting | Not supported by this bounded loader; fixture values contain no commas or quotes |
| Date | ISO `YYYY-MM-DD` |
| UTC timestamp | ISO-8601 ending in `Z` |
| Business grain | One row per `snapshot_date`, normalized `warehouse_code`, and `product_id` |
| Duplicate survivor | Latest `extracted_at_utc`, then greatest `source_row_id` |
| Schema drift | The static gate detects drift in the committed fixture. Runtime loading is positional and rejects many shape/count failures, but it cannot detect every same-width header rename or reorder. |

Text is trimmed. `source_system`, warehouse code, product number, and currency are uppercased. Only `SYNTHETIC_WMS` and `EUR` are accepted. Quantities and unit cost must be non-negative, and reserved quantity cannot exceed on-hand quantity.

Product mapping follows the structured `mapping.product_rule`: the `product_id` and normalized `product_number` business key must match exactly, `snapshot_date` must satisfy the half-open Gold effective interval, and cardinality must be exactly one non-Unknown Product version. This prevents product-number fan-out and assignment to the wrong SCD2 version. Warehouse mapping is explicit in `silver.inventory_warehouse_map` and never inferred from a display name.

### Deterministic fixture outcomes

| Outcome | Expected rows |
|---|---:|
| Bronze raw | 14 |
| Silver accepted | 10 |
| Silver rejected | 4 |
| Gold fact | 10 |

The four intentional rejects are one superseded duplicate, one negative on-hand quantity, one reservation greater than on-hand quantity, and one unmapped product. `INV-0009` deliberately contains surrounding whitespace and lowercase codes and must be accepted after normalization.

## Artifacts and lineage

| Artifact | Responsibility |
|---|---|
| `datasets/source_inventory/inventory_snapshots.csv` | Safe deterministic source fixture |
| `datasets/source_inventory/source_contract.json` | Machine-readable source and outcome contract |
| `scripts/source_inventory/00_create_objects.sql` | Isolated Bronze/Silver objects and warehouse crosswalk |
| `scripts/source_inventory/10_load_bronze.sql` | Atomic raw CSV loader definition |
| `scripts/source_inventory/20_transform_silver.sql` | Parsing, normalization, mapping, deduplication, and quarantine |
| `scripts/source_inventory/30_create_gold_views.sql` | Stable semantic-model views |
| `scripts/source_inventory/run_source_inventory.sql` | Local SQLCMD entrypoint |
| `scripts/source_inventory/run_source_inventory_ci.sql` | Container-path SQLCMD entrypoint |
| `tests/source_inventory/quality_checks.sql` | Runtime data-quality gate |
| `tests/source_inventory/run_tests.sql` | Double-run idempotency and runtime test entrypoint |
| `tests/source_inventory/run_tests_ci.sql` | Container-path double-run and quality entrypoint |
| `tests/source_inventory/capture_run_state.sql` | First-run deterministic business-row snapshot |
| `tests/source_inventory/compare_run_state.sql` | Bidirectional second-run rowset comparison |
| `tests/source_inventory/test_static_contract.py` | SQL-Server-independent contract verification |

```text
inventory_snapshots.csv
  -> bronze.inventory_snapshot_stage
  -> bronze.inventory_snapshot_raw
  -> silver.inventory_snapshot OR silver.inventory_snapshot_reject
  -> gold.fact_inventory_snapshots
       -> gold.dim_products
       -> gold.dim_inventory_locations
```

Bronze preserves all source fields as text before conversion. Silver uses `TRY_CONVERT`, applies a single terminal reason to every rejected Bronze row, and proves `Bronze = accepted + rejected` before committing. Each loader stage uses `XACT_ABORT`, an explicit transaction, and `THROW`; a failed stage does not silently succeed.

## Field mapping

| Source field | Silver/Gold field | Rule |
|---|---|---|
| `source_row_id` | `source_row_id` / `inventory_snapshot_id` | Trim; required and unique in accepted rows |
| `source_system` | `source_system` | Trim, uppercase, require `SYNTHETIC_WMS` |
| `snapshot_date` | `snapshot_date` | `TRY_CONVERT(DATE, ..., 23)` |
| `warehouse_code` | `warehouse_code` -> `warehouse_key` | Trim, uppercase, exact active crosswalk match |
| `warehouse_name` | canonical warehouse name | Source label is retained in Bronze; Silver uses the controlled crosswalk name |
| `product_id`, `product_number` | `product_id` -> `product_key` | Exact pair plus `snapshot_date` must identify one effective Gold product version |
| quantities | typed quantity measures | Parse as integer; validate ranges; derive `available_qty` |
| `reorder_point_qty` | reorder/status fields | Derive `OUT_OF_STOCK`, `LOW_STOCK`, or `AVAILABLE` |
| `unit_cost` | `unit_cost`, `inventory_value` | Parse decimal; derive on-hand value |
| `currency_code` | `currency_code` | Normalize and require `EUR` |
| `extracted_at_utc` | `extracted_at_utc` | Parse ISO-8601 UTC; used in duplicate precedence |

## Execution and verification

Prerequisites are SQL Server 2022 or a compatible instance, an existing `DataWarehouse` database with the `bronze`, `silver`, and `gold` schemas, and the physical `gold.dim_products` table populated by the core pipeline. The canonical `scripts/run_pipeline.sql` performs these steps automatically. For an isolated Inventory rerun, pass the `BasePath` SQLCMD variable from the repository root:

```sql
sqlcmd -S localhost -d DataWarehouse -E -b -i scripts/source_inventory/run_source_inventory.sql -v BasePath="D:\path\to\SQL-Data-Warehouse\datasets"
```

`BULK INSERT` resolves `BasePath` on the SQL Server host, not on the SQLCMD client. The SQL Server service account must be able to read the path. The container-oriented entrypoint uses `/workspace` and `/datasets`.

Run the static contract gate without SQL Server:

```text
python -m unittest discover -s tests/source_inventory -p "test_*.py"
```

Run the SQL runtime and idempotency gate in SQLCMD mode after the core model exists:

```sql
:r .\tests\source_inventory\run_tests.sql
```

The runtime gate checks object existence, fixed fixture counts, reconciliation, exact source-row reject reasons, normalized values, unique Silver and Gold grains, warehouse/product keys, quantity/status/value invariants, deterministic totals, and bidirectionally identical Silver, reject, and Gold business rowsets after a second full refresh. In the documented container topology, use `/workspace/tests/source_inventory/run_tests_ci.sql`.

## Central integration

Inventory is included after the physical Gold model by `scripts/run_pipeline.sql`, the Python wrapper, and `scripts/ci/run_ci_pipeline.sql`. CI executes the double-run Inventory rowset comparison and quality gate. The safe bootstrap is non-destructive; the separate guarded development reset removes all schemas only on explicit confirmation.

The canonical local SQLCMD include is:

```sql
:r .\scripts\source_inventory\run_source_inventory.sql
```

## Power BI semantic-model hook

The TMDL model includes `Inventory Snapshots` and `Inventory Locations`, relates them to effective-dated Product rows by persisted surrogate key and to Date, and defines explicit Inventory quantity, value, and reorder measures. No Power BI Service deployment is present.

| Object | Grain/key | Relationship | Filter direction |
|---|---|---|---|
| `gold.dim_products` | `product_key` | 1 -> many inventory fact | Single |
| `gold.dim_inventory_locations` | `warehouse_key` | 1 -> many inventory fact | Single |
| `gold.fact_inventory_snapshots` | snapshot date + warehouse key + product key | fact table | From dimensions |

Snapshot measures are semi-additive over time: a current-stock card must filter to one snapshot date (normally the latest) and must not sum multiple snapshots. `snapshot_date` uses the shared Power BI Date table.

## Limitations and productionization

- Full refresh only; no CDC, incremental partitions, late-arrival policy, or history-retention policy.
- CSV parsing is deliberately bounded and does not support quoted delimiters.
- Exact header names/order, a nonblank source warehouse label, and the fixture's trailing `Z` timestamp convention are contract/static-fixture requirements; the runtime parser is positional and canonicalizes the warehouse name from the controlled mapping.
- Source authentication, encrypted transport, access control, operational monitoring, alerting, SLAs, backup, and disaster recovery are not implemented.
- The small synthetic fixture does not establish production performance or scalability.
- Core Gold keys are persisted surrogate keys; Inventory location keys remain deterministic view keys for the bounded reference mapping.
- Gateway binding, RLS authorization for Inventory, scheduling, and release approval remain environment responsibilities.
