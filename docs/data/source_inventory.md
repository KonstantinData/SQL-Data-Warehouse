# Synthetic inventory snapshot source

## Status and scope

This package is a production-oriented reference implementation built with fictional synthetic data. It demonstrates a complete additional source-onboarding path without modifying the repository's existing CRM/ERP pipeline. It is not a production deployment, does not process real operational inventory, and does not claim production operating experience.

The source represents full inventory snapshots exported by a fictional warehouse management system. The analytical use case is stock visibility by snapshot date, warehouse, and product. Incremental ingestion, change data capture, scheduling, alerting, retention, and a deployed Power BI model are outside this package.

## Source analysis and contract

The authoritative machine-readable contract is `datasets/source_inventory/source_contract.json`; the deterministic fixture is `datasets/source_inventory/inventory_snapshots.csv`.

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

Product mapping intentionally uses the pair `product_id` plus `product_number` against `gold.dim_products`. The existing model contains repeated product numbers across product versions; using the pair prevents a product-number-only fan-out. Warehouse mapping is explicit in `silver.inventory_warehouse_map` and never inferred from a display name.

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
| `product_id`, `product_number` | `product_id` -> `product_key` | Exact pair must identify one existing Gold product |
| quantities | typed quantity measures | Parse as integer; validate ranges; derive `available_qty` |
| `reorder_point_qty` | reorder/status fields | Derive `OUT_OF_STOCK`, `LOW_STOCK`, or `AVAILABLE` |
| `unit_cost` | `unit_cost`, `inventory_value` | Parse decimal; derive on-hand value |
| `currency_code` | `currency_code` | Normalize and require `EUR` |
| `extracted_at_utc` | `extracted_at_utc` | Parse ISO-8601 UTC; used in duplicate precedence |

## Execution and verification

Prerequisites are SQL Server 2019 or newer, an existing `DataWarehouse` database with the `bronze`, `silver`, and `gold` schemas, and the existing `gold.dim_products` view populated by the core pipeline. Run from the repository root in SQLCMD mode:

```sql
:r .\scripts\source_inventory\run_source_inventory.sql
```

Running from the repository root resolves the SQLCMD `:r` includes only. `BULK INSERT` resolves `@base_path = N'datasets'` on the SQL Server host, not on the SQLCMD client; that relative server path must point to the repository datasets and the SQL Server service account must be able to read it. Where it does not, execute the owned definition scripts and call `bronze.load_inventory_snapshot` with an absolute server-visible base path before calling `silver.load_inventory_snapshot` and creating the Gold views. The container-oriented entrypoint expects the repository at `/workspace` and copied datasets at `/datasets`.

Run the static contract gate without SQL Server:

```text
python -m unittest discover -s tests/source_inventory -p "test_*.py"
```

Run the SQL runtime and idempotency gate in SQLCMD mode after the core model exists:

```sql
:r .\tests\source_inventory\run_tests.sql
```

The runtime gate checks object existence, fixed fixture counts, reconciliation, exact source-row reject reasons, normalized values, unique Silver and Gold grains, warehouse/product keys, quantity/status/value invariants, deterministic totals, and bidirectionally identical Silver, reject, and Gold business rowsets after a second full refresh. In the documented container topology, use `/workspace/tests/source_inventory/run_tests_ci.sql`.

## Reserved central integration hooks

These steps are instructions for the integration task; they are not applied by this package.

1. In `scripts/run_pipeline.sql`, after `scripts/gold_layer/create_gold_views.sql`, include:

   ```sql
   :r .\scripts\source_inventory\run_source_inventory.sql
   ```

2. `scripts/orchestrate_pipeline.py` currently stops after Silver. Extend its ordered SQL file list with `scripts/gold_layer/create_gold_views.sql` first and then `scripts/source_inventory/run_source_inventory.sql`. The orchestrator must invoke SQLCMD from the repository root so nested include paths resolve correctly.
3. In `scripts/ci/run_ci_pipeline.sql`, add `:r /workspace/scripts/source_inventory/run_source_inventory_ci.sql` after `/workspace/scripts/gold_layer/create_gold_views.sql`.
4. In `scripts/ci/run_ci_checks.sh`, add a separate `sqlcmd -b` invocation for `/workspace/tests/source_inventory/quality_checks.sql` after the existing central quality-check invocation (or have the integration task add the equivalent include to its SQLCMD quality script).

The package must run after `scripts/init.database.sql`; that script recreates the database and would erase inventory objects installed earlier.

## Power BI semantic-model hook

No PBIX, TMDL, or deployed semantic model is present in this repository. The stable Gold views and this wiring contract are the complete in-scope hook.

| Object | Grain/key | Relationship | Filter direction |
|---|---|---|---|
| `gold.dim_products` | `product_key` | 1 -> many inventory fact | Single |
| `gold.dim_inventory_locations` | `warehouse_key` | 1 -> many inventory fact | Single |
| `gold.fact_inventory_snapshots` | snapshot date + warehouse key + product key | fact table | From dimensions |

Recommended measures sum `on_hand_qty`, `reserved_qty`, `available_qty`, and `inventory_value`. These snapshot measures are semi-additive over time: a current-stock card must filter to one snapshot date (normally the latest) and must not sum multiple snapshots. Connect `snapshot_date` to a separately managed date dimension when one exists; this package does not introduce a competing central date model.

## Limitations and productionization

- Full refresh only; no CDC, incremental partitions, late-arrival policy, or history-retention policy.
- CSV parsing is deliberately bounded and does not support quoted delimiters.
- Exact header names/order, a nonblank source warehouse label, and the fixture's trailing `Z` timestamp convention are contract/static-fixture requirements; the runtime parser is positional and canonicalizes the warehouse name from the controlled mapping.
- Source authentication, encrypted transport, access control, operational monitoring, alerting, SLAs, backup, and disaster recovery are not implemented.
- The small synthetic fixture does not establish production performance or scalability.
- The Gold keys follow existing `ROW_NUMBER` view conventions and are deterministic for the current dimension contents, not durable warehouse surrogate keys across arbitrary dimension rewrites.
- Central pipeline, CI, Python orchestration, and Power BI model integration remain owned by the separate integration task.
