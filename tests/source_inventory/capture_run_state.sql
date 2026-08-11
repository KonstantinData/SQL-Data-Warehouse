/* Capture complete deterministic business rowsets after the first run. */
USE DataWarehouse;
GO

DROP TABLE IF EXISTS #source_inventory_silver_run_one;
DROP TABLE IF EXISTS #source_inventory_reject_run_one;
DROP TABLE IF EXISTS #source_inventory_gold_run_one;

SELECT
    source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
    product_id, product_number, on_hand_qty, reserved_qty, available_qty,
    reorder_point_qty, unit_cost, inventory_value, currency_code, stock_status,
    extracted_at_utc
INTO #source_inventory_silver_run_one
FROM silver.inventory_snapshot;

SELECT
    bronze_row_id, source_row_id, reason_code, source_system, snapshot_date,
    warehouse_code, product_id, product_number, on_hand_qty, reserved_qty
INTO #source_inventory_reject_run_one
FROM silver.inventory_snapshot_reject;

SELECT
    inventory_snapshot_id, snapshot_date, warehouse_key, product_key,
    on_hand_qty, reserved_qty, available_qty, reorder_point_qty, unit_cost,
    inventory_value, currency_code, stock_status, extracted_at_utc
INTO #source_inventory_gold_run_one
FROM gold.fact_inventory_snapshots;
GO
