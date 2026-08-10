/* Compare complete deterministic business rowsets after the second run. */
USE DataWarehouse;
GO

IF EXISTS (
    SELECT * FROM #source_inventory_silver_run_one
    EXCEPT
    SELECT
        source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
        product_id, product_number, on_hand_qty, reserved_qty, available_qty,
        reorder_point_qty, unit_cost, inventory_value, currency_code, stock_status,
        extracted_at_utc
    FROM silver.inventory_snapshot
)
OR EXISTS (
    SELECT
        source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
        product_id, product_number, on_hand_qty, reserved_qty, available_qty,
        reorder_point_qty, unit_cost, inventory_value, currency_code, stock_status,
        extracted_at_utc
    FROM silver.inventory_snapshot
    EXCEPT
    SELECT * FROM #source_inventory_silver_run_one
)
    THROW 51030, 'source_inventory rerun changed the Silver business rowset.', 1;

IF EXISTS (
    SELECT * FROM #source_inventory_reject_run_one
    EXCEPT
    SELECT bronze_row_id, source_row_id, reason_code, source_system, snapshot_date,
           warehouse_code, product_id, product_number, on_hand_qty, reserved_qty
    FROM silver.inventory_snapshot_reject
)
OR EXISTS (
    SELECT bronze_row_id, source_row_id, reason_code, source_system, snapshot_date,
           warehouse_code, product_id, product_number, on_hand_qty, reserved_qty
    FROM silver.inventory_snapshot_reject
    EXCEPT
    SELECT * FROM #source_inventory_reject_run_one
)
    THROW 51031, 'source_inventory rerun changed the reject business rowset.', 1;

IF EXISTS (
    SELECT * FROM #source_inventory_gold_run_one
    EXCEPT
    SELECT inventory_snapshot_id, snapshot_date, warehouse_key, product_key,
           on_hand_qty, reserved_qty, available_qty, reorder_point_qty, unit_cost,
           inventory_value, currency_code, stock_status, extracted_at_utc
    FROM gold.fact_inventory_snapshots
)
OR EXISTS (
    SELECT inventory_snapshot_id, snapshot_date, warehouse_key, product_key,
           on_hand_qty, reserved_qty, available_qty, reorder_point_qty, unit_cost,
           inventory_value, currency_code, stock_status, extracted_at_utc
    FROM gold.fact_inventory_snapshots
    EXCEPT
    SELECT * FROM #source_inventory_gold_run_one
)
    THROW 51032, 'source_inventory rerun changed the Gold business rowset.', 1;
GO
