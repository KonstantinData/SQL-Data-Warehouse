/* Publish stable semantic-model views for inventory analysis. */
USE DataWarehouse;
GO

CREATE OR ALTER VIEW gold.dim_inventory_locations
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY warehouse_code) AS warehouse_key,
    warehouse_code,
    warehouse_name,
    country_code
FROM silver.inventory_warehouse_map
WHERE is_active = 1;
GO

CREATE OR ALTER VIEW gold.fact_inventory_snapshots
AS
SELECT
    s.source_row_id AS inventory_snapshot_id,
    s.snapshot_date,
    w.warehouse_key,
    p.product_key,
    s.on_hand_qty,
    s.reserved_qty,
    s.available_qty,
    s.reorder_point_qty,
    s.unit_cost,
    s.inventory_value,
    s.currency_code,
    s.stock_status,
    s.extracted_at_utc
FROM silver.inventory_snapshot s
INNER JOIN gold.dim_inventory_locations w
    ON w.warehouse_code = s.warehouse_code
INNER JOIN gold.dim_products p
    ON p.product_id = s.product_id
   AND UPPER(p.product_number) = s.product_number;
GO
