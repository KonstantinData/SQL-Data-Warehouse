/* Fail-closed runtime checks for the deterministic source_inventory fixture. */
:on error exit

USE DataWarehouse;
GO

SET NOCOUNT ON;

DECLARE @missing_object_count INT = 0;

IF OBJECT_ID('bronze.inventory_snapshot_raw', 'U') IS NULL
BEGIN SET @missing_object_count += 1; PRINT 'ERROR: bronze.inventory_snapshot_raw is missing.'; END;
IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NULL
BEGIN SET @missing_object_count += 1; PRINT 'ERROR: silver.inventory_snapshot is missing.'; END;
IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NULL
BEGIN SET @missing_object_count += 1; PRINT 'ERROR: silver.inventory_snapshot_reject is missing.'; END;
IF OBJECT_ID('gold.dim_inventory_locations', 'V') IS NULL
BEGIN SET @missing_object_count += 1; PRINT 'ERROR: gold.dim_inventory_locations is missing.'; END;
IF OBJECT_ID('gold.fact_inventory_snapshots', 'V') IS NULL
BEGIN SET @missing_object_count += 1; PRINT 'ERROR: gold.fact_inventory_snapshots is missing.'; END;

IF @missing_object_count > 0
    THROW 51031, 'source_inventory required objects are missing.', 1;
GO

SET NOCOUNT ON;
DECLARE @error_count INT = 0;
DECLARE @bronze_batch_id UNIQUEIDENTIFIER;

IF (SELECT COUNT(DISTINCT load_batch_id) FROM bronze.inventory_snapshot_raw) <> 1
BEGIN SET @error_count += 1; PRINT 'ERROR: Bronze inventory rows do not belong to exactly one load batch.'; END;

SELECT @bronze_batch_id = MIN(load_batch_id)
FROM bronze.inventory_snapshot_raw;

IF EXISTS (SELECT 1 FROM silver.inventory_snapshot WHERE load_batch_id <> @bronze_batch_id)
   OR EXISTS (SELECT 1 FROM silver.inventory_snapshot_reject WHERE load_batch_id <> @bronze_batch_id)
BEGIN SET @error_count += 1; PRINT 'ERROR: Silver or reject rows do not reconcile to the current Bronze batch.'; END;

IF OBJECT_ID('bronze.inventory_snapshot_raw', 'U') IS NOT NULL
   AND (SELECT COUNT(*) FROM bronze.inventory_snapshot_raw) <> 14
BEGIN SET @error_count += 1; PRINT 'ERROR: expected 14 Bronze inventory rows.'; END;

IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
   AND (SELECT COUNT(*) FROM silver.inventory_snapshot) <> 10
BEGIN SET @error_count += 1; PRINT 'ERROR: expected 10 accepted Silver inventory rows.'; END;

IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NOT NULL
   AND (SELECT COUNT(*) FROM silver.inventory_snapshot_reject) <> 4
BEGIN SET @error_count += 1; PRINT 'ERROR: expected 4 quarantined inventory rows.'; END;

IF OBJECT_ID('bronze.inventory_snapshot_raw', 'U') IS NOT NULL
   AND OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
   AND OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NOT NULL
   AND (SELECT COUNT(*) FROM bronze.inventory_snapshot_raw)
       <> (SELECT COUNT(*) FROM silver.inventory_snapshot)
          + (SELECT COUNT(*) FROM silver.inventory_snapshot_reject)
BEGIN SET @error_count += 1; PRINT 'ERROR: Bronze rows do not reconcile to accepted plus rejected rows.'; END;

IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NOT NULL
   AND EXISTS (
       SELECT expected.reason_code, expected.expected_count
       FROM (VALUES
           (N'DUPLICATE_SUPERSEDED', 1),
           (N'NEGATIVE_ON_HAND_QTY', 1),
           (N'RESERVED_EXCEEDS_ON_HAND_QTY', 1),
           (N'PRODUCT_MAPPING_NOT_FOUND', 1)
       ) expected(reason_code, expected_count)
       LEFT JOIN (
           SELECT reason_code, COUNT(*) AS actual_count
           FROM silver.inventory_snapshot_reject
           GROUP BY reason_code
       ) actual ON actual.reason_code = expected.reason_code
       WHERE ISNULL(actual.actual_count, 0) <> expected.expected_count
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: reject reason counts do not match the source contract.'; END;

IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM silver.inventory_snapshot_reject
       WHERE reason_code NOT IN (
           N'DUPLICATE_SUPERSEDED', N'NEGATIVE_ON_HAND_QTY',
           N'RESERVED_EXCEEDS_ON_HAND_QTY', N'PRODUCT_MAPPING_NOT_FOUND'
       )
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: unexpected reject reason found.'; END;

IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NOT NULL
   AND (
       EXISTS (
           SELECT source_row_id, reason_code
           FROM (VALUES
               (N'INV-0010', N'DUPLICATE_SUPERSEDED'),
               (N'INV-0011', N'NEGATIVE_ON_HAND_QTY'),
               (N'INV-0012', N'RESERVED_EXCEEDS_ON_HAND_QTY'),
               (N'INV-0013', N'PRODUCT_MAPPING_NOT_FOUND')
           ) expected(source_row_id, reason_code)
           EXCEPT
           SELECT source_row_id, reason_code
           FROM silver.inventory_snapshot_reject
       )
       OR EXISTS (
           SELECT source_row_id, reason_code
           FROM silver.inventory_snapshot_reject
           EXCEPT
           SELECT source_row_id, reason_code
           FROM (VALUES
               (N'INV-0010', N'DUPLICATE_SUPERSEDED'),
               (N'INV-0011', N'NEGATIVE_ON_HAND_QTY'),
               (N'INV-0012', N'RESERVED_EXCEEDS_ON_HAND_QTY'),
               (N'INV-0013', N'PRODUCT_MAPPING_NOT_FOUND')
           ) expected(source_row_id, reason_code)
       )
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: reject source-row mapping does not match the contract.'; END;

IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM silver.inventory_snapshot
       WHERE on_hand_qty < 0 OR reserved_qty < 0 OR reserved_qty > on_hand_qty
          OR reorder_point_qty < 0 OR unit_cost < 0
          OR available_qty <> on_hand_qty - reserved_qty
          OR currency_code <> 'EUR'
          OR stock_status <> CASE
              WHEN available_qty = 0 THEN N'OUT_OF_STOCK'
              WHEN available_qty <= reorder_point_qty THEN N'LOW_STOCK'
              ELSE N'AVAILABLE'
          END
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: Silver quantity, cost, currency, or status invariant failed.'; END;

IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
   AND EXISTS (
       SELECT snapshot_date, warehouse_code, product_id
       FROM silver.inventory_snapshot
       GROUP BY snapshot_date, warehouse_code, product_id
       HAVING COUNT(*) > 1
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: Silver inventory grain is not unique.'; END;

IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM silver.inventory_snapshot
       WHERE source_row_id = N'INV-0009'
         AND source_system = N'SYNTHETIC_WMS'
         AND warehouse_code = N'WH-BER-01'
         AND product_number = N'FR-R92B-58'
         AND currency_code = 'EUR'
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: expected whitespace/case normalization was not applied.'; END;

IF OBJECT_ID('gold.fact_inventory_snapshots', 'V') IS NOT NULL
   AND (SELECT COUNT(*) FROM gold.fact_inventory_snapshots) <> 10
BEGIN SET @error_count += 1; PRINT 'ERROR: expected 10 Gold inventory fact rows.'; END;

IF OBJECT_ID('gold.fact_inventory_snapshots', 'V') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM gold.fact_inventory_snapshots
       WHERE warehouse_key IS NULL OR product_key IS NULL
          OR available_qty <> on_hand_qty - reserved_qty
          OR inventory_value <> CONVERT(DECIMAL(19,2), on_hand_qty * unit_cost)
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: Gold key or arithmetic invariant failed.'; END;

IF OBJECT_ID('gold.fact_inventory_snapshots', 'V') IS NOT NULL
   AND EXISTS (
       SELECT snapshot_date, warehouse_key, product_key
       FROM gold.fact_inventory_snapshots
       GROUP BY snapshot_date, warehouse_key, product_key
       HAVING COUNT(*) > 1
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: Gold inventory fact grain is not unique.'; END;

IF OBJECT_ID('gold.fact_inventory_snapshots', 'V') IS NOT NULL
   AND (
       (SELECT SUM(CONVERT(BIGINT, on_hand_qty)) FROM gold.fact_inventory_snapshots) <> 404
       OR (SELECT SUM(CONVERT(BIGINT, reserved_qty)) FROM gold.fact_inventory_snapshots) <> 69
       OR (SELECT SUM(CONVERT(BIGINT, available_qty)) FROM gold.fact_inventory_snapshots) <> 335
       OR (SELECT SUM(inventory_value) FROM gold.fact_inventory_snapshots) <> CONVERT(DECIMAL(19,2), 4826.00)
   )
BEGIN SET @error_count += 1; PRINT 'ERROR: deterministic Gold fixture totals do not match the contract.'; END;

IF @error_count > 0
    RAISERROR('source_inventory quality checks failed. Violations: %d', 16, 1, @error_count);
ELSE
    PRINT 'source_inventory quality checks passed.';
GO
