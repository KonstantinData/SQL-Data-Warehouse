/* Cleanse, validate, map, deduplicate, and quarantine inventory rows. */
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_inventory_snapshot
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID('gold.dim_products', 'V') IS NULL AND OBJECT_ID('gold.dim_products', 'U') IS NULL
        THROW 51020, 'source_inventory requires gold.dim_products before Silver mapping.', 1;

    IF NOT EXISTS (SELECT 1 FROM bronze.inventory_snapshot_raw)
        THROW 51021, 'source_inventory cannot transform an empty Bronze source.', 1;

    DROP TABLE IF EXISTS #prepared_inventory;
    DROP TABLE IF EXISTS #classified_inventory;

    ;WITH normalized AS (
        SELECT
            r.bronze_row_id,
            r.load_batch_id,
            NULLIF(TRIM(r.source_row_id), N'') AS source_row_id_clean,
            COUNT(*) OVER (PARTITION BY NULLIF(TRIM(r.source_row_id), N'')) AS source_row_id_count,
            UPPER(NULLIF(TRIM(r.source_system), N'')) AS source_system_clean,
            TRY_CONVERT(DATE, NULLIF(TRIM(r.snapshot_date), N''), 23) AS snapshot_date_typed,
            UPPER(NULLIF(TRIM(r.warehouse_code), N'')) AS warehouse_code_clean,
            NULLIF(TRIM(r.warehouse_name), N'') AS warehouse_name_clean,
            TRY_CONVERT(INT, NULLIF(TRIM(r.product_id), N'')) AS product_id_typed,
            UPPER(NULLIF(TRIM(r.product_number), N'')) AS product_number_clean,
            TRY_CONVERT(INT, NULLIF(TRIM(r.on_hand_qty), N'')) AS on_hand_qty_typed,
            TRY_CONVERT(INT, NULLIF(TRIM(r.reserved_qty), N'')) AS reserved_qty_typed,
            TRY_CONVERT(INT, NULLIF(TRIM(r.reorder_point_qty), N'')) AS reorder_point_qty_typed,
            TRY_CONVERT(DECIMAL(18,2), NULLIF(TRIM(r.unit_cost), N'')) AS unit_cost_typed,
            UPPER(NULLIF(TRIM(r.currency_code), N'')) AS currency_code_clean,
            CASE
                WHEN RIGHT(UPPER(NULLIF(TRIM(REPLACE(r.extracted_at_utc, CHAR(13), N'')), N'')), 1) = N'Z' THEN 1
                ELSE 0
            END AS extracted_at_has_utc_suffix,
            TRY_CONVERT(DATETIME2(0), REPLACE(NULLIF(TRIM(r.extracted_at_utc), N''), N'Z', N''), 126) AS extracted_at_utc_typed,
            r.source_row_id, r.source_system, r.snapshot_date, r.warehouse_code,
            r.product_id, r.product_number, r.on_hand_qty, r.reserved_qty
        FROM bronze.inventory_snapshot_raw r
    ), mapped AS (
        SELECT
            n.*,
            wm.warehouse_name AS mapped_warehouse_name,
            (SELECT COUNT(*)
             FROM gold.dim_products dp
             WHERE dp.product_id = n.product_id_typed
               AND UPPER(dp.product_number) = n.product_number_clean) AS product_match_count
        FROM normalized n
        LEFT JOIN silver.inventory_warehouse_map wm
          ON wm.warehouse_code = n.warehouse_code_clean
         AND wm.is_active = 1
    )
    SELECT
        m.*,
        CASE
            WHEN m.source_row_id_clean IS NULL THEN N'MISSING_SOURCE_ROW_ID'
            WHEN LEN(m.source_row_id_clean) > 30 THEN N'SOURCE_ROW_ID_TOO_LONG'
            WHEN m.source_row_id_count > 1 THEN N'DUPLICATE_SOURCE_ROW_ID'
            WHEN m.source_system_clean <> N'SYNTHETIC_WMS' OR m.source_system_clean IS NULL THEN N'UNSUPPORTED_SOURCE_SYSTEM'
            WHEN LEN(m.source_system_clean) > 30 THEN N'SOURCE_SYSTEM_TOO_LONG'
            WHEN m.snapshot_date_typed IS NULL THEN N'INVALID_SNAPSHOT_DATE'
            WHEN m.warehouse_code_clean IS NULL THEN N'MISSING_WAREHOUSE_CODE'
            WHEN LEN(m.warehouse_code_clean) > 30 THEN N'WAREHOUSE_CODE_TOO_LONG'
            WHEN m.mapped_warehouse_name IS NULL THEN N'WAREHOUSE_MAPPING_NOT_FOUND'
            WHEN m.warehouse_name_clean IS NULL THEN N'MISSING_WAREHOUSE_NAME'
            WHEN LEN(m.warehouse_name_clean) > 100 THEN N'WAREHOUSE_NAME_TOO_LONG'
            WHEN m.product_id_typed IS NULL THEN N'INVALID_PRODUCT_ID'
            WHEN m.product_number_clean IS NULL THEN N'MISSING_PRODUCT_NUMBER'
            WHEN LEN(m.product_number_clean) > 50 THEN N'PRODUCT_NUMBER_TOO_LONG'
            WHEN m.product_match_count = 0 THEN N'PRODUCT_MAPPING_NOT_FOUND'
            WHEN m.product_match_count > 1 THEN N'PRODUCT_MAPPING_AMBIGUOUS'
            WHEN m.on_hand_qty_typed IS NULL THEN N'INVALID_ON_HAND_QTY'
            WHEN m.on_hand_qty_typed < 0 THEN N'NEGATIVE_ON_HAND_QTY'
            WHEN m.reserved_qty_typed IS NULL OR m.reserved_qty_typed < 0 THEN N'INVALID_RESERVED_QTY'
            WHEN m.reserved_qty_typed > m.on_hand_qty_typed THEN N'RESERVED_EXCEEDS_ON_HAND_QTY'
            WHEN m.reorder_point_qty_typed IS NULL OR m.reorder_point_qty_typed < 0 THEN N'INVALID_REORDER_POINT_QTY'
            WHEN m.unit_cost_typed IS NULL OR m.unit_cost_typed < 0 THEN N'INVALID_UNIT_COST'
            WHEN TRY_CONVERT(
                DECIMAL(19,2),
                CONVERT(DECIMAL(18,0), m.on_hand_qty_typed) * m.unit_cost_typed
            ) IS NULL THEN N'INVENTORY_VALUE_OVERFLOW'
            WHEN m.currency_code_clean <> N'EUR' OR m.currency_code_clean IS NULL THEN N'UNSUPPORTED_CURRENCY_CODE'
            WHEN m.extracted_at_has_utc_suffix = 0 THEN N'INVALID_EXTRACTED_AT_UTC_FORMAT'
            WHEN m.extracted_at_utc_typed IS NULL THEN N'INVALID_EXTRACTED_AT_UTC'
            ELSE NULL
        END AS validation_reason
    INTO #prepared_inventory
    FROM mapped m;

    SELECT TOP (0)
        p.*,
        CAST(NULL AS NVARCHAR(60)) AS reason_code
    INTO #classified_inventory
    FROM #prepared_inventory p;

    INSERT INTO #classified_inventory
    SELECT p.*, p.validation_reason
    FROM #prepared_inventory p
    WHERE p.validation_reason IS NOT NULL;

    ;WITH valid_ranked AS (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.snapshot_date_typed, p.warehouse_code_clean, p.product_id_typed
                ORDER BY p.extracted_at_utc_typed DESC, p.source_row_id_clean DESC, p.bronze_row_id DESC
            ) AS duplicate_rank
        FROM #prepared_inventory p
        WHERE p.validation_reason IS NULL
    )
    INSERT INTO #classified_inventory
    SELECT
        v.bronze_row_id, v.load_batch_id, v.source_row_id_clean, v.source_row_id_count, v.source_system_clean,
        v.snapshot_date_typed, v.warehouse_code_clean, v.warehouse_name_clean,
        v.product_id_typed, v.product_number_clean, v.on_hand_qty_typed,
        v.reserved_qty_typed, v.reorder_point_qty_typed, v.unit_cost_typed,
        v.currency_code_clean, v.extracted_at_has_utc_suffix, v.extracted_at_utc_typed, v.source_row_id,
        v.source_system, v.snapshot_date, v.warehouse_code, v.product_id,
        v.product_number, v.on_hand_qty, v.reserved_qty, v.mapped_warehouse_name,
        v.product_match_count, v.validation_reason,
        CASE WHEN v.duplicate_rank > 1 THEN N'DUPLICATE_SUPERSEDED' ELSE NULL END
    FROM valid_ranked v;

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM silver.inventory_snapshot;
        DELETE FROM silver.inventory_snapshot_reject;

        INSERT INTO silver.inventory_snapshot_reject (
            bronze_row_id, source_row_id, reason_code, source_system, snapshot_date,
            warehouse_code, product_id, product_number, on_hand_qty, reserved_qty, load_batch_id
        )
        SELECT
            bronze_row_id, source_row_id, reason_code, source_system, snapshot_date,
            warehouse_code, product_id, product_number, on_hand_qty, reserved_qty, load_batch_id
        FROM #classified_inventory
        WHERE reason_code IS NOT NULL;

        INSERT INTO silver.inventory_snapshot (
            source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
            product_id, product_number, on_hand_qty, reserved_qty, reorder_point_qty,
            unit_cost, currency_code, stock_status, extracted_at_utc, load_batch_id
        )
        SELECT
            source_row_id_clean,
            source_system_clean,
            snapshot_date_typed,
            warehouse_code_clean,
            mapped_warehouse_name,
            product_id_typed,
            product_number_clean,
            on_hand_qty_typed,
            reserved_qty_typed,
            reorder_point_qty_typed,
            unit_cost_typed,
            currency_code_clean,
            CASE
                WHEN on_hand_qty_typed - reserved_qty_typed = 0 THEN N'OUT_OF_STOCK'
                WHEN on_hand_qty_typed - reserved_qty_typed <= reorder_point_qty_typed THEN N'LOW_STOCK'
                ELSE N'AVAILABLE'
            END,
            extracted_at_utc_typed,
            load_batch_id
        FROM #classified_inventory
        WHERE reason_code IS NULL;

        IF (SELECT COUNT(*) FROM bronze.inventory_snapshot_raw)
           <> (SELECT COUNT(*) FROM silver.inventory_snapshot)
              + (SELECT COUNT(*) FROM silver.inventory_snapshot_reject)
            THROW 51022, 'Inventory reconciliation failed during Silver load.', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
