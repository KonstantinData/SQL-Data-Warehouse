/*
================================================================================
Atomic Silver full-snapshot loader
================================================================================
Loads CRM customer/product/sales and all three ERP entities as one transaction.
Validated Product versions are preserved. Gold resolves each Sale to the
effective Product version and prevents fact fan-out with a surrogate key.
Silver publication, six watermarks, and batch completion commit together.
================================================================================
*/

USE DataWarehouse;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
    @batch_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @step_id BIGINT;
    DECLARE @pipeline_name NVARCHAR(128);
    DECLARE @source_version NVARCHAR(255);
    DECLARE @source_watermark BIGINT;
    DECLARE @max_reject_rows BIGINT;
    DECLARE @rejects_before BIGINT;
    DECLARE @rows_read BIGINT;
    DECLARE @rows_published BIGINT;
    DECLARE @rows_rejected BIGINT;
    DECLARE @total_reject_rows BIGINT;
    DECLARE @batch_time_utc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @error_number INT;
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @error_procedure NVARCHAR(256);
    DECLARE @error_line INT;
    DECLARE @error_message NVARCHAR(4000);
    DECLARE @watermark_before BIGINT;

    IF @@TRANCOUNT <> 0
        THROW 51200, 'silver.load_silver cannot run inside a caller-owned transaction.', 1;
    IF ISNULL(APPLOCK_MODE(N'public', N'SQL-Data-Warehouse:operational-pipeline', N'Session'), N'NoLock') <> N'Exclusive'
        THROW 51205, 'silver.load_silver requires the coordinator session lock.', 1;

    SELECT
        @pipeline_name = pipeline_name,
        @source_version = source_version,
        @source_watermark = source_watermark,
        @max_reject_rows = max_reject_rows
    FROM control.pipeline_batch
    WHERE batch_id = @batch_id AND status = 'RUNNING';

    IF @pipeline_name IS NULL
        THROW 51201, 'batch_id must reference a RUNNING pipeline batch.', 1;

    SELECT @watermark_before = MAX(watermark_value)
    FROM control.load_watermark
    WHERE pipeline_name = @pipeline_name;

    IF EXISTS (
        SELECT source_name
        FROM (VALUES
            (N'crm_cust_info', (SELECT COUNT_BIG(*) FROM bronze.crm_cust_info WHERE load_batch_id = @batch_id)),
            (N'crm_prd_info', (SELECT COUNT_BIG(*) FROM bronze.crm_prd_info WHERE load_batch_id = @batch_id)),
            (N'crm_sales_details', (SELECT COUNT_BIG(*) FROM bronze.crm_sales_details WHERE load_batch_id = @batch_id)),
            (N'erp_cust_az12', (SELECT COUNT_BIG(*) FROM bronze.erp_cust_az12 WHERE load_batch_id = @batch_id)),
            (N'erp_loc_a101', (SELECT COUNT_BIG(*) FROM bronze.erp_loc_a101 WHERE load_batch_id = @batch_id)),
            (N'erp_px_cat_g1v2', (SELECT COUNT_BIG(*) FROM bronze.erp_px_cat_g1v2 WHERE load_batch_id = @batch_id))
        ) required(source_name, row_count)
        WHERE row_count = 0
    )
        THROW 51202, 'Every required Bronze source must contain rows for this batch.', 1;

    INSERT control.pipeline_step (
        batch_id, step_name, status, source_name, target_name, watermark_before, watermark_after
    )
    VALUES (
        @batch_id, N'silver.full_snapshot', 'RUNNING',
        N'bronze (six tables)', N'silver (six tables)', @watermark_before, @source_watermark
    );
    SET @step_id = SCOPE_IDENTITY();

    SELECT @rejects_before = COUNT_BIG(*) FROM control.load_reject WHERE batch_id = @batch_id;

    BEGIN TRY
        WITH ranked AS (
            SELECT b.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY cust_id
                       ORDER BY cust_create_date DESC, source_row_number DESC, cust_key DESC
                   ) AS rn
            FROM bronze.crm_cust_info b
            WHERE load_batch_id = @batch_id
              AND cust_id IS NOT NULL
              AND NULLIF(TRIM(cust_key), N'') IS NOT NULL
        )
        SELECT cust_id, TRIM(cust_key) AS cust_key,
               TRIM(cust_firstname) AS cust_firstname,
               TRIM(cust_lastname) AS cust_lastname,
               CASE UPPER(TRIM(cust_marital_status))
                   WHEN 'M' THEN N'Married' WHEN 'S' THEN N'Single' ELSE N'n/a' END AS cust_marital_status,
               CASE UPPER(TRIM(cust_gender))
                   WHEN 'M' THEN N'Male' WHEN 'F' THEN N'Female' ELSE N'n/a' END AS cust_gender,
               cust_create_date,
               CONVERT(BIT, CASE WHEN cust_create_date > CONVERT(DATE, @batch_time_utc) THEN 1 ELSE 0 END) AS cust_is_future
        INTO #silver_cust
        FROM ranked
        WHERE rn = 1;

        INSERT control.load_reject (
            batch_id, step_id, source_name, source_file, source_row_number,
            business_key, column_name, rule_code, raw_value, raw_payload, error_message
        )
        SELECT @batch_id, @step_id, N'crm_prd_info', source_file, source_row_number,
               TRIM(prd_key), N'prd_cost', N'NEGATIVE_PRODUCT_COST', CONVERT(NVARCHAR(100), prd_cost),
               CONCAT_WS(N'|', prd_id, prd_key, prd_nm, prd_line, prd_start_dt),
               N'Product version has a negative cost and is excluded from the published Silver history.'
        FROM bronze.crm_prd_info
        WHERE load_batch_id = @batch_id
          AND NULLIF(TRIM(prd_key), N'') IS NOT NULL
          AND prd_cost < 0;

        SELECT prd_id, TRIM(prd_key) AS prd_key, TRIM(prd_nm) AS prd_nm,
               prd_cost,
               CASE UPPER(TRIM(prd_line))
                   WHEN 'M' THEN N'Mountain' WHEN 'R' THEN N'Road'
                   WHEN 'S' THEN N'Other Sales' WHEN 'T' THEN N'Touring'
                   ELSE COALESCE(NULLIF(TRIM(prd_line), N''), N'n/a') END AS prd_line,
               prd_start_dt,
               CASE WHEN prd_end_dt < prd_start_dt THEN NULL ELSE prd_end_dt END AS prd_end_dt
        INTO #silver_prd
        FROM bronze.crm_prd_info
        WHERE load_batch_id = @batch_id
          AND prd_id IS NOT NULL
          AND NULLIF(TRIM(prd_key), N'') IS NOT NULL
          AND (prd_cost IS NULL OR prd_cost >= 0);

        SELECT b.*,
               TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(b.sls_order_dt, 0)), 112) AS parsed_order_date,
               TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(b.sls_ship_dt, 0)), 112) AS parsed_ship_date,
               TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(b.sls_due_dt, 0)), 112) AS parsed_due_date,
               CASE
                   WHEN b.sls_price IS NULL OR b.sls_price = 0
                       THEN ABS(b.sls_sales) / NULLIF(ABS(b.sls_quantity), 0)
                   ELSE ABS(b.sls_price)
               END AS normalized_price
        INTO #sales_evaluated
        FROM bronze.crm_sales_details b
        WHERE load_batch_id = @batch_id;

        INSERT control.load_reject (
            batch_id, step_id, source_name, source_file, source_row_number,
            business_key, column_name, rule_code, raw_value, raw_payload, error_message
        )
        SELECT @batch_id, @step_id, N'crm_sales_details', e.source_file, e.source_row_number,
               CONCAT(e.sls_ord_num, N'/', e.sls_prd_key),
               CASE
                   WHEN NULLIF(TRIM(e.sls_ord_num), N'') IS NULL THEN N'sls_ord_num'
                   WHEN NULLIF(TRIM(e.sls_prd_key), N'') IS NULL THEN N'sls_prd_key'
                   WHEN e.sls_cust_id IS NULL THEN N'sls_cust_id'
                   WHEN e.parsed_order_date IS NULL THEN N'sls_order_dt'
                   WHEN e.sls_ship_dt <> 0 AND e.parsed_ship_date IS NULL THEN N'sls_ship_dt'
                   WHEN e.sls_due_dt <> 0 AND e.parsed_due_date IS NULL THEN N'sls_due_dt'
                   WHEN e.parsed_ship_date < e.parsed_order_date OR e.parsed_due_date < e.parsed_order_date
                        OR e.parsed_due_date < e.parsed_ship_date THEN N'sales_date_sequence'
                   WHEN e.sls_quantity IS NULL OR e.sls_quantity <= 0 THEN N'sls_quantity'
                   WHEN e.normalized_price IS NULL OR e.normalized_price <= 0 THEN N'sls_price'
                   WHEN NOT EXISTS (SELECT 1 FROM #silver_cust c WHERE c.cust_id = e.sls_cust_id) THEN N'sls_cust_id'
                   ELSE N'sls_prd_key'
               END,
               CASE
                   WHEN NULLIF(TRIM(e.sls_ord_num), N'') IS NULL THEN N'MISSING_ORDER_KEY'
                   WHEN NULLIF(TRIM(e.sls_prd_key), N'') IS NULL THEN N'MISSING_PRODUCT_KEY'
                   WHEN e.sls_cust_id IS NULL THEN N'MISSING_CUSTOMER_KEY'
                   WHEN e.parsed_order_date IS NULL THEN N'INVALID_ORDER_DATE'
                   WHEN e.sls_ship_dt <> 0 AND e.parsed_ship_date IS NULL THEN N'INVALID_SHIP_DATE'
                   WHEN e.sls_due_dt <> 0 AND e.parsed_due_date IS NULL THEN N'INVALID_DUE_DATE'
                   WHEN e.parsed_ship_date < e.parsed_order_date OR e.parsed_due_date < e.parsed_order_date
                        OR e.parsed_due_date < e.parsed_ship_date THEN N'INVALID_DATE_SEQUENCE'
                   WHEN e.sls_quantity IS NULL OR e.sls_quantity <= 0 THEN N'INVALID_QUANTITY'
                   WHEN e.normalized_price IS NULL OR e.normalized_price <= 0 THEN N'INVALID_PRICE'
                   WHEN NOT EXISTS (SELECT 1 FROM #silver_cust c WHERE c.cust_id = e.sls_cust_id) THEN N'ORPHAN_CUSTOMER'
                   ELSE N'ORPHAN_PRODUCT'
               END,
               CASE WHEN e.parsed_order_date IS NULL THEN CONVERT(NVARCHAR(50), e.sls_order_dt) END,
               CONCAT_WS(N'|', e.sls_ord_num, e.sls_prd_key, e.sls_cust_id, e.sls_order_dt,
                         e.sls_ship_dt, e.sls_due_dt, e.sls_sales, e.sls_quantity, e.sls_price),
               N'Sales row failed required-key, date, positive-value, or referential validation.'
        FROM #sales_evaluated e
        WHERE NULLIF(TRIM(e.sls_ord_num), N'') IS NULL
           OR NULLIF(TRIM(e.sls_prd_key), N'') IS NULL
           OR e.sls_cust_id IS NULL
           OR e.parsed_order_date IS NULL
           OR (e.sls_ship_dt <> 0 AND e.parsed_ship_date IS NULL)
           OR (e.sls_due_dt <> 0 AND e.parsed_due_date IS NULL)
           OR e.parsed_ship_date < e.parsed_order_date
           OR e.parsed_due_date < e.parsed_order_date
           OR e.parsed_due_date < e.parsed_ship_date
           OR e.sls_quantity IS NULL OR e.sls_quantity <= 0
           OR e.normalized_price IS NULL OR e.normalized_price <= 0
           OR NOT EXISTS (SELECT 1 FROM #silver_cust c WHERE c.cust_id = e.sls_cust_id)
           OR NOT EXISTS (
                SELECT 1 FROM #silver_prd p
                WHERE SUBSTRING(p.prd_key, 7, LEN(p.prd_key)) = TRIM(e.sls_prd_key)
           );

        SELECT TRIM(e.sls_ord_num) AS sls_ord_num, TRIM(e.sls_prd_key) AS sls_prd_key,
               e.sls_cust_id, e.sls_order_dt, e.sls_ship_dt, e.sls_due_dt,
               e.sls_quantity * e.normalized_price AS sls_sales,
               e.sls_quantity, e.normalized_price AS sls_price,
               e.parsed_order_date AS order_date,
               e.parsed_ship_date AS ship_date,
               e.parsed_due_date AS due_date
        INTO #silver_sales
        FROM #sales_evaluated e
        WHERE NULLIF(TRIM(e.sls_ord_num), N'') IS NOT NULL
          AND NULLIF(TRIM(e.sls_prd_key), N'') IS NOT NULL
          AND e.sls_cust_id IS NOT NULL
          AND e.parsed_order_date IS NOT NULL
          AND (e.sls_ship_dt = 0 OR e.parsed_ship_date IS NOT NULL)
          AND (e.sls_due_dt = 0 OR e.parsed_due_date IS NOT NULL)
          AND (e.parsed_ship_date IS NULL OR e.parsed_ship_date >= e.parsed_order_date)
          AND (e.parsed_due_date IS NULL OR e.parsed_due_date >= e.parsed_order_date)
          AND (e.parsed_ship_date IS NULL OR e.parsed_due_date IS NULL OR e.parsed_due_date >= e.parsed_ship_date)
          AND e.sls_quantity > 0
          AND e.normalized_price > 0
          AND EXISTS (SELECT 1 FROM #silver_cust c WHERE c.cust_id = e.sls_cust_id)
          AND EXISTS (
                SELECT 1 FROM #silver_prd p
                WHERE SUBSTRING(p.prd_key, 7, LEN(p.prd_key)) = TRIM(e.sls_prd_key)
          );

        WITH normalized AS (
            SELECT REPLACE(CASE WHEN LEFT(TRIM(cid), 3) = N'NAS' THEN SUBSTRING(TRIM(cid), 4, 50) ELSE TRIM(cid) END, N'-', N'') AS normalized_cid,
                   CASE WHEN bdate < '1924-01-01' OR bdate > CONVERT(DATE, @batch_time_utc) THEN NULL ELSE bdate END AS bdate,
                   CASE UPPER(TRIM(gen))
                       WHEN 'M' THEN N'Male' WHEN 'MALE' THEN N'Male'
                       WHEN 'F' THEN N'Female' WHEN 'FEMALE' THEN N'Female'
                       ELSE N'n/a' END AS gen,
                   source_row_number
            FROM bronze.erp_cust_az12 WHERE load_batch_id = @batch_id
        ), ranked AS (
            SELECT normalized_cid AS cid, bdate, gen,
                   ROW_NUMBER() OVER (PARTITION BY normalized_cid ORDER BY source_row_number DESC) AS rn
            FROM normalized
        )
        SELECT cid, bdate, gen INTO #silver_erp_cust FROM ranked WHERE rn = 1 AND NULLIF(cid, N'') IS NOT NULL;

        WITH ranked AS (
            SELECT REPLACE(TRIM(cid), N'-', N'') AS cid,
                   CASE UPPER(TRIM(cntry))
                       WHEN 'DE' THEN N'Germany' WHEN 'GERMANY' THEN N'Germany'
                       WHEN 'US' THEN N'United States' WHEN 'USA' THEN N'United States'
                       WHEN 'UNITED STATES' THEN N'United States'
                       ELSE COALESCE(NULLIF(TRIM(cntry), N''), N'n/a') END AS cntry,
                   ROW_NUMBER() OVER (PARTITION BY REPLACE(TRIM(cid), N'-', N'') ORDER BY source_row_number DESC) AS rn
            FROM bronze.erp_loc_a101 WHERE load_batch_id = @batch_id
        )
        SELECT cid, cntry INTO #silver_erp_loc FROM ranked WHERE rn = 1 AND NULLIF(cid, N'') IS NOT NULL;

        WITH ranked AS (
            SELECT TRIM(id) AS id, TRIM(cat) AS cat, TRIM(subcat) AS subcat,
                   CASE UPPER(TRIM(maintenance)) WHEN 'YES' THEN N'Yes' WHEN 'NO' THEN N'No' ELSE N'n/a' END AS maintenance,
                   ROW_NUMBER() OVER (PARTITION BY TRIM(id) ORDER BY source_row_number DESC) AS rn
            FROM bronze.erp_px_cat_g1v2 WHERE load_batch_id = @batch_id
        )
        SELECT id, cat, subcat, maintenance INTO #silver_erp_cat FROM ranked WHERE rn = 1 AND NULLIF(id, N'') IS NOT NULL;

        SELECT @rows_read =
            (SELECT COUNT_BIG(*) FROM bronze.crm_cust_info WHERE load_batch_id = @batch_id)
            + (SELECT COUNT_BIG(*) FROM bronze.crm_prd_info WHERE load_batch_id = @batch_id)
            + (SELECT COUNT_BIG(*) FROM bronze.crm_sales_details WHERE load_batch_id = @batch_id)
            + (SELECT COUNT_BIG(*) FROM bronze.erp_cust_az12 WHERE load_batch_id = @batch_id)
            + (SELECT COUNT_BIG(*) FROM bronze.erp_loc_a101 WHERE load_batch_id = @batch_id)
            + (SELECT COUNT_BIG(*) FROM bronze.erp_px_cat_g1v2 WHERE load_batch_id = @batch_id);

        SELECT @rows_published =
            (SELECT COUNT_BIG(*) FROM #silver_cust)
            + (SELECT COUNT_BIG(*) FROM #silver_prd)
            + (SELECT COUNT_BIG(*) FROM #silver_sales)
            + (SELECT COUNT_BIG(*) FROM #silver_erp_cust)
            + (SELECT COUNT_BIG(*) FROM #silver_erp_loc)
            + (SELECT COUNT_BIG(*) FROM #silver_erp_cat);

        SELECT @rows_rejected = COUNT_BIG(*)
        FROM (
            SELECT DISTINCT source_name, source_file, source_row_number
            FROM control.load_reject WHERE batch_id = @batch_id AND step_id = @step_id
        ) rejected_rows;

        SELECT @total_reject_rows = COUNT_BIG(*)
        FROM (
            SELECT DISTINCT source_name, source_file, source_row_number
            FROM control.load_reject WHERE batch_id = @batch_id
        ) rejected_rows;

        IF @total_reject_rows > @max_reject_rows
        BEGIN
            DECLARE @reject_limit_message NVARCHAR(2048) = CONCAT(
                N'Reject count ', @total_reject_rows, N' exceeds max_reject_rows ',
                @max_reject_rows, N'; Silver and watermarks were not published.'
            );
            THROW 51203, @reject_limit_message, 1;
        END;

        BEGIN TRANSACTION;

        DELETE FROM silver.crm_sales_details;
        DELETE FROM silver.crm_prd_info;
        DELETE FROM silver.crm_cust_info;
        DELETE FROM silver.erp_cust_az12;
        DELETE FROM silver.erp_loc_a101;
        DELETE FROM silver.erp_px_cat_g1v2;

        INSERT silver.crm_cust_info (
            cust_id, cust_key, cust_firstname, cust_lastname, cust_marital_status,
            cust_gender, cust_create_date, cust_is_future, dwh_batch_id
        )
        SELECT cust_id, cust_key, cust_firstname, cust_lastname, cust_marital_status,
               cust_gender, cust_create_date, cust_is_future, @batch_id
        FROM #silver_cust;

        INSERT silver.crm_prd_info (
            prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, dwh_batch_id
        )
        SELECT prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, @batch_id
        FROM #silver_prd;

        INSERT silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
            sls_due_dt, sls_sales, sls_quantity, sls_price,
            order_date, ship_date, due_date, dwh_batch_id
        )
        SELECT sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
               sls_due_dt, sls_sales, sls_quantity, sls_price,
               order_date, ship_date, due_date, @batch_id
        FROM #silver_sales;

        INSERT silver.erp_cust_az12 (cid, bdate, gen, dwh_batch_id)
            SELECT cid, bdate, gen, @batch_id FROM #silver_erp_cust;
        INSERT silver.erp_loc_a101 (cid, cntry, dwh_batch_id)
            SELECT cid, cntry, @batch_id FROM #silver_erp_loc;
        INSERT silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance, dwh_batch_id)
            SELECT id, cat, subcat, maintenance, @batch_id FROM #silver_erp_cat;

        UPDATE w
        SET watermark_value = @source_watermark,
            source_version = @source_version,
            batch_id = @batch_id,
            updated_at_utc = SYSUTCDATETIME()
        FROM control.load_watermark w
        INNER JOIN (VALUES
            (N'crm_cust_info'), (N'crm_prd_info'), (N'crm_sales_details'),
            (N'erp_cust_az12'), (N'erp_loc_a101'), (N'erp_px_cat_g1v2')
        ) s(source_name)
            ON s.source_name = w.source_name
           AND w.pipeline_name = @pipeline_name;

        INSERT control.load_watermark (
            pipeline_name, source_name, watermark_value, source_version, batch_id
        )
        SELECT @pipeline_name, s.source_name, @source_watermark, @source_version, @batch_id
        FROM (VALUES
            (N'crm_cust_info'), (N'crm_prd_info'), (N'crm_sales_details'),
            (N'erp_cust_az12'), (N'erp_loc_a101'), (N'erp_px_cat_g1v2')
        ) s(source_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM control.load_watermark w
            WHERE w.pipeline_name = @pipeline_name AND w.source_name = s.source_name
        );

        UPDATE control.pipeline_step
        SET status = 'SUCCEEDED',
            rows_read = @rows_read,
            rows_accepted = @rows_published,
            rows_rejected = @rows_rejected,
            rows_superseded = @rows_read - @rows_published - @rows_rejected,
            rows_published = @rows_published,
            watermark_after = @source_watermark,
            completed_at_utc = SYSUTCDATETIME()
        WHERE step_id = @step_id AND status = 'RUNNING';

        UPDATE control.pipeline_batch
        SET status = 'SUCCEEDED', completed_at_utc = SYSUTCDATETIME()
        WHERE batch_id = @batch_id AND status = 'RUNNING';

        IF @@ROWCOUNT <> 1
            THROW 51204, 'Pipeline batch did not transition from RUNNING to SUCCEEDED.', 1;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        SELECT
            @error_number = ERROR_NUMBER(),
            @error_severity = ERROR_SEVERITY(),
            @error_state = ERROR_STATE(),
            @error_procedure = ERROR_PROCEDURE(),
            @error_line = ERROR_LINE(),
            @error_message = ERROR_MESSAGE();

        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

        BEGIN TRY
            UPDATE control.pipeline_step
            SET status = 'FAILED',
                rows_read = COALESCE(rows_read, @rows_read),
                rows_rejected = COALESCE(rows_rejected, @rows_rejected),
                completed_at_utc = SYSUTCDATETIME(),
                error_number = @error_number,
                error_severity = @error_severity,
                error_state = @error_state,
                error_procedure = @error_procedure,
                error_line = @error_line,
                error_message = @error_message
            WHERE step_id = @step_id AND status = 'RUNNING';
        END TRY
        BEGIN CATCH
            -- Best-effort audit persistence must not mask the original error.
        END CATCH;

        THROW;
    END CATCH;
END;
GO
