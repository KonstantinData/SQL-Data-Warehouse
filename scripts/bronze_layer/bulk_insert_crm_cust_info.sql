/*
================================================================================
Fail-closed Bronze full-snapshot loader
================================================================================
All six CSV files are staged as text before any published Bronze table changes.
Conversion rejects are persisted in control.load_reject. Publication replaces
all six Bronze tables in one transaction and rethrows every failure.
================================================================================
*/

USE DataWarehouse;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @batch_id BIGINT,
    @base_path NVARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @step_id BIGINT;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @separator NCHAR(1);
    DECLARE @root NVARCHAR(4000);
    DECLARE @crm_cust_file NVARCHAR(4000);
    DECLARE @crm_prd_file NVARCHAR(4000);
    DECLARE @crm_sales_file NVARCHAR(4000);
    DECLARE @erp_cust_file NVARCHAR(4000);
    DECLARE @erp_loc_file NVARCHAR(4000);
    DECLARE @erp_cat_file NVARCHAR(4000);
    DECLARE @rows_read BIGINT;
    DECLARE @rows_rejected BIGINT;
    DECLARE @rows_published BIGINT = 0;
    DECLARE @max_reject_rows BIGINT;
    DECLARE @error_number INT;
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @error_procedure NVARCHAR(256);
    DECLARE @error_line INT;
    DECLARE @error_message NVARCHAR(4000);

    IF @@TRANCOUNT <> 0
        THROW 51100, 'bronze.load_bronze cannot run inside a caller-owned transaction.', 1;
    IF ISNULL(APPLOCK_MODE(N'public', N'SQL-Data-Warehouse:operational-pipeline', N'Session'), N'NoLock') <> N'Exclusive'
        THROW 51104, 'bronze.load_bronze requires the coordinator session lock.', 1;
    IF NOT EXISTS (
        SELECT 1 FROM control.pipeline_batch
        WHERE batch_id = @batch_id AND status = 'RUNNING'
    )
        THROW 51101, 'batch_id must reference a RUNNING pipeline batch.', 1;

    SELECT @max_reject_rows = max_reject_rows
    FROM control.pipeline_batch
    WHERE batch_id = @batch_id;

    SET @root = TRIM(@base_path);
    SET @separator = CASE WHEN LEFT(@root, 1) = N'/' OR CHARINDEX(N'/', @root) > 0 THEN N'/' ELSE N'\' END;
    IF RIGHT(@root, 1) NOT IN (N'\', N'/') SET @root += @separator;

    SET @crm_cust_file = @root + N'source_crm' + @separator + N'cst_info.csv';
    SET @crm_prd_file = @root + N'source_crm' + @separator + N'prd_info.csv';
    SET @crm_sales_file = @root + N'source_crm' + @separator + N'sales_details.csv';
    SET @erp_cust_file = @root + N'source_erp' + @separator + N'CST_AZ12.csv';
    SET @erp_loc_file = @root + N'source_erp' + @separator + N'LOC_A101.csv';
    SET @erp_cat_file = @root + N'source_erp' + @separator + N'PX_CAT_G1V2.csv';

    INSERT control.pipeline_step (
        batch_id, step_name, status, source_name, target_name
    )
    VALUES (
        @batch_id, N'bronze.full_snapshot', 'RUNNING',
        N'crm+erp CSV snapshot', N'bronze (six tables)'
    );
    SET @step_id = SCOPE_IDENTITY();

    CREATE TABLE #crm_cust_raw (
        cust_id NVARCHAR(4000) NULL,
        cust_key NVARCHAR(4000) NULL,
        cust_firstname NVARCHAR(4000) NULL,
        cust_lastname NVARCHAR(4000) NULL,
        cust_marital_status NVARCHAR(4000) NULL,
        cust_gender NVARCHAR(4000) NULL,
        cust_create_date NVARCHAR(4000) NULL
    );
    CREATE TABLE #crm_prd_raw (
        prd_id NVARCHAR(4000) NULL,
        prd_key NVARCHAR(4000) NULL,
        prd_nm NVARCHAR(4000) NULL,
        prd_cost NVARCHAR(4000) NULL,
        prd_line NVARCHAR(4000) NULL,
        prd_start_dt NVARCHAR(4000) NULL,
        prd_end_dt NVARCHAR(4000) NULL
    );
    CREATE TABLE #crm_sales_raw (
        sls_ord_num NVARCHAR(4000) NULL,
        sls_prd_key NVARCHAR(4000) NULL,
        sls_cust_id NVARCHAR(4000) NULL,
        sls_order_dt NVARCHAR(4000) NULL,
        sls_ship_dt NVARCHAR(4000) NULL,
        sls_due_dt NVARCHAR(4000) NULL,
        sls_sales NVARCHAR(4000) NULL,
        sls_quantity NVARCHAR(4000) NULL,
        sls_price NVARCHAR(4000) NULL
    );
    CREATE TABLE #erp_cust_raw (
        cid NVARCHAR(4000) NULL,
        bdate NVARCHAR(4000) NULL,
        gen NVARCHAR(4000) NULL
    );
    CREATE TABLE #erp_loc_raw (
        cid NVARCHAR(4000) NULL,
        cntry NVARCHAR(4000) NULL
    );
    CREATE TABLE #erp_cat_raw (
        id NVARCHAR(4000) NULL,
        cat NVARCHAR(4000) NULL,
        subcat NVARCHAR(4000) NULL,
        maintenance NVARCHAR(4000) NULL
    );

    BEGIN TRY
        SET @sql = N'BULK INSERT #crm_cust_raw FROM N''' + REPLACE(@crm_cust_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        SET @sql = N'BULK INSERT #crm_prd_raw FROM N''' + REPLACE(@crm_prd_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        SET @sql = N'BULK INSERT #crm_sales_raw FROM N''' + REPLACE(@crm_sales_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        SET @sql = N'BULK INSERT #erp_cust_raw FROM N''' + REPLACE(@erp_cust_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        SET @sql = N'BULK INSERT #erp_loc_raw FROM N''' + REPLACE(@erp_loc_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        SET @sql = N'BULK INSERT #erp_cat_raw FROM N''' + REPLACE(@erp_cat_file, N'''', N'''''')
            + N''' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDQUOTE = ''"'', ROWTERMINATOR = ''0x0a'', TABLOCK);';
        EXEC sys.sp_executesql @sql;

        ALTER TABLE #crm_cust_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;
        ALTER TABLE #crm_prd_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;
        ALTER TABLE #crm_sales_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;
        ALTER TABLE #erp_cust_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;
        ALTER TABLE #erp_loc_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;
        ALTER TABLE #erp_cat_raw ADD source_record_id BIGINT IDENTITY(1, 1) NOT NULL;

        SELECT @rows_read =
            (SELECT COUNT_BIG(*) FROM #crm_cust_raw)
            + (SELECT COUNT_BIG(*) FROM #crm_prd_raw)
            + (SELECT COUNT_BIG(*) FROM #crm_sales_raw)
            + (SELECT COUNT_BIG(*) FROM #erp_cust_raw)
            + (SELECT COUNT_BIG(*) FROM #erp_loc_raw)
            + (SELECT COUNT_BIG(*) FROM #erp_cat_raw);

        IF NOT EXISTS (SELECT 1 FROM #crm_cust_raw)
           OR NOT EXISTS (SELECT 1 FROM #crm_prd_raw)
           OR NOT EXISTS (SELECT 1 FROM #crm_sales_raw)
           OR NOT EXISTS (SELECT 1 FROM #erp_cust_raw)
           OR NOT EXISTS (SELECT 1 FROM #erp_loc_raw)
           OR NOT EXISTS (SELECT 1 FROM #erp_cat_raw)
            THROW 51103, 'Every required source file must contain at least one data row.', 1;

        INSERT control.load_reject (
            batch_id, step_id, source_name, source_file, source_row_number,
            business_key, column_name, rule_code, raw_value, raw_payload, error_message
        )
        SELECT @batch_id, @step_id, N'crm_cust_info', @crm_cust_file,
               source_record_id + 1, cust_key, N'cust_id',
               N'INVALID_INTEGER', cust_id,
               CONCAT_WS(N'|', cust_id, cust_key, cust_firstname, cust_lastname, cust_create_date),
               N'cust_id is not a valid integer.'
        FROM #crm_cust_raw
        WHERE NULLIF(TRIM(REPLACE(cust_id, CHAR(13), N'')), N'') IS NOT NULL
          AND TRY_CONVERT(INT, TRIM(REPLACE(cust_id, CHAR(13), N''))) IS NULL
        UNION ALL
        SELECT @batch_id, @step_id, N'crm_cust_info', @crm_cust_file,
               source_record_id + 1, cust_key, N'cust_create_date',
               N'INVALID_DATE', cust_create_date,
               CONCAT_WS(N'|', cust_id, cust_key, cust_firstname, cust_lastname, cust_create_date),
               N'cust_create_date is not a valid date.'
        FROM #crm_cust_raw
        WHERE NULLIF(TRIM(REPLACE(cust_create_date, CHAR(13), N'')), N'') IS NOT NULL
          AND TRY_CONVERT(DATE, TRIM(REPLACE(cust_create_date, CHAR(13), N'')), 23) IS NULL
        UNION ALL
        SELECT @batch_id, @step_id, N'crm_prd_info', @crm_prd_file,
               source_record_id + 1, prd_key, v.column_name,
               v.rule_code, v.raw_value,
               CONCAT_WS(N'|', prd_id, prd_key, prd_nm, prd_cost, prd_start_dt, prd_end_dt),
               v.error_message
        FROM #crm_prd_raw r
        CROSS APPLY (
            SELECT N'prd_id', N'INVALID_INTEGER', r.prd_id, N'prd_id is not a valid integer.'
            WHERE NULLIF(TRIM(r.prd_id), N'') IS NOT NULL AND TRY_CONVERT(INT, TRIM(r.prd_id)) IS NULL
            UNION ALL
            SELECT N'prd_cost', N'INVALID_INTEGER', r.prd_cost, N'prd_cost is not a valid integer.'
            WHERE NULLIF(TRIM(r.prd_cost), N'') IS NOT NULL AND TRY_CONVERT(INT, TRIM(r.prd_cost)) IS NULL
            UNION ALL
            SELECT N'prd_start_dt', N'INVALID_DATETIME', r.prd_start_dt, N'prd_start_dt is not a valid datetime.'
            WHERE NULLIF(TRIM(r.prd_start_dt), N'') IS NOT NULL AND TRY_CONVERT(DATETIME2(0), TRIM(r.prd_start_dt), 23) IS NULL
            UNION ALL
            SELECT N'prd_end_dt', N'INVALID_DATETIME', REPLACE(r.prd_end_dt, CHAR(13), N''), N'prd_end_dt is not a valid datetime.'
            WHERE NULLIF(TRIM(REPLACE(r.prd_end_dt, CHAR(13), N'')), N'') IS NOT NULL
              AND TRY_CONVERT(DATETIME2(0), TRIM(REPLACE(r.prd_end_dt, CHAR(13), N'')), 23) IS NULL
        ) v(column_name, rule_code, raw_value, error_message)
        UNION ALL
        SELECT @batch_id, @step_id, N'crm_sales_details', @crm_sales_file,
               source_record_id + 1,
               CONCAT(sls_ord_num, N'/', sls_prd_key), v.column_name,
               N'INVALID_INTEGER', v.raw_value,
               CONCAT_WS(N'|', sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
                         sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price),
               CONCAT(v.column_name, N' is not a valid integer.')
        FROM #crm_sales_raw r
        CROSS APPLY (
            SELECT N'sls_cust_id', r.sls_cust_id UNION ALL
            SELECT N'sls_order_dt', r.sls_order_dt UNION ALL
            SELECT N'sls_ship_dt', r.sls_ship_dt UNION ALL
            SELECT N'sls_due_dt', r.sls_due_dt UNION ALL
            SELECT N'sls_sales', r.sls_sales UNION ALL
            SELECT N'sls_quantity', r.sls_quantity UNION ALL
            SELECT N'sls_price', REPLACE(r.sls_price, CHAR(13), N'')
        ) v(column_name, raw_value)
        WHERE NULLIF(TRIM(v.raw_value), N'') IS NOT NULL
          AND TRY_CONVERT(INT, TRIM(v.raw_value)) IS NULL
        UNION ALL
        SELECT @batch_id, @step_id, N'erp_cust_az12', @erp_cust_file,
               source_record_id + 1, cid, N'bdate', N'INVALID_DATE', bdate,
               CONCAT_WS(N'|', cid, bdate, gen), N'bdate is not a valid date.'
        FROM #erp_cust_raw
        WHERE NULLIF(TRIM(bdate), N'') IS NOT NULL
          AND TRY_CONVERT(DATE, TRIM(bdate), 23) IS NULL;

        INSERT control.load_reject (
            batch_id, step_id, source_name, source_file, source_row_number,
            business_key, column_name, rule_code, raw_value, raw_payload, error_message
        )
        SELECT @batch_id, @step_id, source_name, source_file, source_row_number,
               business_key, column_name, N'VALUE_TOO_LONG', raw_value, raw_payload,
               CONCAT(column_name, N' exceeds the target column length.')
        FROM (
            SELECT N'crm_cust_info' source_name, @crm_cust_file source_file, source_record_id + 1 source_row_number,
                   cust_key business_key, v.column_name, v.raw_value,
                   CONCAT_WS(N'|', cust_id, cust_key, cust_firstname, cust_lastname, cust_create_date) raw_payload
            FROM #crm_cust_raw r CROSS APPLY (VALUES
                (N'cust_key', r.cust_key, 50), (N'cust_firstname', r.cust_firstname, 50),
                (N'cust_lastname', r.cust_lastname, 50), (N'cust_marital_status', r.cust_marital_status, 50),
                (N'cust_gender', r.cust_gender, 10)
            ) v(column_name, raw_value, max_length) WHERE LEN(v.raw_value) > v.max_length
            UNION ALL
            SELECT N'crm_prd_info', @crm_prd_file, source_record_id + 1, prd_key,
                   v.column_name, v.raw_value,
                   CONCAT_WS(N'|', prd_id, prd_key, prd_nm, prd_cost, prd_line)
            FROM #crm_prd_raw r CROSS APPLY (VALUES
                (N'prd_key', r.prd_key, 50), (N'prd_nm', r.prd_nm, 50), (N'prd_line', r.prd_line, 50)
            ) v(column_name, raw_value, max_length) WHERE LEN(v.raw_value) > v.max_length
            UNION ALL
            SELECT N'crm_sales_details', @crm_sales_file, source_record_id + 1,
                   CONCAT(sls_ord_num, N'/', sls_prd_key), v.column_name, v.raw_value,
                   CONCAT_WS(N'|', sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt)
            FROM #crm_sales_raw r CROSS APPLY (VALUES
                (N'sls_ord_num', r.sls_ord_num, 50), (N'sls_prd_key', r.sls_prd_key, 50)
            ) v(column_name, raw_value, max_length) WHERE LEN(v.raw_value) > v.max_length
            UNION ALL
            SELECT N'erp_cust_az12', @erp_cust_file, source_record_id + 1, cid,
                   v.column_name, v.raw_value, CONCAT_WS(N'|', cid, bdate, gen)
            FROM #erp_cust_raw r CROSS APPLY (VALUES (N'cid', r.cid, 50), (N'gen', r.gen, 10)) v(column_name, raw_value, max_length)
            WHERE LEN(v.raw_value) > v.max_length
            UNION ALL
            SELECT N'erp_loc_a101', @erp_loc_file, source_record_id + 1, cid,
                   v.column_name, v.raw_value, CONCAT_WS(N'|', cid, cntry)
            FROM #erp_loc_raw r CROSS APPLY (VALUES (N'cid', r.cid, 50), (N'cntry', r.cntry, 50)) v(column_name, raw_value, max_length)
            WHERE LEN(v.raw_value) > v.max_length
            UNION ALL
            SELECT N'erp_px_cat_g1v2', @erp_cat_file, source_record_id + 1, id,
                   v.column_name, v.raw_value, CONCAT_WS(N'|', id, cat, subcat, maintenance)
            FROM #erp_cat_raw r CROSS APPLY (VALUES
                (N'id', r.id, 50), (N'cat', r.cat, 50), (N'subcat', r.subcat, 50), (N'maintenance', r.maintenance, 50)
            ) v(column_name, raw_value, max_length) WHERE LEN(v.raw_value) > v.max_length
        ) length_rejects;

        INSERT control.load_reject (
            batch_id, step_id, source_name, source_file, source_row_number,
            business_key, column_name, rule_code, raw_value, raw_payload, error_message
        )
        SELECT @batch_id, @step_id, source_name, source_file, source_row_number,
               business_key, column_name, rule_code, raw_value, raw_payload, error_message
        FROM (
            SELECT N'crm_cust_info' AS source_name, @crm_cust_file AS source_file,
                   source_record_id + 1 AS source_row_number,
                   cust_key AS business_key, N'cust_id' AS column_name,
                   N'MISSING_REQUIRED_KEY' AS rule_code, cust_id AS raw_value,
                   CONCAT_WS(N'|', cust_id, cust_key, cust_firstname, cust_lastname, cust_create_date) AS raw_payload,
                   N'cust_id is required.' AS error_message
            FROM #crm_cust_raw
            WHERE NULLIF(TRIM(cust_id), N'') IS NULL OR NULLIF(TRIM(cust_key), N'') IS NULL
            UNION ALL
            SELECT N'crm_prd_info', @crm_prd_file,
                   source_record_id + 1, prd_key, N'prd_key',
                   N'MISSING_REQUIRED_KEY', prd_key,
                   CONCAT_WS(N'|', prd_id, prd_key, prd_nm, prd_cost, prd_start_dt),
                   N'prd_id and prd_key are required.'
            FROM #crm_prd_raw
            WHERE NULLIF(TRIM(prd_id), N'') IS NULL OR NULLIF(TRIM(prd_key), N'') IS NULL
            UNION ALL
            SELECT N'crm_sales_details', @crm_sales_file,
                   source_record_id + 1,
                   CONCAT(sls_ord_num, N'/', sls_prd_key), N'sales_business_key',
                   N'MISSING_REQUIRED_KEY', NULL,
                   CONCAT_WS(N'|', sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt),
                   N'Order number, product key, and customer id are required.'
            FROM #crm_sales_raw
            WHERE NULLIF(TRIM(sls_ord_num), N'') IS NULL
               OR NULLIF(TRIM(sls_prd_key), N'') IS NULL
               OR NULLIF(TRIM(sls_cust_id), N'') IS NULL
            UNION ALL
            SELECT N'erp_cust_az12', @erp_cust_file, source_record_id + 1,
                   cid, N'cid', N'MISSING_REQUIRED_KEY', cid,
                   CONCAT_WS(N'|', cid, bdate, gen), N'cid is required.'
            FROM #erp_cust_raw WHERE NULLIF(TRIM(cid), N'') IS NULL
            UNION ALL
            SELECT N'erp_loc_a101', @erp_loc_file, source_record_id + 1,
                   cid, N'cid', N'MISSING_REQUIRED_KEY', cid,
                   CONCAT_WS(N'|', cid, cntry), N'cid is required.'
            FROM #erp_loc_raw WHERE NULLIF(TRIM(cid), N'') IS NULL
            UNION ALL
            SELECT N'erp_px_cat_g1v2', @erp_cat_file, source_record_id + 1,
                   id, N'id', N'MISSING_REQUIRED_KEY', id,
                   CONCAT_WS(N'|', id, cat, subcat, maintenance), N'id is required.'
            FROM #erp_cat_raw WHERE NULLIF(TRIM(id), N'') IS NULL
        ) required_rejects;

        SELECT DISTINCT source_name, source_row_number
        INTO #rejected_source_rows
        FROM control.load_reject
        WHERE batch_id = @batch_id AND step_id = @step_id;

        SELECT @rows_rejected = COUNT_BIG(*) FROM #rejected_source_rows;

        IF @rows_rejected > @max_reject_rows
            THROW 51102, 'Bronze reject count exceeds max_reject_rows; no target was published.', 1;

        BEGIN TRANSACTION;

        DELETE FROM bronze.crm_sales_details;
        DELETE FROM bronze.crm_prd_info;
        DELETE FROM bronze.crm_cust_info;
        DELETE FROM bronze.erp_cust_az12;
        DELETE FROM bronze.erp_loc_a101;
        DELETE FROM bronze.erp_px_cat_g1v2;

        INSERT bronze.crm_cust_info (
            cust_id, cust_key, cust_firstname, cust_lastname, cust_marital_status,
            cust_gender, cust_create_date, load_batch_id, source_file, source_row_number
        )
        SELECT TRY_CONVERT(INT, TRIM(cust_id)), TRIM(cust_key), cust_firstname, cust_lastname,
               cust_marital_status, cust_gender,
               TRY_CONVERT(DATE, TRIM(REPLACE(cust_create_date, CHAR(13), N'')), 23),
               @batch_id, @crm_cust_file,
               source_record_id + 1
        FROM #crm_cust_raw
        WHERE TRY_CONVERT(INT, NULLIF(TRIM(cust_id), N'')) IS NOT NULL
          AND NULLIF(TRIM(cust_key), N'') IS NOT NULL
          AND (NULLIF(TRIM(REPLACE(cust_create_date, CHAR(13), N'')), N'') IS NULL
               OR TRY_CONVERT(DATE, TRIM(REPLACE(cust_create_date, CHAR(13), N'')), 23) IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'crm_cust_info' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        INSERT bronze.crm_prd_info (
            prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt,
            load_batch_id, source_file, source_row_number
        )
        SELECT TRY_CONVERT(INT, TRIM(prd_id)), TRIM(prd_key), prd_nm,
               TRY_CONVERT(INT, NULLIF(TRIM(prd_cost), N'')), prd_line,
               TRY_CONVERT(DATETIME2(0), NULLIF(TRIM(prd_start_dt), N''), 23),
               TRY_CONVERT(DATETIME2(0), NULLIF(TRIM(REPLACE(prd_end_dt, CHAR(13), N'')), N''), 23),
               @batch_id, @crm_prd_file,
               source_record_id + 1
        FROM #crm_prd_raw
        WHERE TRY_CONVERT(INT, NULLIF(TRIM(prd_id), N'')) IS NOT NULL
          AND NULLIF(TRIM(prd_key), N'') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'crm_prd_info' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        INSERT bronze.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
            sls_due_dt, sls_sales, sls_quantity, sls_price,
            load_batch_id, source_file, source_row_number
        )
        SELECT TRIM(sls_ord_num), TRIM(sls_prd_key), TRY_CONVERT(INT, TRIM(sls_cust_id)),
               TRY_CONVERT(INT, NULLIF(TRIM(sls_order_dt), N'')),
               TRY_CONVERT(INT, NULLIF(TRIM(sls_ship_dt), N'')),
               TRY_CONVERT(INT, NULLIF(TRIM(sls_due_dt), N'')),
               TRY_CONVERT(INT, NULLIF(TRIM(sls_sales), N'')),
               TRY_CONVERT(INT, NULLIF(TRIM(sls_quantity), N'')),
               TRY_CONVERT(INT, NULLIF(TRIM(REPLACE(sls_price, CHAR(13), N'')), N'')),
               @batch_id, @crm_sales_file,
               source_record_id + 1
        FROM #crm_sales_raw
        WHERE NULLIF(TRIM(sls_ord_num), N'') IS NOT NULL
          AND NULLIF(TRIM(sls_prd_key), N'') IS NOT NULL
          AND TRY_CONVERT(INT, NULLIF(TRIM(sls_cust_id), N'')) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'crm_sales_details' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        INSERT bronze.erp_cust_az12 (
            cid, bdate, gen, load_batch_id, source_file, source_row_number
        )
        SELECT TRIM(cid), TRY_CONVERT(DATE, NULLIF(TRIM(bdate), N''), 23),
               TRIM(REPLACE(gen, CHAR(13), N'')), @batch_id, @erp_cust_file,
               source_record_id + 1
        FROM #erp_cust_raw
        WHERE NULLIF(TRIM(cid), N'') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'erp_cust_az12' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        INSERT bronze.erp_loc_a101 (
            cid, cntry, load_batch_id, source_file, source_row_number
        )
        SELECT TRIM(cid), TRIM(REPLACE(cntry, CHAR(13), N'')), @batch_id, @erp_loc_file,
               source_record_id + 1
        FROM #erp_loc_raw
        WHERE NULLIF(TRIM(cid), N'') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'erp_loc_a101' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        INSERT bronze.erp_px_cat_g1v2 (
            id, cat, subcat, maintenance, load_batch_id, source_file, source_row_number
        )
        SELECT TRIM(id), TRIM(cat), TRIM(subcat),
               TRIM(REPLACE(maintenance, CHAR(13), N'')), @batch_id, @erp_cat_file,
               source_record_id + 1
        FROM #erp_cat_raw
        WHERE NULLIF(TRIM(id), N'') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM #rejected_source_rows r
              WHERE r.source_name = N'erp_px_cat_g1v2' AND r.source_row_number = source_record_id + 1
          );
        SET @rows_published += @@ROWCOUNT;

        UPDATE control.pipeline_step
        SET status = 'SUCCEEDED',
            rows_read = @rows_read,
            rows_accepted = @rows_published,
            rows_rejected = @rows_rejected,
            rows_published = @rows_published,
            completed_at_utc = SYSUTCDATETIME()
        WHERE step_id = @step_id AND status = 'RUNNING';

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
