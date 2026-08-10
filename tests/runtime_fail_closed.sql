:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

SELECT * INTO #before_bronze_cust FROM bronze.crm_cust_info;
SELECT * INTO #before_bronze_prd FROM bronze.crm_prd_info;
SELECT * INTO #before_bronze_sales FROM bronze.crm_sales_details;
SELECT * INTO #before_erp_cust FROM bronze.erp_cust_az12;
SELECT * INTO #before_erp_loc FROM bronze.erp_loc_a101;
SELECT * INTO #before_erp_cat FROM bronze.erp_px_cat_g1v2;
SELECT * INTO #before_watermarks FROM control.load_watermark;

DECLARE @version NVARCHAR(255) = CONCAT(N'runtime-fail-closed-', CONVERT(NVARCHAR(36), NEWID()));
DECLARE @watermark BIGINT = ISNULL((SELECT MAX(watermark_value) FROM control.load_watermark), 0) + 1;
DECLARE @batch_id BIGINT;
DECLARE @caught_number INT;
DECLARE @caught_message NVARCHAR(4000);

BEGIN TRY
    EXEC control.run_pipeline
        @base_path = N'C:\definitely-missing-sql-dw-runtime-fixture',
        @source_version = @version,
        @source_watermark = @watermark,
        @max_reject_rows = 0,
        @batch_id = @batch_id OUTPUT;
END TRY
BEGIN CATCH
    SELECT @caught_number = ERROR_NUMBER(), @caught_message = ERROR_MESSAGE();
END CATCH;

IF @caught_number IS NULL THROW 52100, 'Missing source failure did not reach the caller.', 1;
SELECT @batch_id = batch_id FROM control.pipeline_batch WHERE source_version = @version;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id = @batch_id AND status = 'FAILED'
      AND completed_at_utc IS NOT NULL
      AND error_number = @caught_number AND error_message = @caught_message
)
    THROW 52101, 'Failed batch evidence does not match the caller-visible error.', 1;
IF 1 <> (
    SELECT COUNT(*) FROM control.pipeline_step
    WHERE batch_id = @batch_id AND step_name = N'bronze.full_snapshot'
      AND status = 'FAILED' AND completed_at_utc IS NOT NULL
      AND error_number = @caught_number AND error_message = @caught_message
)
    THROW 52102, 'Failed Bronze step evidence is incomplete or inconsistent.', 1;
IF EXISTS (SELECT 1 FROM control.pipeline_step WHERE batch_id = @batch_id AND status = 'RUNNING')
    THROW 52103, 'A failed batch retained RUNNING steps.', 1;

IF EXISTS (SELECT * FROM bronze.crm_cust_info EXCEPT SELECT * FROM #before_bronze_cust)
   OR EXISTS (SELECT * FROM #before_bronze_cust EXCEPT SELECT * FROM bronze.crm_cust_info)
   OR EXISTS (SELECT * FROM bronze.crm_prd_info EXCEPT SELECT * FROM #before_bronze_prd)
   OR EXISTS (SELECT * FROM #before_bronze_prd EXCEPT SELECT * FROM bronze.crm_prd_info)
   OR EXISTS (SELECT * FROM bronze.crm_sales_details EXCEPT SELECT * FROM #before_bronze_sales)
   OR EXISTS (SELECT * FROM #before_bronze_sales EXCEPT SELECT * FROM bronze.crm_sales_details)
   OR EXISTS (SELECT * FROM bronze.erp_cust_az12 EXCEPT SELECT * FROM #before_erp_cust)
   OR EXISTS (SELECT * FROM #before_erp_cust EXCEPT SELECT * FROM bronze.erp_cust_az12)
   OR EXISTS (SELECT * FROM bronze.erp_loc_a101 EXCEPT SELECT * FROM #before_erp_loc)
   OR EXISTS (SELECT * FROM #before_erp_loc EXCEPT SELECT * FROM bronze.erp_loc_a101)
   OR EXISTS (SELECT * FROM bronze.erp_px_cat_g1v2 EXCEPT SELECT * FROM #before_erp_cat)
   OR EXISTS (SELECT * FROM #before_erp_cat EXCEPT SELECT * FROM bronze.erp_px_cat_g1v2)
    THROW 52104, 'Bronze content changed after a failed stage.', 1;

IF EXISTS (SELECT * FROM control.load_watermark EXCEPT SELECT * FROM #before_watermarks)
   OR EXISTS (SELECT * FROM #before_watermarks EXCEPT SELECT * FROM control.load_watermark)
    THROW 52105, 'Watermark content changed after a failed stage.', 1;

PRINT 'Fail-closed runtime checks passed.';
GO
