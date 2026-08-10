:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF N'$(ConfirmRuntimeTests)' <> N'RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE'
    THROW 52200, 'Mutating runtime tests require the exact disposable-database confirmation token.', 1;

DECLARE @base_path NVARCHAR(4000) = N'$(TestBasePath)';
DECLARE @version NVARCHAR(255) = CONCAT(N'runtime-idempotency-', CONVERT(NVARCHAR(36), NEWID()));
DECLARE @watermark BIGINT = ISNULL((SELECT MAX(watermark_value) FROM control.load_watermark), 0) + 1;
DECLARE @first_batch BIGINT;
DECLARE @second_batch BIGINT;
DECLARE @caught_number INT;
DECLARE @mismatched_watermark BIGINT;

EXEC control.run_pipeline
    @base_path = @base_path,
    @source_version = @version,
    @source_watermark = @watermark,
    @max_reject_rows = 23,
    @batch_id = @first_batch OUTPUT;

IF NOT EXISTS (SELECT 1 FROM control.pipeline_batch WHERE batch_id = @first_batch AND status = 'SUCCEEDED')
    THROW 52201, 'First synthetic snapshot did not succeed.', 1;

SELECT * INTO #bronze_cust FROM bronze.crm_cust_info;
SELECT * INTO #bronze_prd FROM bronze.crm_prd_info;
SELECT * INTO #bronze_sales FROM bronze.crm_sales_details;
SELECT * INTO #bronze_erp_cust FROM bronze.erp_cust_az12;
SELECT * INTO #bronze_erp_loc FROM bronze.erp_loc_a101;
SELECT * INTO #bronze_erp_cat FROM bronze.erp_px_cat_g1v2;
SELECT * INTO #silver_cust FROM silver.crm_cust_info;
SELECT * INTO #silver_prd FROM silver.crm_prd_info;
SELECT * INTO #silver_sales FROM silver.crm_sales_details;
SELECT * INTO #silver_erp_cust FROM silver.erp_cust_az12;
SELECT * INTO #silver_erp_loc FROM silver.erp_loc_a101;
SELECT * INTO #silver_erp_cat FROM silver.erp_px_cat_g1v2;
SELECT * INTO #watermarks FROM control.load_watermark;
SELECT * INTO #rejects FROM control.load_reject;

EXEC control.run_pipeline
    @base_path = @base_path,
    @source_version = @version,
    @source_watermark = @watermark,
    @max_reject_rows = 23,
    @batch_id = @second_batch OUTPUT;

IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id = @second_batch AND status = 'SKIPPED' AND completed_at_utc IS NOT NULL
)
    THROW 52202, 'Successful replay was not audited as completed SKIPPED.', 1;
IF EXISTS (SELECT 1 FROM control.pipeline_step WHERE batch_id = @second_batch)
    THROW 52203, 'SKIPPED replay unexpectedly created pipeline steps.', 1;
IF 1 <> (SELECT COUNT(*) FROM control.pipeline_batch WHERE pipeline_name = N'sql-data-warehouse-core-snapshot' AND source_version = @version AND status = 'SUCCEEDED')
    THROW 52204, 'Replay produced more than one successful batch.', 1;

IF EXISTS (SELECT * FROM bronze.crm_cust_info EXCEPT SELECT * FROM #bronze_cust)
   OR EXISTS (SELECT * FROM #bronze_cust EXCEPT SELECT * FROM bronze.crm_cust_info)
   OR EXISTS (SELECT * FROM bronze.crm_prd_info EXCEPT SELECT * FROM #bronze_prd)
   OR EXISTS (SELECT * FROM #bronze_prd EXCEPT SELECT * FROM bronze.crm_prd_info)
   OR EXISTS (SELECT * FROM bronze.crm_sales_details EXCEPT SELECT * FROM #bronze_sales)
   OR EXISTS (SELECT * FROM #bronze_sales EXCEPT SELECT * FROM bronze.crm_sales_details)
   OR EXISTS (SELECT * FROM bronze.erp_cust_az12 EXCEPT SELECT * FROM #bronze_erp_cust)
   OR EXISTS (SELECT * FROM #bronze_erp_cust EXCEPT SELECT * FROM bronze.erp_cust_az12)
   OR EXISTS (SELECT * FROM bronze.erp_loc_a101 EXCEPT SELECT * FROM #bronze_erp_loc)
   OR EXISTS (SELECT * FROM #bronze_erp_loc EXCEPT SELECT * FROM bronze.erp_loc_a101)
   OR EXISTS (SELECT * FROM bronze.erp_px_cat_g1v2 EXCEPT SELECT * FROM #bronze_erp_cat)
   OR EXISTS (SELECT * FROM #bronze_erp_cat EXCEPT SELECT * FROM bronze.erp_px_cat_g1v2)
   OR EXISTS (SELECT * FROM silver.crm_cust_info EXCEPT SELECT * FROM #silver_cust)
   OR EXISTS (SELECT * FROM #silver_cust EXCEPT SELECT * FROM silver.crm_cust_info)
   OR EXISTS (SELECT * FROM silver.crm_prd_info EXCEPT SELECT * FROM #silver_prd)
   OR EXISTS (SELECT * FROM #silver_prd EXCEPT SELECT * FROM silver.crm_prd_info)
   OR EXISTS (SELECT * FROM silver.crm_sales_details EXCEPT SELECT * FROM #silver_sales)
   OR EXISTS (SELECT * FROM #silver_sales EXCEPT SELECT * FROM silver.crm_sales_details)
   OR EXISTS (SELECT * FROM silver.erp_cust_az12 EXCEPT SELECT * FROM #silver_erp_cust)
   OR EXISTS (SELECT * FROM #silver_erp_cust EXCEPT SELECT * FROM silver.erp_cust_az12)
   OR EXISTS (SELECT * FROM silver.erp_loc_a101 EXCEPT SELECT * FROM #silver_erp_loc)
   OR EXISTS (SELECT * FROM #silver_erp_loc EXCEPT SELECT * FROM silver.erp_loc_a101)
   OR EXISTS (SELECT * FROM silver.erp_px_cat_g1v2 EXCEPT SELECT * FROM #silver_erp_cat)
   OR EXISTS (SELECT * FROM #silver_erp_cat EXCEPT SELECT * FROM silver.erp_px_cat_g1v2)
    THROW 52205, 'SKIPPED replay changed Bronze or Silver content.', 1;

IF EXISTS (SELECT * FROM control.load_watermark EXCEPT SELECT * FROM #watermarks)
   OR EXISTS (SELECT * FROM #watermarks EXCEPT SELECT * FROM control.load_watermark)
   OR EXISTS (SELECT * FROM control.load_reject EXCEPT SELECT * FROM #rejects)
   OR EXISTS (SELECT * FROM #rejects EXCEPT SELECT * FROM control.load_reject)
    THROW 52206, 'SKIPPED replay changed watermark or reject evidence.', 1;

BEGIN TRY
    SET @mismatched_watermark = @watermark + 1;
    EXEC control.run_pipeline @base_path=@base_path, @source_version=@version,
        @source_watermark=@mismatched_watermark, @max_reject_rows=23, @batch_id=@second_batch OUTPUT;
END TRY
BEGIN CATCH
    SET @caught_number = ERROR_NUMBER();
END CATCH;
IF @caught_number <> 51011 THROW 52207, 'Successful-version watermark mismatch did not fail closed.', 1;

DECLARE @failed_version NVARCHAR(255) = CONCAT(N'runtime-restart-', CONVERT(NVARCHAR(36), NEWID()));
DECLARE @failed_watermark BIGINT = @watermark + 1;
DECLARE @failed_batch BIGINT;
SET @caught_number = NULL;
BEGIN TRY
    EXEC control.run_pipeline @base_path=N'C:\definitely-missing-sql-dw-runtime-fixture',
        @source_version=@failed_version, @source_watermark=@failed_watermark,
        @max_reject_rows=0, @batch_id=@failed_batch OUTPUT;
END TRY
BEGIN CATCH SET @caught_number = ERROR_NUMBER(); END CATCH;
SELECT @failed_batch = batch_id FROM control.pipeline_batch WHERE source_version = @failed_version;
IF @caught_number IS NULL OR NOT EXISTS (SELECT 1 FROM control.pipeline_batch WHERE batch_id=@failed_batch AND status='FAILED')
    THROW 52208, 'Failed restart fixture was not persisted.', 1;

SET @caught_number = NULL;
BEGIN TRY
    EXEC control.run_pipeline @base_path=@base_path, @source_version=@failed_version,
        @source_watermark=@failed_watermark, @max_reject_rows=23, @batch_id=@second_batch OUTPUT;
END TRY
BEGIN CATCH SET @caught_number = ERROR_NUMBER(); END CATCH;
IF @caught_number <> 51006 THROW 52209, 'Unlinked retry of a failed version did not fail closed.', 1;

EXEC control.run_pipeline @base_path=@base_path, @source_version=@failed_version,
    @source_watermark=@failed_watermark, @max_reject_rows=23,
    @restart_of_batch_id=@failed_batch, @batch_id=@second_batch OUTPUT;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id=@second_batch AND status='SUCCEEDED' AND restart_of_batch_id=@failed_batch
)
    THROW 52210, 'Explicit linked restart did not succeed with lineage.', 1;

PRINT 'Runtime idempotency and restart checks passed.';
GO
