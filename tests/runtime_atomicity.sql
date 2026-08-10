:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF N'$(ConfirmRuntimeTests)' <> N'RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE'
    THROW 52400, 'Mutating runtime tests require the exact disposable-database confirmation token.', 1;

SELECT * INTO #silver_cust FROM silver.crm_cust_info;
SELECT * INTO #silver_prd FROM silver.crm_prd_info;
SELECT * INTO #silver_sales FROM silver.crm_sales_details;
SELECT * INTO #silver_erp_cust FROM silver.erp_cust_az12;
SELECT * INTO #silver_erp_loc FROM silver.erp_loc_a101;
SELECT * INTO #silver_erp_cat FROM silver.erp_px_cat_g1v2;
SELECT * INTO #watermarks FROM control.load_watermark;

DECLARE @version NVARCHAR(255) = CONCAT(N'runtime-atomicity-', CONVERT(NVARCHAR(36), NEWID()));
DECLARE @watermark BIGINT = ISNULL((SELECT MAX(watermark_value) FROM control.load_watermark), 0) + 1;
DECLARE @batch_id BIGINT;
DECLARE @caught_number INT;

EXEC(N'CREATE OR ALTER TRIGGER silver.runtime_atomicity_failure
ON silver.erp_px_cat_g1v2
INSTEAD OF INSERT
AS
BEGIN
    THROW 52410, ''Injected late Silver publication failure.'', 1;
END;');

BEGIN TRY
    EXEC control.run_pipeline
        @base_path=N'$(TestBasePath)', @source_version=@version,
        @source_watermark=@watermark, @max_reject_rows=24,
        @batch_id=@batch_id OUTPUT;
END TRY
BEGIN CATCH
    SET @caught_number = ERROR_NUMBER();
END CATCH;

DROP TRIGGER IF EXISTS silver.runtime_atomicity_failure;
SELECT @batch_id = batch_id FROM control.pipeline_batch WHERE source_version=@version;

IF @caught_number <> 52410 THROW 52401, 'Injected Silver failure did not reach the caller.', 1;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id=@batch_id AND status='FAILED' AND error_number=52410
)
    THROW 52402, 'Injected Silver failure was not durably audited.', 1;

IF EXISTS (SELECT * FROM silver.crm_cust_info EXCEPT SELECT * FROM #silver_cust)
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
    THROW 52403, 'Silver content changed despite a late publication failure.', 1;
IF EXISTS (SELECT * FROM control.load_watermark EXCEPT SELECT * FROM #watermarks)
   OR EXISTS (SELECT * FROM #watermarks EXCEPT SELECT * FROM control.load_watermark)
    THROW 52404, 'Watermarks advanced despite a failed Silver transaction.', 1;

PRINT 'Silver publication atomicity checks passed.';
GO
