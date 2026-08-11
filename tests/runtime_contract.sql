:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'control.pipeline_batch', N'U') IS NULL THROW 52000, 'Missing control.pipeline_batch.', 1;
IF OBJECT_ID(N'control.pipeline_step', N'U') IS NULL THROW 52001, 'Missing control.pipeline_step.', 1;
IF OBJECT_ID(N'control.load_watermark', N'U') IS NULL THROW 52002, 'Missing control.load_watermark.', 1;
IF OBJECT_ID(N'control.load_reject', N'U') IS NULL THROW 52003, 'Missing control.load_reject.', 1;
IF OBJECT_ID(N'control.run_pipeline', N'P') IS NULL THROW 52004, 'Missing control.run_pipeline.', 1;
IF OBJECT_ID(N'bronze.load_bronze', N'P') IS NULL THROW 52005, 'Missing bronze.load_bronze.', 1;
IF OBJECT_ID(N'silver.load_silver', N'P') IS NULL THROW 52006, 'Missing silver.load_silver.', 1;

IF COL_LENGTH(N'control.pipeline_batch', N'source_version') IS NULL
   OR COL_LENGTH(N'control.pipeline_batch', N'source_watermark') IS NULL
   OR COL_LENGTH(N'control.pipeline_batch', N'max_reject_rows') IS NULL
   OR COL_LENGTH(N'control.pipeline_batch', N'restart_of_batch_id') IS NULL
   OR COL_LENGTH(N'control.pipeline_batch', N'error_message') IS NULL
    THROW 52007, 'Batch audit contract is incomplete.', 1;

IF COL_LENGTH(N'control.load_reject', N'source_file') IS NULL
   OR COL_LENGTH(N'control.load_reject', N'rule_code') IS NULL
   OR COL_LENGTH(N'control.load_reject', N'raw_payload') IS NULL
    THROW 52008, 'Reject provenance contract is incomplete.', 1;
IF COL_LENGTH(N'control.pipeline_step', N'rows_superseded') IS NULL
   OR COL_LENGTH(N'control.pipeline_step', N'watermark_before') IS NULL
   OR COL_LENGTH(N'control.pipeline_step', N'watermark_after') IS NULL
    THROW 52011, 'Step reconciliation or watermark contract is incomplete.', 1;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'control.pipeline_batch')
      AND name = N'UX_control_pipeline_batch_success_version'
      AND is_unique = 1
      AND has_filter = 1
)
    THROW 52009, 'Successful source-version idempotency index is missing.', 1;

IF EXISTS (
    SELECT required.table_name
    FROM (VALUES
        (N'bronze.crm_cust_info'), (N'bronze.crm_prd_info'), (N'bronze.crm_sales_details'),
        (N'bronze.erp_cust_az12'), (N'bronze.erp_loc_a101'), (N'bronze.erp_px_cat_g1v2'),
        (N'silver.crm_cust_info'), (N'silver.crm_prd_info'), (N'silver.crm_sales_details'),
        (N'silver.erp_cust_az12'), (N'silver.erp_loc_a101'), (N'silver.erp_px_cat_g1v2')
    ) required(table_name)
    WHERE OBJECT_ID(required.table_name, N'U') IS NULL
)
    THROW 52010, 'One or more required Bronze/Silver tables are missing.', 1;

PRINT 'Runtime contract checks passed.';
GO
