/* Reconcile every source row to either published Bronze or durable quarantine. */

USE DataWarehouse;
GO
SET NOCOUNT ON;

DECLARE @violations INT = 0;
DECLARE @batch_id BIGINT = (
    SELECT TOP (1) batch_id
    FROM control.pipeline_batch
    WHERE pipeline_name = N'sql-data-warehouse-full-snapshot'
      AND status = 'SUCCEEDED'
    ORDER BY batch_id DESC
);
DECLARE @bronze_step_id BIGINT = (
    SELECT step_id
    FROM control.pipeline_step
    WHERE batch_id = @batch_id AND step_name = N'bronze.full_snapshot'
);

IF @batch_id IS NULL
    THROW 51000, 'Bronze reconciliation requires a successful operational batch.', 1;

DECLARE @expected TABLE (source_name NVARCHAR(128), expected_rows BIGINT);
INSERT @expected VALUES
    (N'crm_cust_info', $(EXPECTED_BRONZE_CRM_CUST_INFO)),
    (N'crm_prd_info', $(EXPECTED_BRONZE_CRM_PRD_INFO)),
    (N'crm_sales_details', $(EXPECTED_BRONZE_CRM_SALES_DETAILS)),
    (N'erp_cust_az12', $(EXPECTED_BRONZE_ERP_CUST_AZ12)),
    (N'erp_loc_a101', $(EXPECTED_BRONZE_ERP_LOC_A101)),
    (N'erp_px_cat_g1v2', $(EXPECTED_BRONZE_ERP_PX_CAT_G1V2));

DECLARE @actual TABLE (source_name NVARCHAR(128), published_rows BIGINT);
INSERT @actual VALUES
    (N'crm_cust_info', (SELECT COUNT_BIG(*) FROM bronze.crm_cust_info WHERE load_batch_id = @batch_id)),
    (N'crm_prd_info', (SELECT COUNT_BIG(*) FROM bronze.crm_prd_info WHERE load_batch_id = @batch_id)),
    (N'crm_sales_details', (SELECT COUNT_BIG(*) FROM bronze.crm_sales_details WHERE load_batch_id = @batch_id)),
    (N'erp_cust_az12', (SELECT COUNT_BIG(*) FROM bronze.erp_cust_az12 WHERE load_batch_id = @batch_id)),
    (N'erp_loc_a101', (SELECT COUNT_BIG(*) FROM bronze.erp_loc_a101 WHERE load_batch_id = @batch_id)),
    (N'erp_px_cat_g1v2', (SELECT COUNT_BIG(*) FROM bronze.erp_px_cat_g1v2 WHERE load_batch_id = @batch_id));

IF EXISTS (
    SELECT 1
    FROM @expected AS expected
    INNER JOIN @actual AS actual ON actual.source_name = expected.source_name
    OUTER APPLY (
        SELECT COUNT_BIG(DISTINCT reject.source_row_number) AS rejected_rows
        FROM control.load_reject AS reject
        WHERE reject.batch_id = @batch_id
          AND reject.step_id = @bronze_step_id
          AND reject.source_name = expected.source_name
    ) AS rejects
    WHERE actual.published_rows + rejects.rejected_rows <> expected.expected_rows
)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): published plus quarantined Bronze rows do not reconcile to a source CSV.';
END;

IF EXISTS (
    SELECT 1 FROM @actual WHERE published_rows = 0
)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): a required Bronze source published no rows.';
END;

IF EXISTS (
    SELECT 1
    FROM control.load_reject
    WHERE batch_id = @batch_id
      AND (source_file IS NULL OR rule_code IS NULL OR error_message IS NULL)
)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): quarantine evidence is incomplete.';
END;

IF @violations > 0
    THROW 51001, 'Bronze load reconciliation failed.', 1;

PRINT 'Bronze load contract passed: every source row is published or quarantined.';
GO
