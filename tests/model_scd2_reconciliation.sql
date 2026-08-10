:ON ERROR EXIT

USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

DECLARE @open_end_product_id INT = 2147480001;
DECLARE @snapshot_as_of DATE = CONVERT(DATE, '20240101', 112);

IF EXISTS (SELECT 1 FROM silver.crm_prd_info WHERE prd_id = @open_end_product_id)
    THROW 53426, 'Open-end counterexample identifier unexpectedly exists in Silver.', 1;
IF EXISTS (SELECT 1 FROM gold.dim_products WHERE product_id = @open_end_product_id)
    THROW 53427, 'Open-end counterexample identifier unexpectedly exists in Gold.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    /* Isolate the product lifecycle contract from unrelated fact/customer work. */
    DELETE FROM silver.crm_sales_details;
    DELETE FROM silver.crm_cust_info;

    INSERT silver.crm_prd_info
    (
        prd_id, prd_key, prd_nm, prd_cost, prd_line,
        prd_start_dt, prd_end_dt, dwh_batch_id
    )
    VALUES
    (
        @open_end_product_id, N'ZZZZZ-AUDIT-OPEN-END', N'Open-end sentinel version',
        1, N'Audit', CONVERT(DATETIME, '20200101', 112),
        CONVERT(DATETIME, '99991231', 112), NULL
    );

    EXEC gold.usp_load_gold @snapshot_as_of = @snapshot_as_of;

    IF NOT EXISTS
    (
        SELECT 1
        FROM gold.dim_products
        WHERE product_id = @open_end_product_id
          AND source_end_date = CONVERT(DATE, '99991231', 112)
          AND effective_to IS NULL
          AND is_current = 1
    )
        THROW 53428, 'The 9999-12-31 source sentinel was not normalized as an open current interval.', 1;

    DELETE FROM silver.crm_prd_info WHERE prd_id = @open_end_product_id;
    EXEC gold.usp_load_gold @snapshot_as_of = @snapshot_as_of;

    IF NOT EXISTS
    (
        SELECT 1
        FROM gold.dim_products
        WHERE product_id = @open_end_product_id
          AND source_end_date = CONVERT(DATE, '99991231', 112)
          AND effective_to = @snapshot_as_of
          AND is_current = 0
    )
        THROW 53429, 'A removed open-end sentinel version was not retired at the snapshot boundary.', 1;

    INSERT silver.crm_prd_info
    (
        prd_id, prd_key, prd_nm, prd_cost, prd_line,
        prd_start_dt, prd_end_dt, dwh_batch_id
    )
    VALUES
    (
        @open_end_product_id, N'ZZZZZ-AUDIT-OPEN-END', N'Open-end sentinel version',
        1, N'Audit', CONVERT(DATETIME, '20200101', 112),
        CONVERT(DATETIME, '99991231', 112), NULL
    );

    DECLARE @reappearance_rejected BIT = 0;
    BEGIN TRY
        EXEC gold.usp_load_gold @snapshot_as_of = @snapshot_as_of;
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() <> 51010 THROW;
        SET @reappearance_rejected = 1;
    END CATCH;

    IF @reappearance_rejected = 0
        THROW 53430, 'A previously closed product version silently reappeared as current.', 1;

    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;

SELECT N'PASS' AS model_scd2_reconciliation;
GO
