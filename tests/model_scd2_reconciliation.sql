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

DECLARE @retired_product_id INT = 2147480000;
DECLARE @snapshot_as_of DATE = CONVERT(DATE, '20240101', 112);

IF EXISTS (SELECT 1 FROM silver.crm_prd_info WHERE prd_id = @retired_product_id)
    THROW 53420, 'SCD2 counterexample identifier unexpectedly exists in Silver.', 1;
IF EXISTS (SELECT 1 FROM gold.dim_products WHERE product_id = @retired_product_id)
    THROW 53423, 'SCD2 counterexample identifier unexpectedly exists in Gold.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    INSERT gold.dim_products
    (
        product_id, product_number, product_name, product_cost, product_line,
        category, subcategory, maintenance, effective_from, effective_to,
        source_end_date, is_current
    )
    VALUES
    (
        @retired_product_id, N'AUDIT-REMOVED-VERSION', N'Removed snapshot version',
        1.00, N'Audit', N'Audit', N'Audit', N'No', CONVERT(DATE, '20200101', 112),
        NULL, NULL, 1
    );

    DECLARE @removed_current_id INT;
    DECLARE @older_version_id INT;
    SELECT TOP (1)
        @removed_current_id = current_version.product_id,
        @older_version_id = historical.product_id
    FROM gold.dim_products current_version
    JOIN gold.dim_products historical
      ON historical.product_number = current_version.product_number
     AND historical.product_id <> current_version.product_id
     AND historical.is_current = 0
    WHERE current_version.product_key > 0 AND current_version.is_current = 1
    ORDER BY current_version.product_key;

    IF @removed_current_id IS NULL
        THROW 53424, 'No multi-version product is available for the closed-version counterexample.', 1;

    DELETE FROM silver.crm_prd_info WHERE prd_id = @removed_current_id;

    EXEC gold.usp_load_gold @snapshot_as_of = @snapshot_as_of;

    IF NOT EXISTS (
        SELECT 1 FROM gold.dim_products
        WHERE product_id = @retired_product_id
          AND is_current = 0
          AND effective_to = @snapshot_as_of
    )
        THROW 53421, 'A product version missing from the full snapshot remained current.', 1;

    IF EXISTS (SELECT 1 FROM gold.dim_products WHERE product_id = @older_version_id AND is_current = 1)
        THROW 53425, 'Removing the newest version silently reopened an older closed version.', 1;

    IF EXISTS (
        SELECT product_number FROM gold.dim_products
        WHERE product_key > 0 AND is_current = 1
        GROUP BY product_number HAVING COUNT_BIG(*) > 1
    )
        THROW 53422, 'SCD2 reconciliation left multiple current product versions.', 1;

    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;

SELECT N'PASS' AS model_scd2_reconciliation;
GO
