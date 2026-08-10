:ON ERROR EXIT

USE DataWarehouse;
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

IF EXISTS (
    SELECT required.object_id, required.constraint_name
    FROM (VALUES
        (OBJECT_ID(N'silver.crm_cust_info'), N'CK_silver_cust_positive_id'),
        (OBJECT_ID(N'silver.crm_prd_info'), N'CK_silver_prd_positive_id'),
        (OBJECT_ID(N'silver.crm_sales_details'), N'CK_silver_sales_positive_customer_id'),
        (OBJECT_ID(N'silver.inventory_snapshot'), N'ck_silver_inventory_product_id'),
        (OBJECT_ID(N'gold.dim_customers'), N'CK_dim_customers_reserved_member'),
        (OBJECT_ID(N'gold.dim_products'), N'CK_dim_products_reserved_member')
    ) required(object_id, constraint_name)
    LEFT JOIN sys.check_constraints actual
      ON actual.parent_object_id = required.object_id
     AND actual.name = required.constraint_name
     AND actual.is_disabled = 0
     AND actual.is_not_trusted = 0
    WHERE actual.object_id IS NULL
)
    THROW 53400, 'Reserved-key constraints must exist on their exact tables, be enabled, and be trusted.', 1;

IF NOT EXISTS (
    SELECT 1 FROM gold.dim_customers
    WHERE customer_key = 0 AND customer_id = -1 AND customer_number = N'UNKNOWN'
) OR NOT EXISTS (
    SELECT 1 FROM gold.dim_products
    WHERE product_key = 0 AND product_id = -1 AND product_number = N'UNKNOWN'
)
    THROW 53401, 'Gold Unknown members do not match the protected sentinel contract.', 1;

DECLARE @rejected BIT;

SET @rejected = 0;
BEGIN TRY
    BEGIN TRANSACTION;
    INSERT silver.crm_cust_info (cust_id, cust_key) VALUES (-1, N'ATTACK');
    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    IF ERROR_NUMBER() <> 547 THROW;
    SET @rejected = 1;
END CATCH;
IF @rejected = 0 THROW 53402, 'Silver accepted a reserved customer identifier.', 1;

SET @rejected = 0;
BEGIN TRY
    BEGIN TRANSACTION;
    INSERT silver.crm_prd_info (prd_id, prd_key) VALUES (0, N'ATTACK');
    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    IF ERROR_NUMBER() <> 547 THROW;
    SET @rejected = 1;
END CATCH;
IF @rejected = 0 THROW 53403, 'Silver accepted a reserved product identifier.', 1;

SET @rejected = 0;
BEGIN TRY
    BEGIN TRANSACTION;
    UPDATE gold.dim_customers SET customer_number = N'OVERWRITTEN' WHERE customer_key = 0;
    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    IF ERROR_NUMBER() <> 547 THROW;
    SET @rejected = 1;
END CATCH;
IF @rejected = 0 THROW 53404, 'Gold allowed the protected Unknown customer to be overwritten.', 1;

SET @rejected = 0;
BEGIN TRY
    BEGIN TRANSACTION;
    UPDATE gold.dim_products SET product_number = N'OVERWRITTEN' WHERE product_key = 0;
    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    IF ERROR_NUMBER() <> 547 THROW;
    SET @rejected = 1;
END CATCH;
IF @rejected = 0 THROW 53405, 'Gold allowed the protected Unknown product to be overwritten.', 1;

SELECT N'PASS' AS model_sentinel_contract;
GO
