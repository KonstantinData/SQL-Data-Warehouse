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

IF (SELECT COUNT(*) FROM sys.columns
    WHERE object_id = OBJECT_ID(N'silver.crm_sales_details')
      AND name IN (N'sls_sales', N'sls_price')
      AND system_type_id = TYPE_ID(N'decimal') AND precision = 18 AND scale = 2) <> 2
    THROW 53410, 'Silver monetary columns must use DECIMAL(18,2).', 1;

DECLARE @large_amount DECIMAL(18,2) = CONVERT(DECIMAL(18,2),
    CONVERT(DECIMAL(28,6), 50000) * CONVERT(DECIMAL(28,6), 50000));
DECLARE @fractional_price DECIMAL(18,2) = CONVERT(DECIMAL(18,2),
    CONVERT(DECIMAL(28,6), 10) / CONVERT(DECIMAL(28,6), 3));
DECLARE @normalized_amount DECIMAL(18,2) = CONVERT(DECIMAL(18,2),
    CONVERT(DECIMAL(28,6), 3) * @fractional_price);

IF @large_amount <> CONVERT(DECIMAL(18,2), 2500000000)
    THROW 53411, 'Large monetary multiplication overflowed or lost precision.', 1;
IF @fractional_price <> CONVERT(DECIMAL(18,2), 3.33)
   OR @normalized_amount <> CONVERT(DECIMAL(18,2), 9.99)
    THROW 53412, 'Fractional unit-price normalization used integer arithmetic.', 1;

BEGIN TRANSACTION;
INSERT silver.crm_sales_details
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_sales, sls_quantity, sls_price)
VALUES
    (N'DECIMAL-CONTRACT', N'DECIMAL-CONTRACT', 1, @large_amount, 50000, 50000.00);
ROLLBACK TRANSACTION;

SELECT N'PASS' AS model_decimal_arithmetic;
GO
