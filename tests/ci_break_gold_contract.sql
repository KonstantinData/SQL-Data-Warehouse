USE DataWarehouse;
GO
SET NOCOUNT ON;

DECLARE @product_key INT = (
    SELECT TOP (1) product_key
    FROM gold.dim_products
    WHERE product_key <> 0 AND is_current = 1
    ORDER BY product_key
);
IF @product_key IS NULL
    THROW 51000, 'Cannot create Gold negative fixture because no current product exists.', 1;

UPDATE gold.dim_products SET is_current = 0 WHERE product_key = @product_key;
IF @@ROWCOUNT <> 1
    THROW 51001, 'Gold negative fixture did not mutate exactly one product.', 1;
GO
