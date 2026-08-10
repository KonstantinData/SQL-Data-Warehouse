USE DataWarehouse;
GO
SET NOCOUNT ON;

DECLARE @product_key INT = (
    SELECT TOP (1) historical.product_key
    FROM gold.dim_products historical
    WHERE historical.product_key > 0
      AND historical.is_current = 0
      AND EXISTS (
          SELECT 1 FROM gold.dim_products current_version
          WHERE current_version.product_number = historical.product_number
            AND current_version.is_current = 1
      )
    ORDER BY historical.product_key
);
IF @product_key IS NULL
    THROW 51000, 'Cannot create Gold negative fixture because no historical/current product pair exists.', 1;

ALTER TABLE gold.dim_products NOCHECK CONSTRAINT CK_dim_products_current_interval;
UPDATE gold.dim_products SET is_current = 1 WHERE product_key = @product_key;
IF @@ROWCOUNT <> 1
    THROW 51001, 'Gold negative fixture did not create exactly one duplicate-current product.', 1;
GO
