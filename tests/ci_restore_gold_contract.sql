USE DataWarehouse;
GO
SET NOCOUNT ON;

;WITH duplicated AS (
    SELECT product_key,
           ROW_NUMBER() OVER (
               PARTITION BY product_number ORDER BY effective_from DESC, product_id DESC
           ) AS version_rank
    FROM gold.dim_products
    WHERE product_key > 0 AND is_current = 1
      AND product_number IN (
          SELECT product_number FROM gold.dim_products
          WHERE product_key > 0 AND is_current = 1
          GROUP BY product_number HAVING COUNT_BIG(*) > 1
      )
)
UPDATE product SET is_current = 0
FROM gold.dim_products product
JOIN duplicated d ON d.product_key = product.product_key
WHERE d.version_rank > 1;

IF @@ROWCOUNT <> 1
    THROW 51000, 'Gold negative fixture cleanup did not restore exactly one current product.', 1;

ALTER TABLE gold.dim_products WITH CHECK CHECK CONSTRAINT CK_dim_products_current_interval;
GO
