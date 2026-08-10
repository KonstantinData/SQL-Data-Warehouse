USE DataWarehouse;
GO
SET NOCOUNT ON;

;WITH missing_current AS (
    SELECT product_number
    FROM gold.dim_products
    WHERE product_key <> 0
    GROUP BY product_number
    HAVING SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) = 0
), latest_version AS (
    SELECT product_key,
           ROW_NUMBER() OVER (
               PARTITION BY product_number ORDER BY effective_from DESC, product_id DESC
           ) AS version_rank
    FROM gold.dim_products
    WHERE product_number IN (SELECT product_number FROM missing_current)
)
UPDATE product
SET is_current = 1
FROM gold.dim_products AS product
INNER JOIN latest_version AS latest ON latest.product_key = product.product_key
WHERE latest.version_rank = 1;

IF @@ROWCOUNT <> 1
    THROW 51000, 'Gold negative fixture cleanup did not restore exactly one current product.', 1;
GO
