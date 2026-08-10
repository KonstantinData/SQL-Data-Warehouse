SET NOCOUNT ON;

IF NOT EXISTS (
    SELECT 1
    FROM silver.erp_px_cat_g1v2 AS category
    WHERE EXISTS (
        SELECT 1
        FROM silver.crm_prd_info AS product
        WHERE REPLACE(SUBSTRING(product.prd_key, 1, 5), '-', '_') = category.id
    )
)
    THROW 51000, 'Cannot create Gold negative fixture because no category joins a product.', 1;

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance, dwh_create_date)
SELECT TOP (1) id, cat, subcat, maintenance, dwh_create_date
FROM silver.erp_px_cat_g1v2 AS category
WHERE EXISTS (
    SELECT 1
    FROM silver.crm_prd_info AS product
    WHERE REPLACE(SUBSTRING(product.prd_key, 1, 5), '-', '_') = category.id
)
ORDER BY id;
