SET NOCOUNT ON;

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY dwh_create_date, cat, subcat, maintenance
           ) AS duplicate_rank
    FROM silver.erp_px_cat_g1v2
)
DELETE FROM ranked
WHERE duplicate_rank > 1;

IF @@ROWCOUNT <> 1
    THROW 51000, 'Gold negative fixture cleanup did not remove exactly one category row.', 1;
