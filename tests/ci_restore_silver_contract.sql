SET NOCOUNT ON;

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cust_id
               ORDER BY dwh_create_date, cust_key, cust_create_date
           ) AS duplicate_rank
    FROM silver.crm_cust_info
)
DELETE FROM ranked
WHERE duplicate_rank > 1;

IF @@ROWCOUNT <> 1
    THROW 51000, 'Silver negative fixture cleanup did not remove exactly one row.', 1;
