SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

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
