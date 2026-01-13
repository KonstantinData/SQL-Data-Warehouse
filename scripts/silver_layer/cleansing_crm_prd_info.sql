/*
**Script Overview:**
======================================================================================================================
This SQL script cleanses product records from `bronze.crm_prd_info` and loads them into `silver.crm_prd_info`.
It ensures data consistency by:

- Removing duplicate product IDs (keeping the most recent start date).
- Standardizing product line labels.
- Trimming unwanted spaces in product identifiers and names.
- Defaulting missing/invalid costs to 0.
- Nulling invalid date ranges where end dates precede start dates.

This script is part of an ETL process, ensuring accurate and up-to-date product data for analytics and reporting. 🚀
=======================================================================================================================
*/

INSERT INTO DataWarehouse.silver.crm_prd_info (
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    TRIM(prd_key) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    CASE
        WHEN prd_cost IS NULL OR prd_cost < 0 THEN 0
        ELSE prd_cost
    END AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE TRIM(prd_line)
    END AS prd_line,
    prd_start_dt,
    CASE
        WHEN prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt THEN NULL
        ELSE prd_end_dt
    END AS prd_end_dt
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS flag_last
    FROM bronze.crm_prd_info
    WHERE prd_id IS NOT NULL
) latest_record
WHERE flag_last = 1;
