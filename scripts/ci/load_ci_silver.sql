/*
================================================================================
CI Silver-layer loader.
================================================================================
Notes:
  - Loads a minimal set of Silver tables from Bronze so Gold views have rows.
  - Intended for CI only (keeps logic lightweight vs. production cleansing).
================================================================================
*/

USE DataWarehouse;
GO

INSERT INTO silver.crm_prd_info (
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
    prd_key,
    TRIM(prd_nm),
    CASE
        WHEN prd_cost IS NULL OR prd_cost <= 0 THEN 1
        ELSE prd_cost
    END,
    TRIM(prd_line),
    prd_start_dt,
    CASE
        WHEN prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt THEN NULL
        ELSE prd_end_dt
    END
FROM bronze.crm_prd_info;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    s.sls_ord_num,
    s.sls_prd_key,
    s.sls_cust_id,
    s.sls_order_dt,
    s.sls_ship_dt,
    s.sls_due_dt,
    s.sls_sales,
    s.sls_quantity,
    s.sls_price
FROM bronze.crm_sales_details s
INNER JOIN silver.crm_prd_info p
    ON SUBSTRING(p.prd_key, 7, LEN(p.prd_key)) = s.sls_prd_key
WHERE EXISTS (
    SELECT 1
    FROM silver.crm_cust_info c
    WHERE c.cust_id = s.sls_cust_id
);

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    cid,
    bdate,
    gen
FROM bronze.erp_cust_az12;

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101;

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
