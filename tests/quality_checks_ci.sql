/*
===============================================================================
CI Quality Checks (Fail-fast)
===============================================================================
Script Purpose:
    Validates critical expectations across bronze, silver, and gold layers and
    fails the run if any violations are found. This script is intended for
    automated CI execution.
===============================================================================
*/

SET NOCOUNT ON;

DECLARE @error_count INT = 0;

-- ====================================================================
-- Bronze layer checks (expect zero rows)
-- ====================================================================
IF EXISTS (
    SELECT 1
    FROM bronze.crm_cust_info
    GROUP BY cust_id
    HAVING COUNT(*) > 1 OR cust_id IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_cust_info has duplicate or NULL cust_id values.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_cust_info
    WHERE cust_key != TRIM(cust_key)
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_cust_info has cust_key values with unwanted spaces.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_prd_info has duplicate or NULL prd_id values.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm)
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_prd_info has prd_nm values with unwanted spaces.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_prd_info has NULL or negative prd_cost values.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    WHERE prd_end_dt < prd_start_dt
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_prd_info has invalid date ranges.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_sales_details
    WHERE sls_due_dt <= 0
        OR LEN(sls_due_dt) != 8
        OR sls_due_dt > 20500101
        OR sls_due_dt < 19000101
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_sales_details has invalid due dates.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt
       OR sls_order_dt > sls_due_dt
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_sales_details has invalid order vs ship/due dates.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL
       OR sls_quantity IS NULL
       OR sls_price IS NULL
       OR sls_sales <= 0
       OR sls_quantity <= 0
       OR sls_price <= 0
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.crm_sales_details has inconsistent sales calculations.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.erp_cust_az12
    WHERE bdate < '1924-01-01'
       OR bdate > GETDATE()
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.erp_cust_az12 has out-of-range birthdates.';
END

IF EXISTS (
    SELECT 1
    FROM bronze.erp_px_cat_g1v2
    WHERE cat != TRIM(cat)
       OR subcat != TRIM(subcat)
       OR maintenance != TRIM(maintenance)
)
BEGIN
    SET @error_count += 1;
    PRINT 'bronze.erp_px_cat_g1v2 has unwanted spaces.';
END

-- ====================================================================
-- Silver layer checks (expect zero rows)
-- ====================================================================
IF EXISTS (
    SELECT 1
    FROM silver.crm_cust_info
    GROUP BY cust_id
    HAVING COUNT(*) > 1 OR cust_id IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_cust_info has duplicate or NULL cust_id values.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_cust_info
    WHERE cust_key != TRIM(cust_key)
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_cust_info has cust_key values with unwanted spaces.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_prd_info has duplicate or NULL prd_id values.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm)
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_prd_info has prd_nm values with unwanted spaces.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_prd_info has NULL or negative prd_cost values.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_prd_info
    WHERE prd_end_dt < prd_start_dt
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_prd_info has invalid date ranges.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_sales_details
    WHERE sls_due_dt <= 0
        OR LEN(sls_due_dt) != 8
        OR sls_due_dt > 20500101
        OR sls_due_dt < 19000101
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_sales_details has invalid due dates.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt
       OR sls_order_dt > sls_due_dt
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_sales_details has invalid order vs ship/due dates.';
END

IF EXISTS (
    SELECT 1
    FROM silver.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL
       OR sls_quantity IS NULL
       OR sls_price IS NULL
       OR sls_sales <= 0
       OR sls_quantity <= 0
       OR sls_price <= 0
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.crm_sales_details has inconsistent sales calculations.';
END

IF EXISTS (
    SELECT 1
    FROM silver.erp_cust_az12
    WHERE bdate < '1924-01-01'
       OR bdate > GETDATE()
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.erp_cust_az12 has out-of-range birthdates.';
END

IF EXISTS (
    SELECT 1
    FROM silver.erp_px_cat_g1v2
    WHERE cat != TRIM(cat)
       OR subcat != TRIM(subcat)
       OR maintenance != TRIM(maintenance)
)
BEGIN
    SET @error_count += 1;
    PRINT 'silver.erp_px_cat_g1v2 has unwanted spaces.';
END

-- ====================================================================
-- Gold layer checks (expect zero rows)
-- ====================================================================
IF EXISTS (
    SELECT 1
    FROM gold.dim_customers
    GROUP BY customer_key
    HAVING COUNT(*) > 1
)
BEGIN
    SET @error_count += 1;
    PRINT 'gold.dim_customers has duplicate customer_key values.';
END

IF EXISTS (
    SELECT 1
    FROM gold.dim_products
    GROUP BY product_key
    HAVING COUNT(*) > 1
)
BEGIN
    SET @error_count += 1;
    PRINT 'gold.dim_products has duplicate product_key values.';
END

IF EXISTS (
    SELECT 1
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    WHERE p.product_key IS NULL OR c.customer_key IS NULL
)
BEGIN
    SET @error_count += 1;
    PRINT 'gold.fact_sales has orphaned dimension keys.';
END

IF @error_count > 0
BEGIN
    RAISERROR('CI quality checks failed. Violations: %d', 16, 1, @error_count);
END
