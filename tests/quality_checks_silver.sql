/* Fail-closed Silver data contract. No violating rows are printed. */

SET NOCOUNT ON;

DECLARE @violations INT = 0;

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NULL
   OR OBJECT_ID('silver.crm_prd_info', 'U') IS NULL
   OR OBJECT_ID('silver.crm_sales_details', 'U') IS NULL
   OR OBJECT_ID('silver.erp_cust_az12', 'U') IS NULL
   OR OBJECT_ID('silver.erp_loc_a101', 'U') IS NULL
   OR OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NULL
    THROW 51000, 'Silver contract failed: one or more required tables are missing.', 1;

IF NOT EXISTS (SELECT 1 FROM silver.crm_cust_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): crm_cust_info is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM silver.crm_prd_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): crm_prd_info is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM silver.crm_sales_details)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): crm_sales_details is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM silver.erp_cust_az12)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): erp_cust_az12 is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM silver.erp_loc_a101)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): erp_loc_a101 is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM silver.erp_px_cat_g1v2)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): erp_px_cat_g1v2 is empty.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_cust_info
    GROUP BY cust_id HAVING cust_id IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): customer IDs must be non-null and unique.'; END;

IF (SELECT COUNT_BIG(*) FROM silver.crm_cust_info)
   <> (SELECT COUNT_BIG(DISTINCT cust_id) FROM bronze.crm_cust_info WHERE cust_id IS NOT NULL)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): customer lineage count does not match deduplicated Bronze.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_cust_info
    WHERE cust_key IS NULL
       OR cust_firstname IS NULL
       OR cust_lastname IS NULL
       OR cust_marital_status IS NULL
       OR cust_gender IS NULL
       OR cust_is_future IS NULL
       OR DATALENGTH(cust_key) <> DATALENGTH(TRIM(cust_key))
       OR DATALENGTH(cust_firstname) <> DATALENGTH(TRIM(cust_firstname))
       OR DATALENGTH(cust_lastname) <> DATALENGTH(TRIM(cust_lastname))
       OR cust_marital_status COLLATE Latin1_General_100_BIN2 NOT IN ('Married', 'Single', 'n/a')
       OR cust_gender COLLATE Latin1_General_100_BIN2 NOT IN ('Male', 'Female', 'n/a')
       OR cust_is_future <> CASE WHEN cust_create_date > GETDATE() THEN 1 ELSE 0 END
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): customer normalization contract failed.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_prd_info
    GROUP BY prd_id HAVING prd_id IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): product IDs must be non-null and unique.'; END;

IF (SELECT COUNT_BIG(*) FROM silver.crm_prd_info)
   <> (SELECT COUNT_BIG(*) FROM bronze.crm_prd_info WHERE prd_id IS NOT NULL AND (prd_cost IS NULL OR prd_cost >= 0))
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): product-version lineage count does not match accepted Bronze.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_prd_info
    WHERE prd_key IS NULL
       OR DATALENGTH(prd_key) <> DATALENGTH(TRIM(prd_key))
       OR prd_nm IS NULL
       OR DATALENGTH(prd_nm) <> DATALENGTH(TRIM(prd_nm))
       OR prd_cost < 0
       OR (prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt)
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): product normalization contract failed.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details
    GROUP BY sls_ord_num, sls_prd_key
    HAVING sls_ord_num IS NULL OR sls_prd_key IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): sales grain must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details
    WHERE TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_order_dt, 0)), 112) IS NULL
       OR TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_ship_dt, 0)), 112) IS NULL
       OR TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_due_dt, 0)), 112) IS NULL
       OR sls_order_dt > sls_ship_dt
       OR sls_order_dt > sls_due_dt
       OR sls_sales IS NULL
       OR sls_quantity IS NULL
       OR sls_price IS NULL
       OR sls_sales <= 0
       OR sls_quantity <= 0
       OR sls_price <= 0
       OR CONVERT(DECIMAL(28,2), sls_sales)
          <> CONVERT(DECIMAL(28,2), sls_quantity) * CONVERT(DECIMAL(28,2), sls_price)
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): sales date or measure contract failed.'; END;

IF EXISTS (
    SELECT 1
    FROM silver.crm_sales_details AS sales
    LEFT JOIN silver.crm_cust_info AS customers
        ON customers.cust_id = sales.sls_cust_id
    WHERE customers.cust_id IS NULL
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): sales must resolve to a customer.'; END;

IF EXISTS (
    SELECT 1
    FROM silver.crm_sales_details AS sales
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.crm_prd_info AS product
        WHERE SUBSTRING(product.prd_key, 7, LEN(product.prd_key)) = sales.sls_prd_key
    )
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): every sale must reference a known product history.'; END;

IF EXISTS (
    SELECT 1 FROM silver.erp_cust_az12
    GROUP BY RIGHT(cid, 10) HAVING RIGHT(cid, 10) IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): normalized ERP customer keys must be unique.'; END;

IF EXISTS (
    SELECT 1 FROM silver.erp_loc_a101
    GROUP BY REPLACE(cid, '-', '')
    HAVING REPLACE(cid, '-', '') IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): normalized ERP location keys must be unique.'; END;

IF EXISTS (
    SELECT 1 FROM silver.erp_px_cat_g1v2
    GROUP BY id HAVING id IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Silver): ERP category IDs must be non-null and unique.'; END;

IF @violations > 0
BEGIN
    RAISERROR('Silver quality contract failed. Violations: %d', 16, 1, @violations);
    RETURN;
END;

PRINT 'Silver quality contract passed.';
