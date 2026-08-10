/* Bronze data-content diagnostics.

   Raw source anomalies are expected and do not fail CI. Missing objects or SQL
   execution errors still fail because they are operational failures, not data
   quality observations. Only aggregate counts are written to logs. */

SET NOCOUNT ON;

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
   OR OBJECT_ID('bronze.crm_prd_info', 'U') IS NULL
   OR OBJECT_ID('bronze.crm_sales_details', 'U') IS NULL
   OR OBJECT_ID('bronze.erp_cust_az12', 'U') IS NULL
   OR OBJECT_ID('bronze.erp_loc_a101', 'U') IS NULL
   OR OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NULL
    THROW 51000, 'Bronze diagnostics cannot run because a required table is missing.', 1;

DECLARE @count BIGINT;

SELECT @count = COUNT_BIG(*)
FROM (
    SELECT cust_id
    FROM bronze.crm_cust_info
    GROUP BY cust_id
    HAVING cust_id IS NULL OR COUNT_BIG(*) > 1
) AS violations;
IF @count > 0
    PRINT 'WARNING (Bronze): duplicate_or_null_customer_id count=' + CONVERT(VARCHAR(30), @count);

SELECT @count = COUNT_BIG(*)
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;
IF @count > 0
    PRINT 'WARNING (Bronze): null_or_negative_product_cost count=' + CONVERT(VARCHAR(30), @count);

SELECT @count = COUNT_BIG(*)
FROM bronze.crm_prd_info
WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;
IF @count > 0
    PRINT 'WARNING (Bronze): invalid_product_date_range count=' + CONVERT(VARCHAR(30), @count);

SELECT @count = COUNT_BIG(*)
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales <> sls_quantity * sls_price;
IF @count > 0
    PRINT 'WARNING (Bronze): invalid_sales_measure count=' + CONVERT(VARCHAR(30), @count);

SELECT @count = COUNT_BIG(*)
FROM bronze.crm_sales_details
WHERE TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_order_dt, 0)), 112) IS NULL
   OR TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_ship_dt, 0)), 112) IS NULL
   OR TRY_CONVERT(DATE, CONVERT(CHAR(8), NULLIF(sls_due_dt, 0)), 112) IS NULL;
IF @count > 0
    PRINT 'WARNING (Bronze): invalid_sales_date count=' + CONVERT(VARCHAR(30), @count);

SELECT @count = COUNT_BIG(*)
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CAST(GETDATE() AS DATE);
IF @count > 0
    PRINT 'WARNING (Bronze): out_of_range_birth_date count=' + CONVERT(VARCHAR(30), @count);

PRINT 'Bronze data-content diagnostics completed without enforcing source cleanup.';
