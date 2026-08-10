/* Operational contract: Bronze availability is enforced even though Bronze
   content anomalies are diagnostic. This compensates for the current loader
   procedure printing caught errors without rethrowing them. */

SET NOCOUNT ON;

DECLARE @violations INT = 0;

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.crm_cust_info.';
END;
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.crm_prd_info.';
END;
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.crm_sales_details.';
END;
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.erp_cust_az12.';
END;
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.erp_loc_a101.';
END;
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): missing bronze.erp_px_cat_g1v2.';
END;

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.crm_cust_info)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.crm_cust_info is empty.';
END;
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.crm_prd_info)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.crm_prd_info is empty.';
END;
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.crm_sales_details)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.crm_sales_details is empty.';
END;
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.erp_cust_az12)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.erp_cust_az12 is empty.';
END;
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.erp_loc_a101)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.erp_loc_a101 is empty.';
END;
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM bronze.erp_px_cat_g1v2)
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): bronze.erp_px_cat_g1v2 is empty.';
END;

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.crm_cust_info) <> $(EXPECTED_BRONZE_CRM_CUST_INFO)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.crm_cust_info row count differs from its CSV fixture.'; END;
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.crm_prd_info) <> $(EXPECTED_BRONZE_CRM_PRD_INFO)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.crm_prd_info row count differs from its CSV fixture.'; END;
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.crm_sales_details) <> $(EXPECTED_BRONZE_CRM_SALES_DETAILS)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.crm_sales_details row count differs from its CSV fixture.'; END;
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.erp_cust_az12) <> $(EXPECTED_BRONZE_ERP_CUST_AZ12)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.erp_cust_az12 row count differs from its CSV fixture.'; END;
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.erp_loc_a101) <> $(EXPECTED_BRONZE_ERP_LOC_A101)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.erp_loc_a101 row count differs from its CSV fixture.'; END;
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   AND (SELECT COUNT_BIG(*) FROM bronze.erp_px_cat_g1v2) <> $(EXPECTED_BRONZE_ERP_PX_CAT_G1V2)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): bronze.erp_px_cat_g1v2 row count differs from its CSV fixture.'; END;

IF @violations > 0
BEGIN
    RAISERROR('Bronze load contract failed. Violations: %d', 16, 1, @violations);
    RETURN;
END;

PRINT 'Bronze load contract passed: all six source tables are present and non-empty.';
