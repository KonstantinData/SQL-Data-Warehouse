/* End-to-end execution and transformation contract for the synthetic fixtures.

   This contract intentionally fails when the authoritative runtime/model is
   incomplete. CI must not repair or substitute missing transformations. */

SET NOCOUNT ON;

DECLARE @violations INT = 0;
DECLARE @product_version NVARCHAR(128) = CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion'));

IF @product_version <> N'16.0.4265.3'
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): SQL Server version does not match the reviewed CU26 image.';
END;

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NULL
   OR OBJECT_ID('silver.crm_prd_info', 'U') IS NULL
   OR OBJECT_ID('silver.crm_sales_details', 'U') IS NULL
   OR OBJECT_ID('silver.erp_cust_az12', 'U') IS NULL
   OR OBJECT_ID('silver.erp_loc_a101', 'U') IS NULL
   OR OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): required Silver tables are missing.';
END;

IF OBJECT_ID('gold.dim_customers', 'V') IS NULL
   OR OBJECT_ID('gold.dim_products', 'V') IS NULL
   OR OBJECT_ID('gold.fact_sales', 'V') IS NULL
BEGIN
    SET @violations += 1;
    PRINT 'ERROR (Pipeline): required Gold views are missing.';
END;

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.crm_cust_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative Customer transform produced no rows.'; END;
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.crm_prd_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative Product transform produced no rows.'; END;
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.crm_sales_details)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative Sales transform is missing or produced no rows.'; END;
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.erp_cust_az12)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative ERP customer transform is missing or produced no rows.'; END;
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.erp_loc_a101)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative ERP location transform is missing or produced no rows.'; END;
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM silver.erp_px_cat_g1v2)
BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): authoritative ERP category transform is missing or produced no rows.'; END;

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
BEGIN
    SELECT
        cust_id,
        cust_key,
        TRIM(cust_firstname) AS cust_firstname,
        TRIM(cust_lastname) AS cust_lastname,
        CASE UPPER(TRIM(cust_marital_status))
            WHEN 'M' THEN 'Married'
            WHEN 'S' THEN 'Single'
            ELSE 'n/a'
        END AS cust_marital_status,
        CASE UPPER(TRIM(cust_gender))
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
        END AS cust_gender,
        cust_create_date,
        CONVERT(BIT, CASE WHEN cust_create_date > GETDATE() THEN 1 ELSE 0 END) AS cust_is_future
    INTO #expected_customers
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cust_id ORDER BY cust_create_date DESC
               ) AS latest_rank
        FROM bronze.crm_cust_info
        WHERE cust_id IS NOT NULL
    ) AS source
    WHERE latest_rank = 1;

    IF EXISTS (
        SELECT cust_id,
               CONVERT(VARBINARY(MAX), cust_key),
               CONVERT(VARBINARY(MAX), cust_firstname),
               CONVERT(VARBINARY(MAX), cust_lastname),
               CONVERT(VARBINARY(MAX), cust_marital_status),
               CONVERT(VARBINARY(MAX), cust_gender),
               cust_create_date, cust_is_future
        FROM #expected_customers
        EXCEPT
        SELECT cust_id,
               CONVERT(VARBINARY(MAX), cust_key),
               CONVERT(VARBINARY(MAX), cust_firstname),
               CONVERT(VARBINARY(MAX), cust_lastname),
               CONVERT(VARBINARY(MAX), cust_marital_status),
               CONVERT(VARBINARY(MAX), cust_gender),
               cust_create_date, cust_is_future
        FROM silver.crm_cust_info
    ) OR EXISTS (
        SELECT cust_id,
               CONVERT(VARBINARY(MAX), cust_key),
               CONVERT(VARBINARY(MAX), cust_firstname),
               CONVERT(VARBINARY(MAX), cust_lastname),
               CONVERT(VARBINARY(MAX), cust_marital_status),
               CONVERT(VARBINARY(MAX), cust_gender),
               cust_create_date, cust_is_future
        FROM silver.crm_cust_info
        EXCEPT
        SELECT cust_id,
               CONVERT(VARBINARY(MAX), cust_key),
               CONVERT(VARBINARY(MAX), cust_firstname),
               CONVERT(VARBINARY(MAX), cust_lastname),
               CONVERT(VARBINARY(MAX), cust_marital_status),
               CONVERT(VARBINARY(MAX), cust_gender),
               cust_create_date, cust_is_future
        FROM #expected_customers
    )
    BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): Customer transform output differs from authoritative semantics.'; END;
END;

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    SELECT
        prd_id,
        TRIM(prd_key) AS prd_key,
        TRIM(prd_nm) AS prd_nm,
        CASE WHEN prd_cost IS NULL OR prd_cost < 0 THEN 0 ELSE prd_cost END AS prd_cost,
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
    INTO #expected_products
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY prd_id ORDER BY prd_start_dt DESC
               ) AS latest_rank
        FROM bronze.crm_prd_info
        WHERE prd_id IS NOT NULL
    ) AS source
    WHERE latest_rank = 1;

    IF EXISTS (
        SELECT prd_id,
               CONVERT(VARBINARY(MAX), prd_key),
               CONVERT(VARBINARY(MAX), prd_nm),
               prd_cost,
               CONVERT(VARBINARY(MAX), prd_line),
               prd_start_dt, prd_end_dt
        FROM #expected_products
        EXCEPT
        SELECT prd_id,
               CONVERT(VARBINARY(MAX), prd_key),
               CONVERT(VARBINARY(MAX), prd_nm),
               prd_cost,
               CONVERT(VARBINARY(MAX), prd_line),
               prd_start_dt, prd_end_dt
        FROM silver.crm_prd_info
    ) OR EXISTS (
        SELECT prd_id,
               CONVERT(VARBINARY(MAX), prd_key),
               CONVERT(VARBINARY(MAX), prd_nm),
               prd_cost,
               CONVERT(VARBINARY(MAX), prd_line),
               prd_start_dt, prd_end_dt
        FROM silver.crm_prd_info
        EXCEPT
        SELECT prd_id,
               CONVERT(VARBINARY(MAX), prd_key),
               CONVERT(VARBINARY(MAX), prd_nm),
               prd_cost,
               CONVERT(VARBINARY(MAX), prd_line),
               prd_start_dt, prd_end_dt
        FROM #expected_products
    )
    BEGIN SET @violations += 1; PRINT 'ERROR (Pipeline): Product transform output differs from authoritative semantics.'; END;
END;

IF @violations > 0
BEGIN
    RAISERROR('Pipeline execution contract failed. Violations: %d', 16, 1, @violations);
    RETURN;
END;

PRINT 'Pipeline execution contract passed against the authoritative transformations.';
