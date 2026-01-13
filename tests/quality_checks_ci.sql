/* ========================================================================
   CI Data Quality Checks
   ------------------------------------------------------------------------
   Purpose:
     - Run data quality checks across Bronze, Silver, and Gold layers.
     - Bronze: diagnostics only (raw data is allowed to be dirty) → WARNINGS.
     - Silver & Gold: must be clean → violations fail CI via RAISERROR.

   Assumptions:
     - Database context is already set to DataWarehouse
       (e.g. via sqlcmd -d DataWarehouse in CI).
   ======================================================================== */

SET NOCOUNT ON;

DECLARE @error_count INT = 0;

/* ========================================================================
   BRONZE LAYER CHECKS  (diagnostic only – raw data can be dirty)
   ======================================================================== */

PRINT '====================================================================';
PRINT 'BRONZE LAYER DATA QUALITY (DIAGNOSTIC ONLY – WARNINGS, NO CI FAIL)';
PRINT '====================================================================';

-- 1) Duplicate or NULL customer IDs in bronze.crm_cust_info
IF EXISTS (
    SELECT 1
    FROM bronze.crm_cust_info
    GROUP BY cust_id
    HAVING COUNT(*) > 1 OR cust_id IS NULL
)
BEGIN
    PRINT 'WARNING (Bronze): bronze.crm_cust_info has duplicate or NULL cust_id values.';
END

-- 2) NULL or non-positive product costs in bronze.crm_prd_info
IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost <= 0
)
BEGIN
    PRINT 'WARNING (Bronze): bronze.crm_prd_info has NULL or non-positive prd_cost values.';
END

-- 3) Invalid product date ranges in bronze.crm_prd_info
IF EXISTS (
    SELECT 1
    FROM bronze.crm_prd_info
    WHERE prd_end_dt IS NOT NULL
      AND prd_end_dt < prd_start_dt
)
BEGIN
    PRINT 'WARNING (Bronze): bronze.crm_prd_info has invalid date ranges (prd_end_dt < prd_start_dt).';
END

-- 4) Inconsistent sales calculation in bronze.crm_sales_details
IF EXISTS (
    SELECT 1
    FROM bronze.crm_sales_details
    WHERE sls_sales <> sls_quantity * sls_price
)
BEGIN
    PRINT 'WARNING (Bronze): bronze.crm_sales_details has inconsistent sales calculations (sls_sales <> quantity * price).';
END

-- 5) Out-of-range birthdates in bronze.erp_cust_az12
IF EXISTS (
    SELECT 1
    FROM bronze.erp_cust_az12
    WHERE bdate < '1924-01-01'
       OR bdate > CAST(GETDATE() AS DATE)
)
BEGIN
    PRINT 'WARNING (Bronze): bronze.erp_cust_az12 has out-of-range birthdates (< 1924-01-01 or > today).';
END

PRINT 'Bronze checks completed (diagnostic only).';
PRINT '====================================================================';
PRINT '';
PRINT '';


/* ========================================================================
   SILVER LAYER CHECKS (hard – CI should fail if violations exist)
   ======================================================================== */

PRINT '====================================================================';
PRINT 'SILVER LAYER DATA QUALITY (ENFORCED – VIOLATIONS WILL FAIL CI)';
PRINT '====================================================================';

-- Existenz der wichtigsten Silver-Tabellen
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Silver): Table silver.crm_cust_info does not exist.';
END

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Silver): Table silver.crm_prd_info does not exist.';
END

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Silver): Table silver.crm_sales_details does not exist.';
END

-- Nur inhaltliche Checks, wenn Tabellen existieren
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
BEGIN
    -- Keine NULL-cust_id in Silver
    IF EXISTS (
        SELECT 1
        FROM silver.crm_cust_info
        WHERE cust_id IS NULL
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_cust_info has NULL cust_id values.';
    END

    -- Keine Duplikate in Silver
    IF EXISTS (
        SELECT 1
        FROM silver.crm_cust_info
        GROUP BY cust_id
        HAVING COUNT(*) > 1
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_cust_info has duplicate cust_id values.';
    END
END

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    -- Keine NULL/negativen Kosten in Silver
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        WHERE prd_cost IS NULL OR prd_cost <= 0
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_prd_info has NULL or non-positive prd_cost values.';
    END

    -- Keine invaliden Datumsbereiche mehr in Silver
    IF EXISTS (
        SELECT 1
        FROM silver.crm_prd_info
        WHERE prd_end_dt IS NOT NULL
          AND prd_end_dt < prd_start_dt
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_prd_info has invalid date ranges (prd_end_dt < prd_start_dt).';
    END
END

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
BEGIN
    -- Referentielle Integrität: Customer muss existieren
    IF EXISTS (
        SELECT 1
        FROM silver.crm_sales_details s
        LEFT JOIN silver.crm_cust_info c
            ON s.sls_cust_id = c.cust_id
        WHERE c.cust_id IS NULL
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_sales_details has sales rows without matching customer in silver.crm_cust_info.';
    END

    -- Referentielle Integrität: Product muss existieren
    IF EXISTS (
        SELECT 1
        FROM silver.crm_sales_details s
        LEFT JOIN silver.crm_prd_info p
            ON s.sls_prd_key = p.prd_key
        WHERE p.prd_key IS NULL
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Silver): silver.crm_sales_details has sales rows without matching product in silver.crm_prd_info.';
    END
END

PRINT 'Silver checks completed.';
PRINT '====================================================================';
PRINT '';
PRINT '';


/* ========================================================================
   GOLD LAYER CHECKS (ENFORCED – VIOLATIONS WILL FAIL CI)
   ======================================================================== */

PRINT '====================================================================';
PRINT 'GOLD LAYER DATA QUALITY (ENFORCED – VIOLATIONS WILL FAIL CI)';
PRINT '====================================================================';

-- Gold-Objekte laut create_gold_views.sql:
--   gold.dim_customers
--   gold.dim_products
--   gold.fact_sales

-- 1) Existenz der Views/Tabellen in Gold
IF OBJECT_ID('gold.dim_customers', 'V') IS NULL AND OBJECT_ID('gold.dim_customers', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Gold): gold.dim_customers does not exist (as view or table).';
END

IF OBJECT_ID('gold.dim_products', 'V') IS NULL AND OBJECT_ID('gold.dim_products', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Gold): gold.dim_products does not exist (as view or table).';
END

IF OBJECT_ID('gold.fact_sales', 'V') IS NULL AND OBJECT_ID('gold.fact_sales', 'U') IS NULL
BEGIN
    SET @error_count += 1;
    PRINT 'ERROR (Gold): gold.fact_sales does not exist (as view or table).';
END

-- 2) Optional: einfache inhaltliche Checks (nur wenn Objekte existieren)

IF (OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL OR OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM gold.dim_customers)
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Gold): gold.dim_customers returns no rows.';
    END
END

IF (OBJECT_ID('gold.dim_products', 'V') IS NOT NULL OR OBJECT_ID('gold.dim_products', 'U') IS NOT NULL)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM gold.dim_products)
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Gold): gold.dim_products returns no rows.';
    END
END

IF (OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL OR OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM gold.fact_sales)
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Gold): gold.fact_sales returns no rows.';
    END
END

PRINT 'Gold checks completed.';
PRINT '====================================================================';
PRINT '';
PRINT '';


/* ========================================================================
   FINAL RESULT
   ======================================================================== */

IF @error_count > 0
BEGIN
    RAISERROR('CI quality checks failed. Violations: %d', 16, 1, @error_count);
END
ELSE
BEGIN
    PRINT 'All CI quality checks passed (Silver/Gold). Bronze issues (if any) are diagnostics only.';
END
