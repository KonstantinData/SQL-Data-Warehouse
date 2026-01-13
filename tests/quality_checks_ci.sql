/* ========================================================================
   CI Data Quality Checks
   ------------------------------------------------------------------------
   Purpose:
     - Run data quality checks across Bronze, Silver, and Gold layers.
     - Bronze: diagnostics only (raw data is allowed to be dirty) → WARNINGS.
     - Silver & Gold: must be clean → violations fail CI via RAISERROR.

   Assumptions:
     - Database context is already set to DataWarehouse (e.g. via sqlcmd -d).
     - Tables/Schemata:
         Bronze: bronze.crm_cust_info,
                 bronze.crm_prd_info,
                 bronze.crm_sales_details,
                 bronze.erp_cust_az12
         Silver: silver.crm_cust_info,
                 silver.crm_prd_info,
                 silver.crm_sales_details
       (Passe Namen an, falls dein Schema abweicht.)
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

-- 2) NULL or negative product costs in bronze.crm_prd_info
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


/* ========================================================================
   SILVER LAYER CHECKS (hard – CI should fail if violations exist)
   ======================================================================== */

PRINT '====================================================================';
PRINT 'SILVER LAYER DATA QUALITY (ENFORCED – VIOLATIONS WILL FAIL CI)';
PRINT '====================================================================';

-- Beispiel: Existenz der wichtigsten Silver-Tabellen
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

-- Nur weiterführende Checks ausführen, wenn die Tabellen existieren
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

    -- Optional: Duplikate in Silver sollten nicht mehr vorkommen
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


/* ========================================================================
   GOLD LAYER CHECKS (optional – anpassen an deine Views/Marts)
   ======================================================================== */

PRINT '====================================================================';
PRINT 'GOLD LAYER DATA QUALITY (ENFORCED – VIOLATIONS WILL FAIL CI)';
PRINT '====================================================================';

-- Beispiel: Prüfen, dass eine zentrale Gold-View existiert.
-- Passe die Namen an deine tatsächlichen Views an (oder kommentiere diesen
-- Block aus, bis du deine Gold-Views final definiert hast).

IF OBJECT_ID('gold.vw_customer_sales', 'V') IS NULL
BEGIN
    -- Wenn du diese View (noch) nicht hast, entweder den Namen anpassen
    -- oder diesen Block vorerst auskommentieren.
    SET @error_count += 1;
    PRINT 'ERROR (Gold): View gold.vw_customer_sales does not exist.';
END
ELSE
BEGIN
    -- Beispiel-Check: Es sollte mindestens eine Zeile geben
    IF NOT EXISTS (
        SELECT 1
        FROM gold.vw_customer_sales
    )
    BEGIN
        SET @error_count += 1;
        PRINT 'ERROR (Gold): gold.vw_customer_sales returns no rows.';
    END
END

PRINT 'Gold checks completed.';
PRINT '====================================================================';
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
