/* Fail-closed Gold analytical contract. No violating rows are printed. */

SET NOCOUNT ON;

DECLARE @violations INT = 0;

IF OBJECT_ID('gold.dim_customers', 'V') IS NULL
   OR OBJECT_ID('gold.dim_products', 'V') IS NULL
   OR OBJECT_ID('gold.fact_sales', 'V') IS NULL
    THROW 51000, 'Gold contract failed: one or more required views are missing.', 1;

IF NOT EXISTS (SELECT 1 FROM gold.dim_customers)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): dim_customers is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM gold.dim_products)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): dim_products is empty.'; END;
IF NOT EXISTS (SELECT 1 FROM gold.fact_sales)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact_sales is empty.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_customers
    GROUP BY customer_key HAVING customer_key IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer surrogate keys must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_customers
    GROUP BY customer_id HAVING customer_id IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer business keys must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_customers
    GROUP BY customer_number HAVING customer_number IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer numbers must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_products
    GROUP BY product_key HAVING product_key IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product surrogate keys must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_products
    GROUP BY product_number HAVING product_number IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product business keys must be non-null and unique.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.dim_customers)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_cust_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer dimension row count does not preserve Silver grain.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.dim_products)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_prd_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product dimension row count does not preserve Silver grain.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.fact_sales)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_sales_details)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact row count does not preserve Silver sales grain.'; END;

IF EXISTS (
    SELECT 1 FROM gold.fact_sales
    GROUP BY order_number, product_key
    HAVING order_number IS NULL OR product_key IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact grain must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1
    FROM gold.fact_sales AS facts
    LEFT JOIN gold.dim_customers AS customers
        ON customers.customer_key = facts.customer_key
    LEFT JOIN gold.dim_products AS products
        ON products.product_key = facts.product_key
    WHERE customers.customer_key IS NULL OR products.product_key IS NULL
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact keys must resolve to both dimensions.'; END;

IF EXISTS (
    SELECT 1 FROM gold.fact_sales
    WHERE order_date IS NULL
       OR ship_date IS NULL
       OR due_date IS NULL
       OR order_date > ship_date
       OR order_date > due_date
       OR sales_amount IS NULL
       OR quantity IS NULL
       OR price IS NULL
       OR sales_amount <= 0
       OR quantity <= 0
       OR price <= 0
       OR CONVERT(BIGINT, sales_amount) <> CONVERT(BIGINT, quantity) * CONVERT(BIGINT, price)
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact date or measure contract failed.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_customers
    WHERE last_name_hash IS NOT NULL
      AND (LEN(last_name_hash) <> 64
           OR last_name_hash COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9A-F]%')
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): last_name_hash must be 64 uppercase hexadecimal characters.'; END;

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('gold.dim_customers')
      AND name IN ('cust_lastname', 'last_name')
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): raw customer last names must not be exposed.'; END;

IF @violations > 0
BEGIN
    RAISERROR('Gold quality contract failed. Violations: %d', 16, 1, @violations);
    RETURN;
END;

PRINT 'Gold quality contract passed.';
