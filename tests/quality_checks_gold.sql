/* Fail-closed contract for the materialized Gold star schema. */

USE DataWarehouse;
GO
SET NOCOUNT ON;

DECLARE @violations INT = 0;

IF OBJECT_ID('gold.dim_customers', 'U') IS NULL
   OR OBJECT_ID('gold.dim_products', 'U') IS NULL
   OR OBJECT_ID('gold.dim_date', 'U') IS NULL
   OR OBJECT_ID('gold.fact_sales', 'U') IS NULL
    THROW 51000, 'Gold contract failed: one or more physical star-schema tables are missing.', 1;

IF (SELECT COUNT(*) FROM gold.dim_customers WHERE customer_key = 0) <> 1
   OR (SELECT COUNT(*) FROM gold.dim_products WHERE product_key = 0) <> 1
   OR (SELECT COUNT(*) FROM gold.dim_date WHERE date_key = 0) <> 1
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): every dimension requires exactly one Unknown member.'; END;

IF EXISTS (SELECT customer_key FROM gold.dim_customers GROUP BY customer_key HAVING COUNT_BIG(*) > 1)
   OR EXISTS (SELECT product_key FROM gold.dim_products GROUP BY product_key HAVING COUNT_BIG(*) > 1)
   OR EXISTS (SELECT date_key FROM gold.dim_date GROUP BY date_key HAVING COUNT_BIG(*) > 1)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): surrogate keys must be unique.'; END;

IF EXISTS (
    SELECT customer_id FROM gold.dim_customers WHERE customer_key <> 0
    GROUP BY customer_id HAVING customer_id IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer business keys must be unique.'; END;

IF EXISTS (
    SELECT product_id FROM gold.dim_products WHERE product_key <> 0
    GROUP BY product_id HAVING product_id IS NULL OR COUNT_BIG(*) > 1
)
   OR EXISTS (
    SELECT product_number, effective_from FROM gold.dim_products WHERE product_key <> 0
    GROUP BY product_number, effective_from HAVING COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product version keys must be unique.'; END;

IF EXISTS (
    SELECT product_number
    FROM gold.dim_products
    WHERE product_key <> 0 AND is_current = 1
    GROUP BY product_number
    HAVING COUNT_BIG(*) > 1
)
   OR EXISTS (
    SELECT 1 FROM gold.dim_products
    WHERE product_key <> 0
      AND ((is_current = 1 AND effective_to IS NOT NULL)
           OR (is_current = 0 AND effective_to IS NULL))
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product current flags or effective intervals are inconsistent.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.dim_customers WHERE customer_key <> 0)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_cust_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): customer dimension row count does not preserve Silver grain.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.dim_products WHERE product_key <> 0)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_prd_info)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): product dimension row count does not preserve Silver version grain.'; END;

IF (SELECT COUNT_BIG(*) FROM gold.fact_sales)
   <> (SELECT COUNT_BIG(*) FROM silver.crm_sales_details)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact row count does not preserve validated Silver sales grain.'; END;

IF EXISTS (
    SELECT order_number, product_number
    FROM gold.fact_sales
    GROUP BY order_number, product_number
    HAVING order_number IS NULL OR product_number IS NULL OR COUNT_BIG(*) > 1
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact source grain must be non-null and unique.'; END;

IF EXISTS (
    SELECT 1
    FROM gold.fact_sales AS fact
    LEFT JOIN gold.dim_customers AS customer ON customer.customer_key = fact.customer_key
    LEFT JOIN gold.dim_products AS product ON product.product_key = fact.product_key
    LEFT JOIN gold.dim_date AS order_date ON order_date.date_key = fact.order_date_key
    LEFT JOIN gold.dim_date AS ship_date ON ship_date.date_key = fact.ship_date_key
    LEFT JOIN gold.dim_date AS due_date ON due_date.date_key = fact.due_date_key
    WHERE customer.customer_key IS NULL OR product.product_key IS NULL
       OR order_date.date_key IS NULL OR ship_date.date_key IS NULL OR due_date.date_key IS NULL
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): fact foreign keys must resolve.'; END;

IF EXISTS (
    SELECT 1 FROM gold.dim_customers
    WHERE last_name_hash IS NOT NULL
      AND (LEN(last_name_hash) <> 64 OR last_name_hash COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9A-F]%')
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): last_name_hash must be uppercase SHA-256 hexadecimal.'; END;

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('gold.dim_customers') AND name IN ('cust_lastname', 'last_name')
)
BEGIN SET @violations += 1; PRINT 'ERROR (Gold): raw customer last names must not be exposed.'; END;

IF @violations > 0
    THROW 51001, 'Gold quality contract failed.', 1;

PRINT 'Gold quality contract passed.';
GO
