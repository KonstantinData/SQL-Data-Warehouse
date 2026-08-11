:ON ERROR EXIT

USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

SELECT customer_id, customer_key INTO #customer_keys FROM gold.dim_customers;
SELECT product_id, product_key INTO #product_keys FROM gold.dim_products;
SELECT date_key, calendar_date INTO #date_keys FROM gold.dim_date;
SELECT sales_key, order_number, product_number INTO #sales_keys FROM gold.fact_sales;
GO

:r ./scripts/gold_layer/create_gold_views.sql

IF EXISTS (SELECT customer_id, customer_key FROM #customer_keys EXCEPT SELECT customer_id, customer_key FROM gold.dim_customers)
   OR EXISTS (SELECT customer_id, customer_key FROM gold.dim_customers EXCEPT SELECT customer_id, customer_key FROM #customer_keys)
   OR EXISTS (SELECT product_id, product_key FROM #product_keys EXCEPT SELECT product_id, product_key FROM gold.dim_products)
   OR EXISTS (SELECT product_id, product_key FROM gold.dim_products EXCEPT SELECT product_id, product_key FROM #product_keys)
   OR EXISTS (SELECT date_key, calendar_date FROM #date_keys EXCEPT SELECT date_key, calendar_date FROM gold.dim_date)
   OR EXISTS (SELECT date_key, calendar_date FROM gold.dim_date EXCEPT SELECT date_key, calendar_date FROM #date_keys)
   OR EXISTS (SELECT sales_key, order_number, product_number FROM #sales_keys EXCEPT SELECT sales_key, order_number, product_number FROM gold.fact_sales)
   OR EXISTS (SELECT sales_key, order_number, product_number FROM gold.fact_sales EXCEPT SELECT sales_key, order_number, product_number FROM #sales_keys)
    THROW 53200, 'First rerun changed a retained warehouse key.', 1;
GO

:r ./scripts/gold_layer/create_gold_views.sql

IF EXISTS (SELECT customer_id, customer_key FROM #customer_keys EXCEPT SELECT customer_id, customer_key FROM gold.dim_customers)
   OR EXISTS (SELECT customer_id, customer_key FROM gold.dim_customers EXCEPT SELECT customer_id, customer_key FROM #customer_keys)
   OR EXISTS (SELECT product_id, product_key FROM #product_keys EXCEPT SELECT product_id, product_key FROM gold.dim_products)
   OR EXISTS (SELECT product_id, product_key FROM gold.dim_products EXCEPT SELECT product_id, product_key FROM #product_keys)
   OR EXISTS (SELECT date_key, calendar_date FROM #date_keys EXCEPT SELECT date_key, calendar_date FROM gold.dim_date)
   OR EXISTS (SELECT date_key, calendar_date FROM gold.dim_date EXCEPT SELECT date_key, calendar_date FROM #date_keys)
   OR EXISTS (SELECT sales_key, order_number, product_number FROM #sales_keys EXCEPT SELECT sales_key, order_number, product_number FROM gold.fact_sales)
   OR EXISTS (SELECT sales_key, order_number, product_number FROM gold.fact_sales EXCEPT SELECT sales_key, order_number, product_number FROM #sales_keys)
    THROW 53201, 'Second rerun changed a retained warehouse key.', 1;

SELECT N'PASS' AS model_reproducibility;
GO
