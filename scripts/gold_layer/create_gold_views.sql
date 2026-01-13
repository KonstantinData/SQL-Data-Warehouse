/*
======================================================================================================================
Gold Layer Views
======================================================================================================================
This script creates the presentation layer (Gold) views for the DataWarehouse.

Views created:
- gold.dim_customers
- gold.dim_products
- gold.fact_sales

The views:
- Join CRM and ERP data from the Silver layer.
- Generate surrogate keys with ROW_NUMBER().
- Provide analysis-ready dimensions and facts.
======================================================================================================================
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
WITH customer_base AS (
    SELECT
        c.cust_id,
        c.cust_key,
        TRIM(c.cust_firstname) AS cust_firstname,
        TRIM(c.cust_lastname) AS cust_lastname,
        c.cust_marital_status,
        c.cust_gender,
        c.cust_create_date,
        c.cust_is_future,
        e.bdate,
        e.gen,
        l.cntry
    FROM silver.crm_cust_info c
    LEFT JOIN silver.erp_cust_az12 e
        ON RIGHT(e.cid, 10) = c.cust_key
    LEFT JOIN silver.erp_loc_a101 l
        ON REPLACE(l.cid, '-', '') = c.cust_key
)
SELECT
    ROW_NUMBER() OVER (ORDER BY cust_id) AS customer_key,
    cust_id AS customer_id,
    cust_key AS customer_number,
    cust_firstname AS first_name,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', cust_lastname), 2) AS last_name_hash,
    cust_marital_status AS marital_status,
    COALESCE(NULLIF(cust_gender, 'n/a'), gen) AS gender,
    bdate AS birth_date,
    cntry AS country,
    cust_create_date AS customer_create_date,
    cust_is_future
FROM customer_base;
GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
WITH product_base AS (
    SELECT
        p.prd_id,
        p.prd_key,
        TRIM(p.prd_nm) AS prd_nm,
        ISNULL(p.prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(p.prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE TRIM(p.prd_line)
        END AS prd_line,
        p.prd_start_dt,
        p.prd_end_dt,
        REPLACE(SUBSTRING(p.prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(p.prd_key, 7, LEN(p.prd_key)) AS prd_key_clean
    FROM silver.crm_prd_info p
)
SELECT
    ROW_NUMBER() OVER (ORDER BY prd_id) AS product_key,
    prd_id AS product_id,
    prd_key_clean AS product_number,
    prd_nm AS product_name,
    prd_cost AS product_cost,
    prd_line AS product_line,
    prd_start_dt AS product_start_date,
    prd_end_dt AS product_end_date,
    c.cat AS category,
    c.subcat AS subcategory,
    c.maintenance
FROM product_base p
LEFT JOIN silver.erp_px_cat_g1v2 c
    ON c.id = p.cat_id;
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    f.sls_ord_num AS order_number,
    d.customer_key,
    p.product_key,
    CONVERT(DATE, CONVERT(CHAR(8), NULLIF(f.sls_order_dt, 0))) AS order_date,
    CONVERT(DATE, CONVERT(CHAR(8), NULLIF(f.sls_ship_dt, 0))) AS ship_date,
    CONVERT(DATE, CONVERT(CHAR(8), NULLIF(f.sls_due_dt, 0))) AS due_date,
    f.sls_sales AS sales_amount,
    f.sls_quantity AS quantity,
    f.sls_price AS price
FROM silver.crm_sales_details f
INNER JOIN gold.dim_customers d
    ON d.customer_id = f.sls_cust_id
INNER JOIN gold.dim_products p
    ON p.product_number = f.sls_prd_key;
GO
