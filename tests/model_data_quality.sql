USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF (SELECT COUNT(*) FROM gold.dim_customers WHERE customer_key = 0) <> 1
   OR (SELECT COUNT(*) FROM gold.dim_products WHERE product_key = 0) <> 1
   OR (SELECT COUNT(*) FROM gold.dim_date WHERE date_key = 0) <> 1
    THROW 53100, 'Every Gold dimension must contain exactly one Unknown member.', 1;

IF (SELECT COUNT_BIG(*) FROM gold.fact_sales) < 1
    THROW 53101, 'Gold fact must contain the reconciled source grain.', 1;

IF EXISTS
(
    SELECT order_number, product_number FROM gold.fact_sales
    GROUP BY order_number, product_number HAVING COUNT(*) > 1
)
    THROW 53102, 'Gold fact contains duplicate source lines.', 1;

IF EXISTS
(
    SELECT order_number
    FROM gold.fact_sales
    GROUP BY order_number
    HAVING MIN(sales_order_line_number) <> 1
       OR MAX(sales_order_line_number) <> COUNT(*)
       OR COUNT(DISTINCT sales_order_line_number) <> COUNT(*)
)
    THROW 53103, 'Derived order line numbers must be contiguous and unique per order.', 1;

IF EXISTS
(
    SELECT 1
    FROM gold.dim_products AS left_version
    INNER JOIN gold.dim_products AS right_version
        ON right_version.product_number = left_version.product_number
       AND right_version.product_key > left_version.product_key
       AND left_version.product_key <> 0
       AND right_version.product_key <> 0
       AND left_version.effective_from < COALESCE(right_version.effective_to, CONVERT(DATE, '99991231', 112))
       AND right_version.effective_from < COALESCE(left_version.effective_to, CONVERT(DATE, '99991231', 112))
)
    THROW 53104, 'Product SCD2 intervals overlap.', 1;

IF EXISTS
(
    SELECT product_number
    FROM gold.dim_products
    WHERE product_key <> 0 AND is_current = 1
    GROUP BY product_number
    HAVING COUNT(*) > 1
)
    THROW 53105, 'A known product number has more than one current version.', 1;

IF EXISTS
(
    SELECT 1
    FROM gold.dim_date
    WHERE date_key <> 0
      AND
      (
          date_key <> CONVERT(INT, CONVERT(CHAR(8), calendar_date, 112))
          OR calendar_year <> DATEPART(YEAR, calendar_date)
          OR calendar_quarter <> DATEPART(QUARTER, calendar_date)
          OR calendar_month <> DATEPART(MONTH, calendar_date)
          OR day_of_month <> DATEPART(DAY, calendar_date)
      )
)
    THROW 53106, 'Date dimension attributes are inconsistent with the calendar date.', 1;

DECLARE @known_date_count BIGINT = (SELECT COUNT_BIG(*) FROM gold.dim_date WHERE date_key <> 0);
DECLARE @minimum_date DATE = (SELECT MIN(calendar_date) FROM gold.dim_date WHERE date_key <> 0);
DECLARE @maximum_date DATE = (SELECT MAX(calendar_date) FROM gold.dim_date WHERE date_key <> 0);
IF @known_date_count <> DATEDIFF(DAY, @minimum_date, @maximum_date) + 1
    THROW 53107, 'Known date members must form a contiguous range.', 1;

;WITH source_expected AS
(
    SELECT DISTINCT
        NULLIF(TRIM(source.sls_ord_num), N'') AS order_number,
        NULLIF(TRIM(source.sls_prd_key), N'') AS product_number,
        source.sls_cust_id AS customer_id,
        CASE WHEN source.sls_order_dt BETWEEN 10000101 AND 99991231
            THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), source.sls_order_dt), 112) END AS order_date,
        CASE WHEN source.sls_ship_dt BETWEEN 10000101 AND 99991231
            THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), source.sls_ship_dt), 112) END AS ship_date,
        CASE WHEN source.sls_due_dt BETWEEN 10000101 AND 99991231
            THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), source.sls_due_dt), 112) END AS due_date,
        CONVERT(DECIMAL(18, 2), source.sls_sales) AS sales_amount,
        source.sls_quantity AS quantity,
        CONVERT(DECIMAL(18, 2), source.sls_price) AS price
    FROM silver.crm_sales_details AS source
),
numbered AS
(
    SELECT expected.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY expected.order_number
            ORDER BY expected.product_number, expected.customer_id, expected.order_date,
                     expected.ship_date, expected.due_date, expected.sales_amount,
                     expected.quantity, expected.price
        ) AS sales_order_line_number
    FROM source_expected AS expected
),
resolved AS
(
    SELECT
        numbered.order_number, numbered.product_number,
        CONVERT(INT, numbered.sales_order_line_number) AS sales_order_line_number,
        COALESCE(customer.customer_key, 0) AS customer_key,
        CASE WHEN product_match.match_count = 1 THEN product_match.product_key ELSE 0 END AS product_key,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.order_date, 112)), 0) AS order_date_key,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.ship_date, 112)), 0) AS ship_date_key,
        COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.due_date, 112)), 0) AS due_date_key,
        numbered.order_date, numbered.ship_date, numbered.due_date,
        numbered.sales_amount, numbered.quantity, numbered.price,
        CASE
            WHEN numbered.sales_amount IS NULL OR numbered.quantity IS NULL OR numbered.price IS NULL
              OR numbered.quantity <= 0 OR numbered.price < 0 OR numbered.sales_amount < 0
                THEN N'INVALID_SOURCE_MEASURE'
            WHEN numbered.sales_amount <> numbered.quantity * numbered.price
                THEN N'SOURCE_ADJUSTMENT'
            ELSE N'VALID'
        END AS measure_quality_code
    FROM numbered
    LEFT JOIN gold.dim_customers AS customer ON customer.customer_id = numbered.customer_id
    OUTER APPLY
    (
        SELECT COUNT(*) AS match_count, MIN(product.product_key) AS product_key
        FROM gold.dim_products AS product
        WHERE product.product_key <> 0
          AND product.product_number = numbered.product_number
          AND numbered.order_date >= product.effective_from
          AND (product.effective_to IS NULL OR numbered.order_date < product.effective_to)
    ) AS product_match
)
SELECT * INTO #expected_fact FROM resolved;

IF (SELECT COUNT_BIG(*) FROM gold.fact_sales) <> (SELECT COUNT_BIG(*) FROM #expected_fact)
    THROW 53109, 'Gold fact count must preserve the distinct validated source-line grain.', 1;

IF EXISTS
(
    SELECT order_number, product_number, sales_order_line_number, customer_key, product_key,
           order_date_key, ship_date_key, due_date_key, order_date, ship_date, due_date,
           sales_amount, quantity, price, measure_quality_code
    FROM #expected_fact
    EXCEPT
    SELECT order_number, product_number, sales_order_line_number, customer_key, product_key,
           order_date_key, ship_date_key, due_date_key, order_date, ship_date, due_date,
           sales_amount, quantity, price, measure_quality_code
    FROM gold.fact_sales
)
OR EXISTS
(
    SELECT order_number, product_number, sales_order_line_number, customer_key, product_key,
           order_date_key, ship_date_key, due_date_key, order_date, ship_date, due_date,
           sales_amount, quantity, price, measure_quality_code
    FROM gold.fact_sales
    EXCEPT
    SELECT order_number, product_number, sales_order_line_number, customer_key, product_key,
           order_date_key, ship_date_key, due_date_key, order_date, ship_date, due_date,
           sales_amount, quantity, price, measure_quality_code
    FROM #expected_fact
)
    THROW 53108, 'Gold fact payload does not reconcile bidirectionally to Silver.', 1;

SELECT
    N'PASS' AS model_data_quality,
    COUNT_BIG(*) AS fact_rows,
    SUM(CASE WHEN product_key = 0 THEN 1 ELSE 0 END) AS unknown_product_rows,
    SUM(CASE WHEN order_date_key = 0 THEN 1 ELSE 0 END) AS unknown_order_date_rows
FROM gold.fact_sales;
GO
