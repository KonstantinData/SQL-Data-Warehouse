USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF NOT EXISTS
(
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'performance.fact_sales_benchmark')
      AND name = N'IX_perf_order_date'
)
    THROW 52300, 'Run 02_apply_benchmark_indexes.sql before the optimized phase.', 1;
GO

DELETE FROM performance.benchmark_run_log WHERE phase = 'optimized';
DELETE FROM performance.benchmark_result_summary WHERE phase = 'optimized';
DELETE FROM performance.benchmark_results WHERE phase = 'optimized';

DECLARE @date_start_key INT;
DECLARE @date_end_key INT;
DECLARE @customer_key INT;
DECLARE @product_key INT;
SELECT
    @date_start_key = date_start_key,
    @date_end_key = date_end_key,
    @customer_key = customer_key,
    @product_key = product_key
FROM performance.benchmark_metadata
WHERE benchmark_name = N'gold_index_case';

INSERT performance.benchmark_results
SELECT 'optimized', 'Q1_DATE_CATEGORY', COALESCE(product.category, N'Unknown'), date_dim.calendar_year,
       SUM(fact.sales_amount), SUM(CONVERT(BIGINT, fact.quantity))
FROM performance.fact_sales_benchmark AS fact
INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
INNER JOIN gold.dim_products AS product ON product.product_key = fact.product_key
WHERE fact.order_date_key >= @date_start_key AND fact.order_date_key < @date_end_key
GROUP BY COALESCE(product.category, N'Unknown'), date_dim.calendar_year;

INSERT performance.benchmark_results
SELECT 'optimized', 'Q2_CUSTOMER_MONTH', CONVERT(NVARCHAR(100), @customer_key), date_dim.date_key,
       SUM(fact.sales_amount), SUM(CONVERT(BIGINT, fact.quantity))
FROM performance.fact_sales_benchmark AS fact
INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
WHERE fact.customer_key = @customer_key
GROUP BY date_dim.date_key;

INSERT performance.benchmark_results
SELECT 'optimized', 'Q3_PRODUCT_MONTH', CONVERT(NVARCHAR(100), @product_key), date_dim.date_key,
       SUM(fact.sales_amount), SUM(CONVERT(BIGINT, fact.quantity))
FROM performance.fact_sales_benchmark AS fact
INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
WHERE fact.product_key = @product_key
GROUP BY date_dim.date_key;

IF EXISTS
(
    SELECT query_name, group_key_1, group_key_2, sales_amount, quantity
    FROM performance.benchmark_results WHERE phase = 'baseline'
    EXCEPT
    SELECT query_name, group_key_1, group_key_2, sales_amount, quantity
    FROM performance.benchmark_results WHERE phase = 'optimized'
)
OR EXISTS
(
    SELECT query_name, group_key_1, group_key_2, sales_amount, quantity
    FROM performance.benchmark_results WHERE phase = 'optimized'
    EXCEPT
    SELECT query_name, group_key_1, group_key_2, sales_amount, quantity
    FROM performance.benchmark_results WHERE phase = 'baseline'
)
    THROW 52301, 'Optimized query results differ from the baseline.', 1;

INSERT performance.benchmark_result_summary
SELECT phase, query_name, COUNT_BIG(*),
       HASHBYTES('SHA2_256', CONCAT(COUNT_BIG(*), N'|', CHECKSUM_AGG(BINARY_CHECKSUM(group_key_1, group_key_2, sales_amount, quantity))))
FROM performance.benchmark_results
WHERE phase = 'optimized'
GROUP BY phase, query_name;

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @run INT = 0;
WHILE @run <= 5
BEGIN
    DECLARE @run_type VARCHAR(16) = CASE WHEN @run = 0 THEN 'warmup' ELSE 'measured' END;

    SELECT COALESCE(product.category, N'Unknown') AS category, date_dim.calendar_year,
           SUM(fact.sales_amount) AS sales_amount, SUM(CONVERT(BIGINT, fact.quantity)) AS quantity
    FROM performance.fact_sales_benchmark AS fact
    INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
    INNER JOIN gold.dim_products AS product ON product.product_key = fact.product_key
    WHERE fact.order_date_key >= @date_start_key AND fact.order_date_key < @date_end_key
    GROUP BY COALESCE(product.category, N'Unknown'), date_dim.calendar_year
    OPTION (RECOMPILE, MAXDOP 1);
    INSERT performance.benchmark_run_log VALUES ('optimized', 'Q1_DATE_CATEGORY', @run_type, @run, SYSUTCDATETIME());

    SELECT date_dim.date_key, SUM(fact.sales_amount) AS sales_amount,
           SUM(CONVERT(BIGINT, fact.quantity)) AS quantity
    FROM performance.fact_sales_benchmark AS fact
    INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
    WHERE fact.customer_key = @customer_key
    GROUP BY date_dim.date_key
    OPTION (RECOMPILE, MAXDOP 1);
    INSERT performance.benchmark_run_log VALUES ('optimized', 'Q2_CUSTOMER_MONTH', @run_type, @run, SYSUTCDATETIME());

    SELECT date_dim.date_key, SUM(fact.sales_amount) AS sales_amount,
           SUM(CONVERT(BIGINT, fact.quantity)) AS quantity
    FROM performance.fact_sales_benchmark AS fact
    INNER JOIN gold.dim_date AS date_dim ON date_dim.date_key = fact.order_date_key
    WHERE fact.product_key = @product_key
    GROUP BY date_dim.date_key
    OPTION (RECOMPILE, MAXDOP 1);
    INSERT performance.benchmark_run_log VALUES ('optimized', 'Q3_PRODUCT_MONTH', @run_type, @run, SYSUTCDATETIME());

    SET @run += 1;
END;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO
