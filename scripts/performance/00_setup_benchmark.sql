USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'performance') IS NULL
    EXEC(N'CREATE SCHEMA performance AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM gold.fact_sales)
    THROW 52000, 'Benchmark setup requires a populated gold.fact_sales table.', 1;
GO

DROP TABLE IF EXISTS performance.benchmark_run_log;
DROP TABLE IF EXISTS performance.benchmark_result_summary;
DROP TABLE IF EXISTS performance.benchmark_results;
DROP TABLE IF EXISTS performance.benchmark_metadata;
DROP TABLE IF EXISTS performance.fact_sales_benchmark;
GO

CREATE TABLE performance.fact_sales_benchmark
(
    benchmark_row_key BIGINT IDENTITY(1, 1) NOT NULL,
    source_sales_key  BIGINT NOT NULL,
    copy_number       INT NOT NULL,
    customer_key      INT NOT NULL,
    product_key       INT NOT NULL,
    order_date_key    INT NOT NULL,
    sales_amount      DECIMAL(18, 2) NULL,
    quantity          INT NULL,
    CONSTRAINT PK_performance_fact_sales_benchmark
        PRIMARY KEY CLUSTERED (benchmark_row_key)
);
GO

CREATE TABLE performance.benchmark_metadata
(
    benchmark_name       SYSNAME NOT NULL CONSTRAINT PK_performance_benchmark_metadata PRIMARY KEY,
    prepared_at_utc      DATETIME2(0) NOT NULL,
    sql_server_version   NVARCHAR(128) NOT NULL,
    sql_server_edition   NVARCHAR(128) NOT NULL,
    compatibility_level INT NOT NULL,
    gold_row_count       BIGINT NOT NULL,
    target_row_count     BIGINT NOT NULL,
    scale_factor         DECIMAL(18, 6) NOT NULL,
    date_start_key       INT NOT NULL,
    date_end_key         INT NOT NULL,
    customer_key         INT NOT NULL,
    product_key          INT NOT NULL,
    warmup_runs          INT NOT NULL,
    measured_runs        INT NOT NULL,
    maxdop               INT NOT NULL,
    cache_policy         NVARCHAR(50) NOT NULL,
    fixture_fingerprint  VARBINARY(32) NOT NULL
);
GO

CREATE TABLE performance.benchmark_results
(
    phase          VARCHAR(16) NOT NULL,
    query_name     VARCHAR(32) NOT NULL,
    group_key_1    NVARCHAR(100) NOT NULL,
    group_key_2    INT NOT NULL,
    sales_amount   DECIMAL(38, 2) NULL,
    quantity       BIGINT NULL,
    CONSTRAINT PK_performance_benchmark_results
        PRIMARY KEY (phase, query_name, group_key_1, group_key_2),
    CONSTRAINT CK_performance_benchmark_results_phase
        CHECK (phase IN ('baseline', 'optimized'))
);
GO

CREATE TABLE performance.benchmark_result_summary
(
    phase         VARCHAR(16) NOT NULL,
    query_name    VARCHAR(32) NOT NULL,
    result_rows   BIGINT NOT NULL,
    result_hash   VARBINARY(32) NOT NULL,
    CONSTRAINT PK_performance_benchmark_result_summary
        PRIMARY KEY (phase, query_name)
);
GO

CREATE TABLE performance.benchmark_run_log
(
    phase          VARCHAR(16) NOT NULL,
    query_name     VARCHAR(32) NOT NULL,
    run_type       VARCHAR(16) NOT NULL,
    run_number     INT NOT NULL,
    executed_at_utc DATETIME2(0) NOT NULL,
    CONSTRAINT PK_performance_benchmark_run_log
        PRIMARY KEY (phase, query_name, run_type, run_number),
    CONSTRAINT CK_performance_benchmark_run_log_phase
        CHECK (phase IN ('baseline', 'optimized')),
    CONSTRAINT CK_performance_benchmark_run_log_type
        CHECK (run_type IN ('warmup', 'measured'))
);
GO

DECLARE @target_rows BIGINT = 1000000;
DECLARE @maximum_rows BIGINT = 5000000;
DECLARE @gold_rows BIGINT = (SELECT COUNT_BIG(*) FROM gold.fact_sales);
DECLARE @copy_count INT = CEILING(CONVERT(DECIMAL(19, 6), @target_rows) / @gold_rows);

IF @target_rows > @maximum_rows
    THROW 52001, 'Benchmark target exceeds the fail-closed safety cap.', 1;

;WITH copy_numbers AS
(
    SELECT TOP (@copy_count)
        CONVERT(INT, ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS copy_number
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
)
INSERT performance.fact_sales_benchmark
(
    source_sales_key, copy_number, customer_key, product_key, order_date_key,
    sales_amount, quantity
)
SELECT TOP (@target_rows)
    fact.sales_key,
    copies.copy_number,
    fact.customer_key,
    fact.product_key,
    fact.order_date_key,
    fact.sales_amount,
    fact.quantity
FROM copy_numbers AS copies
CROSS JOIN gold.fact_sales AS fact
ORDER BY copies.copy_number, fact.sales_key;

CREATE STATISTICS ST_perf_order_date
    ON performance.fact_sales_benchmark (order_date_key) WITH FULLSCAN;
CREATE STATISTICS ST_perf_customer_date
    ON performance.fact_sales_benchmark (customer_key, order_date_key) WITH FULLSCAN;
CREATE STATISTICS ST_perf_product_date
    ON performance.fact_sales_benchmark (product_key, order_date_key) WITH FULLSCAN;

DECLARE @actual_rows BIGINT = (SELECT COUNT_BIG(*) FROM performance.fact_sales_benchmark);
DECLARE @customer_key INT =
(
    SELECT TOP (1) customer_key
    FROM performance.fact_sales_benchmark
    WHERE customer_key <> 0
    GROUP BY customer_key
    ORDER BY COUNT_BIG(*) DESC, customer_key
);
DECLARE @product_key INT =
(
    SELECT TOP (1) product_key
    FROM performance.fact_sales_benchmark
    WHERE product_key <> 0
    GROUP BY product_key
    ORDER BY COUNT_BIG(*) DESC, product_key
);
DECLARE @date_start_key INT = CASE
    WHEN EXISTS (SELECT 1 FROM performance.fact_sales_benchmark WHERE order_date_key BETWEEN 20120101 AND 20121231)
        THEN 20120101
    ELSE (SELECT MIN(order_date_key) FROM performance.fact_sales_benchmark WHERE order_date_key <> 0)
END;
DECLARE @date_end_key INT = CASE
    WHEN @date_start_key = 20120101 THEN 20130101
    ELSE CONVERT(INT, CONVERT(CHAR(8), DATEADD(YEAR, 1, CONVERT(DATE, CONVERT(CHAR(8), @date_start_key), 112)), 112))
END;
DECLARE @fixture_checksum INT =
(
    SELECT CHECKSUM_AGG(BINARY_CHECKSUM(
        source_sales_key, copy_number, customer_key, product_key,
        order_date_key, sales_amount, quantity))
    FROM performance.fact_sales_benchmark
);
DECLARE @fixture_fingerprint VARBINARY(32) = HASHBYTES
(
    'SHA2_256',
    CONCAT(@actual_rows, N'|', @fixture_checksum, N'|', @customer_key, N'|', @product_key)
);

INSERT performance.benchmark_metadata
(
    benchmark_name, prepared_at_utc, sql_server_version, sql_server_edition,
    compatibility_level, gold_row_count, target_row_count, scale_factor,
    date_start_key, date_end_key, customer_key, product_key, warmup_runs,
    measured_runs, maxdop, cache_policy, fixture_fingerprint
)
SELECT
    N'gold_index_case', SYSUTCDATETIME(),
    CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion')),
    CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition')),
    compatibility_level, @gold_rows, @actual_rows,
    CONVERT(DECIMAL(18, 6), @actual_rows) / @gold_rows,
    @date_start_key, @date_end_key, @customer_key, @product_key,
    1, 5, 1, N'warm-cache', @fixture_fingerprint
FROM sys.databases
WHERE name = DB_NAME();
GO
