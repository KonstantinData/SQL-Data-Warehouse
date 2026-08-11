USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'performance.fact_sales_benchmark', N'U') IS NULL
    THROW 53300, 'Benchmark fixture is not prepared.', 1;

DECLARE @fixture_rows BIGINT = (SELECT COUNT_BIG(*) FROM performance.fact_sales_benchmark);
IF @fixture_rows <> 1000000
    THROW 53301, 'Benchmark fixture must contain exactly 1,000,000 deterministic rows.', 1;

IF NOT EXISTS
(
    SELECT 1
    FROM performance.benchmark_metadata
    WHERE benchmark_name = N'gold_index_case'
      AND target_row_count = @fixture_rows
      AND cache_policy = N'warm-cache'
      AND warmup_runs = 1
      AND measured_runs = 5
      AND maxdop = 1
)
    THROW 53302, 'Benchmark metadata is incomplete or inconsistent.', 1;

IF
(
    SELECT COUNT(*)
    FROM sys.stats AS stats
    CROSS APPLY sys.dm_db_stats_properties(stats.object_id, stats.stats_id) AS properties
    WHERE stats.object_id = OBJECT_ID(N'performance.fact_sales_benchmark')
      AND stats.name IN (N'ST_perf_order_date', N'ST_perf_customer_date', N'ST_perf_product_date')
      AND properties.rows = properties.rows_sampled
) <> 3
    THROW 53303, 'Baseline statistics must exist and be sampled with FULLSCAN.', 1;

SELECT N'PASS' AS performance_fixture_contract, @fixture_rows AS fixture_rows;
GO
