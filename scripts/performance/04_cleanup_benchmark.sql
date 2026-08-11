USE DataWarehouse;
GO

SET NOCOUNT ON;
GO

/* Explicit opt-in cleanup. Only isolated benchmark objects are removed. */
DROP TABLE IF EXISTS performance.benchmark_run_log;
DROP TABLE IF EXISTS performance.benchmark_result_summary;
DROP TABLE IF EXISTS performance.benchmark_results;
DROP TABLE IF EXISTS performance.benchmark_metadata;
DROP TABLE IF EXISTS performance.fact_sales_benchmark;
GO

IF SCHEMA_ID(N'performance') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'performance'))
    DROP SCHEMA performance;
GO
