USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

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
    THROW 53500, 'Baseline and optimized result sets are not equivalent.', 1;

IF EXISTS
(
    SELECT baseline.query_name
    FROM performance.benchmark_result_summary AS baseline
    INNER JOIN performance.benchmark_result_summary AS optimized
        ON optimized.query_name = baseline.query_name
       AND optimized.phase = 'optimized'
    WHERE baseline.phase = 'baseline'
      AND (baseline.result_rows <> optimized.result_rows OR baseline.result_hash <> optimized.result_hash)
)
    THROW 53501, 'Baseline and optimized result fingerprints differ.', 1;

IF EXISTS
(
    SELECT phase, query_name
    FROM performance.benchmark_run_log
    GROUP BY phase, query_name
    HAVING SUM(CASE WHEN run_type = 'warmup' THEN 1 ELSE 0 END) <> 1
        OR SUM(CASE WHEN run_type = 'measured' THEN 1 ELSE 0 END) <> 5
)
OR (SELECT COUNT(DISTINCT CONCAT(phase, N'|', query_name)) FROM performance.benchmark_run_log) <> 6
    THROW 53502, 'Each query and phase requires one warm-up and five measured runs.', 1;

SELECT N'PASS' AS performance_result_equivalence;
GO
