:ON ERROR EXIT

/* Opt-in only. Run from the repository root and retain console output as evidence. */
:r ./scripts/performance/00_setup_benchmark.sql
:r ./scripts/performance/01_run_baseline.sql
:r ./scripts/performance/02_apply_benchmark_indexes.sql
:r ./scripts/performance/03_run_optimized.sql
