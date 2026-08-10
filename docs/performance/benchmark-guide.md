# Reproducible Index Benchmark

## Purpose and claim boundary

This case demonstrates how covering nonclustered indexes can change access paths and logical reads for representative warehouse queries. It uses synthetic data in an isolated schema. Results describe only the recorded SQL Server environment, fixture, and query parameters; they are not capacity, SLA, production-readiness, or production-experience claims.

## Safety boundary

The benchmark creates only `performance.*` objects. It does not drop or change production Gold indexes. Setup fails when Gold is empty and caps the expanded fixture at five million rows. The default workflow does not run `DBCC DROPCLEANBUFFERS` or `DBCC FREEPROCCACHE`; those commands affect an entire instance and are inappropriate on a shared server.

The committed workflow uses a warm-cache policy. If a cold-cache study is needed, use a disposable isolated SQL Server instance and document the reset mechanism separately.

## Workloads

The setup creates exactly 1,000,000 fact rows by deterministic ordered replication of `gold.fact_sales`. It records the source count, scale factor, database compatibility level, SQL Server version/edition, chosen parameters, cache policy, repetition counts, and a fixture fingerprint.

Three queries exercise distinct access paths:

1. a selective date-range and category aggregate (the committed fixture uses 2012 when available);
2. a date drilldown for the most frequent known customer key;
3. a date trend for the most frequent known product-version key.

Before the baseline, the workflow creates FULLSCAN statistics on the same single- and multi-column distributions used by the optimized indexes. This separates access-path value from improved cardinality statistics.

Each phase executes one unmeasured warm-up and five measured warm-cache runs with identical typed parameters, `OPTION (RECOMPILE, MAXDOP 1)`, projections, and grouping. Baseline and optimized results are stored and compared bidirectionally before the optimized phase can pass.

## Execution

Run from the repository root after Gold model tests pass:

```powershell
sqlcmd -b -d DataWarehouse -i .\scripts\performance\00_setup_benchmark.sql
sqlcmd -b -d DataWarehouse -i .\tests\performance_fixture_contract.sql
sqlcmd -b -d DataWarehouse -i .\scripts\performance\01_run_baseline.sql -o .\baseline-statistics.txt
sqlcmd -b -d DataWarehouse -i .\scripts\performance\02_apply_benchmark_indexes.sql
sqlcmd -b -d DataWarehouse -i .\scripts\performance\03_run_optimized.sql -o .\optimized-statistics.txt
sqlcmd -b -d DataWarehouse -i .\tests\performance_index_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\performance_result_equivalence.sql
```

`scripts/performance/run_benchmark.sql` runs setup through optimized measurement in one SQLCMD session. The explicit sequence above is preferable when retaining separate output files and actual plans.

Cleanup is opt-in:

```powershell
sqlcmd -b -d DataWarehouse -i .\scripts\performance\04_cleanup_benchmark.sql
```

## Measurement protocol

`01_run_baseline.sql` and `03_run_optimized.sql` enable `SET STATISTICS IO, TIME ON`. For every query and measured run, retain:

- logical reads for the benchmark fact table (primary metric);
- CPU time and elapsed time (secondary, environment-sensitive metrics);
- returned row count and stored result fingerprint;
- parameter values and phase;
- actual execution plan file.

Use the median of five measured logical-read values, not the best run. Report all five values. CPU and elapsed time can legitimately vary at this scale and must not be used as a brittle automated pass/fail gate.

## Execution-plan guidance

Enable **Include Actual Execution Plan** in SSMS or Azure Data Studio before executing each measurement script. Save one representative measured plan for each query and phase. Review:

- estimated versus actual row counts;
- fact clustered scans versus targeted nonclustered seeks or range scans;
- repeated key lookups;
- implicit conversions on predicates;
- sort/hash spills and memory grants;
- join type and join input cardinality;
- serial versus parallel execution;
- residual predicates and rows read versus rows returned.

The expected design effect is a transition from broad fact scans to covering date/customer/product access paths with fewer fact logical reads. It is not a contractual promise that SQL Server will choose a seek: optimizer version, cardinality estimator, statistics, and data distribution can change the plan. A lower estimated cost is not evidence of faster execution by itself.

## Evidence acceptance

A completed evidence record must include SQL Server version/edition, compatibility level, repository commit, Gold and benchmark row counts, scale factor, parameters, cache policy, MAXDOP, statistics timestamp, all measured reads/CPU/elapsed values, result-equivalence proof, saved plan paths, interpretation, and limitations. Use `benchmark-evidence.md` as the record template.
