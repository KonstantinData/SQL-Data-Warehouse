# Benchmark Evidence Record

## Evidence status

Checked-in runtime measurements: **logical reads collected; result equivalence verified; actual plan files not collected**.

The measurement below was produced by the committed scripts against an isolated SQL Server 2022 Express container. The raw SQLCMD outputs contain `STATISTICS IO/TIME`; the checked-in summary uses fact logical reads as the primary reproducible metric. CPU and elapsed values are not summarized because millisecond-scale container timings are environment-sensitive. No values are inferred or invented.

This is a production-oriented reference implementation using synthetic data. It has not been deployed against a production workload. Future measurements apply only to the recorded environment and fixture.

## Reproducible fixture facts

| Field | Value |
| --- | --- |
| Measurement source Gold fact rows | 60,398 in the pre-runtime-convergence measurement snapshot; current accepted runtime baseline is 60,379 |
| Benchmark target rows | 1,000,000 |
| Cache policy | warm cache |
| Warm-up runs per query/phase | 1 |
| Measured runs per query/phase | 5 |
| Query MAXDOP | 1 |
| Baseline statistics | FULLSCAN |
| Result validation | bidirectional `EXCEPT` plus SHA-256 summary fingerprint |

The recorded measurement snapshot established 2012 as a selective date range (3,397 of 60,398 source facts) and avoided presenting the highly concentrated 2013 data as a selective filter. Re-run the benchmark before using that selectivity statement for the integrated 60,379-row runtime baseline.

## Environment record

Complete this section for every measured run.

| Field | Recorded value |
| --- | --- |
| UTC run time | 2026-08-10 11:01:01 |
| Repository revision | Local verification branch based on `a3b8f9315f75e917e626d1389aa28fe2de8348d4`; final commit is recorded in the handoff |
| SQL Server version | 16.0.4265.3 |
| SQL Server edition | Express Edition (64-bit) |
| Database compatibility level | 160 |
| Gold row count | 60,398 in the recorded pre-convergence measurement snapshot |
| Benchmark row count | 1,000,000 |
| Scale factor | 16.556840 |
| Fixture fingerprint | `D08C6A934E82AEFA4E956DE814E46CD53943B8E5FCC51291D244EC20CBCB23F7` |
| Date range | `[20120101, 20130101)` |
| Customer key | 186 |
| Product key | 397 |
| Cache/reset policy | warm-cache; no server-wide cache clearing |

## Measurement table

Record all five measured runs, then the median.

| Query | Phase | Logical reads 1–5 | Median logical reads | CPU ms 1–5 | Median CPU ms | Elapsed ms 1–5 | Median elapsed ms | Plan path |
| --- | --- | --- | ---: | --- | ---: | --- | ---: | --- |
| Q1 date/category | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | Captured in raw output | — | Captured in raw output | — | Not collected |
| Q1 date/category | Optimized | 1392, 1392, 1392, 1392, 1392 | 1392 | Captured in raw output | — | Captured in raw output | — | Not collected |
| Q2 customer/date | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | Captured in raw output | — | Captured in raw output | — | Not collected |
| Q2 customer/date | Optimized | 9, 9, 9, 9, 9 | 9 | Captured in raw output | — | Captured in raw output | — | Not collected |
| Q3 product/date | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | Captured in raw output | — | Captured in raw output | — | Not collected |
| Q3 product/date | Optimized | 180, 180, 180, 180, 180 | 180 | Captured in raw output | — | Captured in raw output | — | Not collected |

Observed median fact logical-read reductions in this environment were 79.4% for Q1, 99.9% for Q2, and 97.3% for Q3. These are case-specific measurements, not universal performance guarantees.

## Correctness evidence

Record the outputs of:

```powershell
sqlcmd -b -d DataWarehouse -i .\tests\performance_fixture_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\performance_index_contract.sql
sqlcmd -b -d DataWarehouse -i .\tests\performance_result_equivalence.sql
```

Expected evidence:

- exactly 1,000,000 deterministic benchmark rows;
- FULLSCAN baseline statistics;
- exact optimized index key/include order;
- identical result rows and fingerprints across phases;
- one warm-up and five measured executions for every query and phase.

All three SQL contracts passed for this recorded run. Baseline and optimized result rows and SHA-256 summary fingerprints were equal.

## Plan interpretation

Actual plan files were not collected in this run, so no operator-change claim is made. The measured logical-read change is valid independently of a plan screenshot. A future plan-complete run should record the fact access operator, actual versus estimated rows, lookups, spills, conversions, memory grants, and join changes. Treat a scan-to-seek change as an observation, not as proof of improvement without the logical-read and timing evidence.

## Limitations

- Deterministic replication increases row count but does not reproduce real production skew, concurrency, ingestion, retention, or storage behavior.
- CPU and elapsed time are sensitive to hardware, concurrent load, SQL Server build, memory state, and client overhead.
- The benchmark does not exercise index maintenance cost during the normal Gold load.
- Synthetic source history is incomplete; strict product as-of resolution creates many Unknown product facts.
- No capacity, SLA, or production deployment conclusion is supported by this case.
