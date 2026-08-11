# Benchmark Evidence Record

## Evidence status

Evidence classification: **current measurement evidence; plan-complete archive
not collected**.

Current logical-read, CPU, elapsed-time, and result-equivalence evidence was
collected on 2026-08-10 UTC from the integrated **60,379-row** Gold model. The
executable SQL, performance scripts, tests, datasets, and source-loading scope
was clean at commit `1619eeffb44856ba3d653c3677e01d7f0ee306ed` during the
run. Parallel working-tree changes were limited to documentation,
source-contract metadata, Power BI artifacts, and validators; none altered the
measured SQL, performance scripts, tests, or fixtures.

The run used the committed warm-cache scripts without server-wide cache
clearing. Actual execution-plan files were not collected because the committed
CLI flow emits `STATISTICS IO/TIME` rather than `STATISTICS XML`; therefore no
operator-change, seek, lookup, spill, memory-grant, or estimate-accuracy claim
is made. This is current performance evidence for the recorded executable
scope, not a capacity, SLA, production-readiness, or production-workload claim.

## Environment and pipeline record

| Field | Recorded value |
| --- | --- |
| Pipeline start UTC | 2026-08-10 22:07:09.735 |
| Pipeline completion UTC | 2026-08-10 22:10:18.985 |
| Evidence extraction UTC | 2026-08-10 22:10:44.9596344 |
| Pipeline result | `SUCCEEDED`, batch `1` |
| Executable repository revision | `1619eeffb44856ba3d653c3677e01d7f0ee306ed` |
| SQL Server | 16.0.4265.3, Express Edition (64-bit) |
| Database compatibility level | 160 |
| Container image | `mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04` |
| Image digest | `sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89` |
| Docker / host | Docker 29.3.1; WSL2 Linux 6.6.87.2; 32 CPUs; 16,177,260 kB memory exposed |
| Isolation | repository and datasets mounted read-only; `no-new-privileges`; temporary container removed after cleanup |
| Source version / watermark | `benchmark-working-tree-20260811` / `1` |
| Snapshot date | `2024-12-31` |
| Gold rows | Customers 18,485; Products 398; Date 1,140; Sales 60,379; Inventory Locations 2; Inventory Snapshots 10 |

## Reproducible fixture facts

| Field | Value |
| --- | --- |
| Measurement source Gold fact rows | 60,379 |
| Benchmark target rows | 1,000,000 |
| Scale factor | 16.562050 |
| Date range | `[20120101, 20130101)` |
| Customer key | 186 |
| Product key | 397 |
| Cache policy | warm cache |
| Warm-up runs per query/phase | 1 |
| Measured runs per query/phase | 5 |
| Query MAXDOP | 1 |
| Baseline statistics | FULLSCAN; rows and rows sampled both 1,000,000 |
| Fixture fingerprint | `FB11A494BA8338468FD42265D52B7EB2E7EEA26F8C8FBEC330C5791808C62583` |
| Result validation | bidirectional `EXCEPT` plus SHA-256 summary fingerprint |

FULLSCAN statistics were recorded at 22:11:13.9233333 UTC for order date,
22:11:14.0400000 UTC for customer/date, and 22:11:14.1600000 UTC for
product/date.

## Measurement table

All values are listed in measured run order; medians use the five measured
runs after one unmeasured warm-up.

| Query | Phase | Logical reads 1-5 | Median reads | CPU ms 1-5 | Median CPU | Elapsed ms 1-5 | Median elapsed | Plan path |
| --- | --- | --- | ---: | --- | ---: | --- | ---: | --- |
| Q1 date/category | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | 52, 51, 52, 52, 48 | 52 | 54, 53, 51, 50, 50 | 51 | Not collected |
| Q1 date/category | Optimized | 284, 284, 284, 284, 284 | 284 | 16, 16, 16, 16, 16 | 16 | 17, 16, 17, 17, 16 | 17 | Not collected |
| Q2 customer/date | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | 31, 31, 32, 31, 29 | 31 | 33, 32, 31, 31, 31 | 31 | Not collected |
| Q2 customer/date | Optimized | 9, 9, 9, 9, 9 | 9 | 0, 0, 0, 0, 0 | 0 | 0, 0, 0, 0, 0 | 0 | Not collected |
| Q3 product/date | Baseline | 6745, 6745, 6745, 6745, 6745 | 6745 | 40, 36, 39, 36, 44 | 39 | 39, 38, 38, 37, 43 | 38 | Not collected |
| Q3 product/date | Optimized | 180, 180, 180, 180, 180 | 180 | 4, 4, 6, 4, 4 | 4 | 5, 5, 5, 5, 5 | 5 | Not collected |

Median fact logical reads fell by 95.79% for Q1, 99.87% for Q2, and
97.33% for Q3 in this environment. The Q2 zero-millisecond observations reflect
SQL Server timer granularity and do not mean that the query required no time.

## Correctness and integrity evidence

The following committed contracts passed:

- `tests/performance_fixture_contract.sql`;
- `tests/performance_index_contract.sql`;
- `tests/performance_result_equivalence.sql`.

Every query/phase recorded exactly one warm-up and five measured runs. Baseline
and optimized result rows and fingerprints were identical:

| Query | Rows | Shared baseline/optimized fingerprint |
| --- | ---: | --- |
| Q1 | 4 | `2191B3FBE566C6407B068669378D6A2839C4EE6C149E3B8FD59F2E6169E3B91F` |
| Q2 | 27 | `AECEB766CB3B9764897C2CFAF03316E4F15BE46A1CB88A4543A0A6E1574B3FE9` |
| Q3 | 212 | `8F776235DA4D77801774AE6413BFE568B1677B2C907FC1C4A1028F5B1B33C8FE` |

The transient raw SQLCMD outputs were hashed before the disposable container
was removed: baseline
`9e31a6ba8be761e26bf1268525d1419b94e21dad29e77fd584319396f81f7045`
and optimized
`c4cace7b82532a401213e94ec4fb411886b66467ff869b92b36cc53a6e1b8541`.
The raw files are not committed, so the tables and contract outputs above are
the durable repository evidence.

## Plan interpretation boundary

No actual plans were collected. A future plan-complete run should use SSMS/ADS
or a separately reviewed `STATISTICS XML` capture and record fact access
operators, actual versus estimated rows, lookups, spills, implicit conversions,
memory grants, and join changes. The measured logical-read and timing changes
remain valid without a plan screenshot, but they do not prove why the optimizer
selected a particular access path.

## Cleanup

`scripts/performance/04_cleanup_benchmark.sql` completed successfully. The
temporary SQL Server container was then removed, and a filtered Docker check
confirmed that no benchmark container remained.

## Limitations

- Deterministic replication increases row count but does not reproduce real
  production skew, concurrency, ingestion, retention, or storage behavior.
- CPU and elapsed time are sensitive to hardware, concurrent load, SQL Server
  build, memory state, and client overhead.
- The benchmark does not exercise index-maintenance cost during the normal Gold
  load.
- Synthetic source history is incomplete; strict product as-of resolution
  creates many Unknown product facts.
- No capacity, SLA, production deployment, or universal improvement conclusion
  is supported by this case.
