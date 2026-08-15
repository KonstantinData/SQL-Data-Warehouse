# Power BI Performance Analyzer Acceptance

## Scope and environment

The acceptance was executed on 2026-08-15 with Power BI Desktop 2.156.951.0 against the loopback-only SQL acceptance database. It covers all three report pages and all fifteen visual queries in the default unfiltered state. It is Desktop PoC evidence, not a Power BI Service latency claim.

## Reproducible method

1. Open `powerbi/SQLDataWarehouse.pbip` from the measured commit and refresh all tables.
2. Reconcile the SQL fixture anchors in `powerbi/performance/performance-evidence.json`.
3. Open Performance Analyzer and start recording.
4. Activate a page once as its warm-up render, then clear the analyzer.
5. Select **Refresh visuals** five times without clearing the model cache. Wait for every query to finish after each action.
6. Export the five refresh actions to `powerbi/performance/raw` and preserve the raw JSON unchanged, including Desktop's UTF-8 BOM.
7. Repeat for all three pages before and after the source change.
8. Run `python scripts/powerbi_validation/validate_performance_evidence.py --root .`.

The authoritative metric is the `Execute DAX Query` duration associated with each `Visual Container Lifecycle`. Durations are integer milliseconds. The reported median is the upper middle element of the five sorted samples. Decorative title cards have four completed DAX events because the fifth card lifecycle remained open when the export completed; their queries are retained for coverage but are not optimization targets.

Power BI Desktop 2.156.951.0 emitted invalid `Visual Container Lifecycle` end timestamps for `cardVisual` events (a 1969 sentinel), which inflated some displayed visual totals to seconds. Those lifecycle totals remain in the raw evidence for diagnosis but are not used as DAX latency measurements or gates.

## Acceptance bounds

- Result preservation: the DAX `RowCount` must be identical before and after for every visual.
- Capture completeness: exactly five `UserAction_Refresh` events per page; five DAX samples per business visual and at least four for each decorative title card.
- Improvement claim: after median must be strictly lower than before median.
- Non-regression: after median must not exceed the before maximum.
- Integrity: every raw export must match its committed SHA-256 hash and its committed sorted timing samples.

These are evidence-derived comparison rules, not an invented absolute millisecond SLA.

## Results

| Page | Visual | Before min / median / max (ms) | After min / median / max (ms) | Median change | Rows | Result |
|---|---|---:|---:|---:|---:|---|
| Executive Overview | Country performance | 129 / 135 / 161 | 65 / 68 / 70 | -49.6% | 4 | Improved |
| Executive Overview | Sales trend versus previous year | 13 / 16 / 27 | 14 / 15 / 17 | -6.3% | 51 | Pass, no claim |
| Executive Overview | Executive Overview | 7 / 8 / 22 | 6 / 11 / 12 | +37.5% | 1 | Pass, decorative |
| Executive Overview | Performance summary | 5 / 7 / 21 | 6 / 9 / 25 | +28.6% | 1 | Pass, no claim |
| Executive Overview | Sales by product line | 7 / 7 / 21 | 6 / 8 / 10 | +14.3% | 5 | Pass, no claim |
| Sales Performance | Sales operations summary | 28 / 32 / 72 | 29 / 35 / 43 | +9.4% | 1 | Pass, no claim |
| Sales Performance | Product performance detail | 8 / 9 / 11 | 7 / 8 / 9 | -11.1% | 132 | Pass, no claim |
| Sales Performance | Sales Performance | 8 / 9 / 10 | 7 / 8 / 8 | -11.1% | 1 | Pass, decorative |
| Sales Performance | Estimated gross profit trend | 8 / 8 / 9 | 7 / 7 / 12 | -12.5% | 39 | Pass, no claim |
| Sales Performance | Sales by category | 8 / 8 / 10 | 7 / 7 / 12 | -12.5% | 4 | Pass, no claim |
| Data Quality | Quality and refresh summary | 11 / 12 / 14 | 9 / 9 / 23 | -25.0% | 1 | Pass, no claim |
| Data Quality | Data Quality | 9 / 10 / 27 | 5 / 9 / 30 | -10.0% | 1 | Pass, decorative |
| Data Quality | Mixed-scope dimension and freshness exceptions | 8 / 10 / 12 | 5 / 7 / 23 | -30.0% | 1 | Pass, no claim |
| Data Quality | Selected-scope sales exceptions | 9 / 10 / 12 | 5 / 7 / 23 | -30.0% | 1 | Pass, no claim |
| Data Quality | Data quality check register | 8 / 10 / 12 | 4 / 6 / 6 | -40.0% | 9 | Pass, no claim |

`Country performance` was the slowest baseline query and is the only visual for which this acceptance asserts an optimization improvement. `Sales operations summary` increased by 3 ms at the median, so no improvement is claimed; its 35 ms after median remains below its 72 ms before maximum and passes the declared non-regression rule.

## Implemented optimizations

- `On-Time Shipment %` caches ship and due dates once per order before eligibility and on-time filtering. This removes repeated per-order callbacks while retaining the current filter and RLS context.
- `Distinct Orders` and `Customer Count` use `DISTINCTCOUNTNOBLANK` so the storage engine can perform the non-blank distinct count directly.
- `DQ Failed Rows` uses a typed-column `SUM` instead of row iteration and text conversion.

Estimated COGS, the data-quality Power Query, and visual result limits were deliberately not changed: their measured visual queries were already small, while those changes would add model, refresh, or result-shape risk without evidence of a material bottleneck.
