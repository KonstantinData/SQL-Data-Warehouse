# Data Quality Reporting

## Objective

The Data Quality page communicates whether the synthetic reference model is suitable for review. Quality outcomes are materialized globally at model refresh into a disconnected table, so country RLS does not turn a scoped row count into a false release status. Selected-scope exception cards remain explicitly scope-relative. This does not claim production monitoring.

## Status model

- `Not run`: the required check has no current execution evidence.
- `Passed`: an evaluated check has zero failed rows.
- `Passed with warnings`: no Error check failed, but one or more Warning checks failed.
- `Failed`: at least one Error check failed.
- `Not applicable`: the check does not apply to the selected scope.

An unevaluated check is never counted as passed. A zero evaluated-check denominator returns blank, never 100%.

## Implemented semantic checks

- invalid or blank order date;
- sales amount differs from quantity times price;
- missing or unrecognized customer country;
- customer creation date flagged as future;
- nonpositive latest product cost;
- synthetic sales-line count differs from the checked 60,398-line baseline;
- semantic product business key is not unique.

`gold_source_orphan_coverage` is explicitly unevaluated. The current Gold fact uses inner joins, so Power BI cannot detect source rows that Gold already removed. SQL-level source/Silver/Gold reconciliation remains an upstream requirement.

The 60,398 baseline is a smoke assertion for the checked-in synthetic fixture, not a production threshold. Any dataset replacement must revise or remove it through the KPI/change-control process.

## Warehouse quality evidence

Existing Bronze checks are diagnostic. Silver and Gold CI checks can block the CI pipeline, while the analyst-oriented Gold query file itself is not a pass/fail harness. File existence is not evidence that a check ran or passed.

## Stakeholder alert contract

Use this factual form:

> Validation status: `<failed/passed with warnings>`. Layer/check: `<layer>/<check>`. Evidence time: `<timestamp and timezone>`. Impact: `<affected model/KPIs/pages>`. Release decision: `<hold/continue with warning>`. Owner and next action: `<owner/action>`. This alert concerns synthetic reference data and is not evidence about production data.

## Release rule

Any Error failure blocks the `Reference ready` decision. Warning failures require a recorded rationale. Unevaluated source-completeness coverage must remain visible until an auditable upstream result table exists.
