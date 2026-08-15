# Data quality reporting

## Objective

The Data Quality page communicates whether the synthetic reference model is suitable for review. The check register and overall release status materialize globally at refresh in a disconnected table so country RLS cannot turn a scoped row count into a false release status. This is reference evidence, not production monitoring.

## Implemented checks

- Unknown/invalid order-date coverage in Gold;
- sales amount versus quantity multiplied by price;
- missing customer country;
- customer records flagged as future;
- nonpositive effective-dated product cost;
- sales-line variance against the accepted synthetic Gold baseline of 60,379;
- uniqueness of current product business keys;
- unresolved customer/product source references in `gold.fact_sales`;
- sales that predate the first available version of an otherwise known Product.

The source-to-Bronze-to-Silver-to-Gold reconciliation is independently enforced in SQL CI. Inventory has its own `15 = 11 accepted + 4 rejected` reconciliation and double-run equality gate.

## Status and release rule

- `Failed`: at least one evaluated Error check failed and blocks reference release, even when another check is unevaluated.
- `Not run`: no Error check failed, but at least one required check is unevaluated; never treated as passed.
- `Passed with warnings`: every required check was evaluated, no Error check failed, and at least one Warning check failed.
- `Passed`: every required check was evaluated and neither Error nor Warning checks failed.

The evaluation precedence is therefore `Failed` (Error) → `Not run` → `Passed with warnings` → `Passed`.

`Overall DQ Status` is the canonical release-state measure. The quality-summary visual also exposes Error-failure, Warning-failure, and unevaluated-check counts so the status is explainable without relying on colour. The detailed register exposes `Severity`, `Status`, `IsEvaluated`, `Scope`, `PassRate`, evaluation time, and failed rows.

## Global and selected-scope evidence

| Evidence | Scope under `CountrySalesViewer` |
|---|---|
| Overall DQ status, pass rate, failed/unevaluated checks, check register | Global refresh-time evidence |
| Last dataset refresh UTC | Global refresh metadata |
| Invalid order dates, sales-equation mismatches, missing customer country | Protected selected Sales/Customer scope |
| Future customer records | Protected selected Customer scope |
| Nonpositive product cost records | Global Product scope; it must not be described as country-filtered |
| Data-through date | Protected selected Sales scope through the active Customers-to-Sales relationship |

The two exception cards combine operational context for review, but their individual measures retain the scopes above. The mixed-scope dimension and freshness card labels this boundary explicitly. A zero in a protected measure is not a global release result, and the global Product exception count is not evidence of the user's entitled country scope.

Any fixture replacement must update baselines through KPI/change control. Alerts must name evidence time/zone, layer/check, impact, release decision, owner/next action, and the synthetic-data boundary.
