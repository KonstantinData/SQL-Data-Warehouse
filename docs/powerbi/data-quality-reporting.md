# Data quality reporting

## Objective

The Data Quality page communicates whether the synthetic reference model is suitable for review. Checks materialize globally at refresh in a disconnected table so country RLS cannot turn a scoped row count into a false release status. This is reference evidence, not production monitoring.

## Implemented checks

- Unknown/invalid order-date coverage in Gold;
- sales amount versus quantity multiplied by price;
- missing customer country;
- customer records flagged as future;
- nonpositive effective-dated product cost;
- sales-line variance against the accepted synthetic Gold baseline of 60,379;
- uniqueness of current product business keys;
- customer/product Unknown-member coverage in `gold.fact_sales`.

The source-to-Bronze-to-Silver-to-Gold reconciliation is independently enforced in SQL CI. Inventory has its own `14 = 10 accepted + 4 rejected` reconciliation and double-run equality gate.

## Status and release rule

- `Not run`: no current execution evidence; never treated as passed.
- `Passed`: evaluated with zero failed rows.
- `Passed with warnings`: no Error check failed, but a Warning check did.
- `Failed`: at least one Error check failed and blocks reference release.

Any fixture replacement must update baselines through KPI/change control. Alerts must name evidence time/zone, layer/check, impact, release decision, owner/next action, and the synthetic-data boundary.
