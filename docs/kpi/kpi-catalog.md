# KPI Catalog

This catalog defines the explicit measures in the source-controlled Power BI reference model. The machine-readable contract is `kpi-catalog.json`; exact executable DAX is held in `powerbi/SQLDataWarehouse.SemanticModel/definition/tables/_Measures.tmdl`.

## Evidence boundary

These definitions apply only to the repository's synthetic CRM and ERP sample data. They are implementation contracts, not approved enterprise targets, accounting policies, production results, or evidence of operational adoption.

The repository does not define a currency. Monetary measures therefore use neutral numeric formats rather than a currency symbol.

## Key interpretation rules

- `Sales` is one row per source sales line. Orders use distinct nonblank `order_number`; line count is a separate measure.
- Sales and Inventory relate to Product versions by the persisted Gold `product_key`; `product_number` is descriptive and is not assumed unique across history.
- Cost and margin measures are always labelled `Estimated`. Sales uses the Product master cost version effective on the order date, but that cost remains distinct from an accounting posting.
- Inventory quantity and value measures are semi-additive over time. Consumers must select an intentional snapshot date or clearly label a period sum.
- Year-over-year percentages return blank when the prior-year baseline is missing or zero.
- On-time shipment is an order-grain model metric. It is not proof of delivery.
- Data-quality pass rate is check-weighted. Unevaluated checks are excluded from the denominator and reported separately.

## Ownership and approval

Business owner, data owner, and approver fields remain environment-specific. A real deployment must assign accountable people and record approval before these definitions are used for management reporting.
