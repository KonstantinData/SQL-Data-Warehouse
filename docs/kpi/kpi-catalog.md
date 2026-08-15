# KPI Catalog

This catalog covers every executable measure in the source-controlled Power BI reference model. The machine-readable contract is `kpi-catalog.json`; exact executable DAX and format strings are held in `powerbi/SQLDataWarehouse.SemanticModel/definition/tables/_Measures.tmdl`.

## Evidence boundary

Definitions apply only to the repository's synthetic CRM, ERP, and Inventory fixtures. They are implementation contracts, not approved enterprise targets, accounting policies, production results, or evidence of operational adoption. Sales currency is unspecified. Inventory amounts use the fixture contract's `EUR` currency but are not accounting valuations.

## Complete measure inventory

| Domain | Measure | Grain / interpretation | Format |
| --- | --- | --- | --- |
| Sales | `Total Sales` | Sum of curated amounts recalculated in Silver as positive quantity times normalized positive price | `#,0.00` |
| Sales | `Sales Lines` | Accepted published Sales rows | `#,0` |
| Sales | `Distinct Orders` | Distinct nonblank order numbers | `#,0` |
| Sales | `Total Quantity` | Sum of accepted positive line quantities | `#,0` |
| Sales | `Average Selling Price` | Total Sales divided by Total Quantity | `#,0.00` |
| Sales | `Average Order Value` | Total Sales divided by Distinct Orders | `#,0.00` |
| Sales | `Customer Count` | Distinct nonblank customers represented in Sales | `#,0` |
| Sales | `Product Count` | Distinct descriptive product numbers represented in Sales | `#,0` |
| Estimated profitability | `Estimated COGS` | Sold quantity times the persisted effective-dated Product master cost | `#,0.00` |
| Estimated profitability | `Estimated Gross Profit` | Total Sales minus Estimated COGS | `#,0.00` |
| Estimated profitability | `Estimated Gross Margin %` | Estimated Gross Profit divided by Total Sales | `0.0%` |
| Time | `Sales Previous Year` | Total Sales shifted one year through Date | `#,0.00` |
| Time | `Sales YoY` | Absolute change from Sales Previous Year | `#,0.00` |
| Time | `Sales YoY %` | Relative change; blank without a nonzero prior baseline | `0.0%;-0.0%;` |
| Fulfillment | `Late Orders` | Eligible orders shipped after due date | `#,0` |
| Fulfillment | `On-Time Shipment %` | On-time eligible orders divided by eligible orders | `0.0%` |
| Fulfillment | `Average Fulfillment Days` | Average nonnegative order-to-ship calendar days | `0.0` |
| Inventory | `Inventory Snapshot Lines` | Accepted Inventory Snapshot rows in the selected RLS/filter scope | `#,0` |
| Inventory | `Available Inventory Quantity` | Available quantity at snapshot-line grain | `#,0` |
| Inventory | `Inventory Value` | `on_hand_qty * unit_cost` from the Inventory source snapshot | `#,0.00` |
| Inventory | `Below Reorder Snapshot Lines` | Snapshot lines at or below reorder point | `#,0` |
| Data quality | `Invalid Order Date Lines` | Blank dates in published Gold Sales only; source rejects remain in `control.load_reject` | `#,0` |
| Data quality | `Sales Equation Mismatch Lines` | Published normalized rows that violate the curated equation | `#,0` |
| Data quality | `Missing Customer Country` | Imported Customers without normalized country code | `#,0` |
| Data quality | `Future Customer Records` | Imported Customers flagged future-dated at load | `#,0` |
| Data quality | `Nonpositive Product Cost Records` | Imported Product versions with blank/nonpositive cost | `#,0` |
| Data quality | `DQ Failed Rows` | Sum of failure occurrences; not necessarily distinct rows | `#,0` |
| Data quality | `DQ Failed Checks` | Evaluated failed checks across severities | `#,0` |
| Data quality | `DQ Failed Error Checks` | Evaluated failed Error checks | `#,0` |
| Data quality | `DQ Failed Warning Checks` | Evaluated failed Warning checks | `#,0` |
| Data quality | `DQ Pass Rate` | Check-weighted pass rate over evaluated checks | `0.0%` |
| Data quality | `DQ Not Evaluated Checks` | Checks without an evaluation | `#,0` |
| Data quality | `Overall DQ Status` | `Failed`, otherwise `Not run`, otherwise `Passed with warnings`, otherwise `Passed` | Text |
| Refresh | `Last Dataset Refresh UTC` | Import-evaluation timestamp, not source extraction time | `yyyy-mm-dd hh:mm:ss` |
| Refresh | `Data Through Date` | Latest curated Sales order date in current scope | `yyyy-mm-dd` |
| Context | `Reference Data Label` | Static synthetic-reference disclosure | Text |
| Context | `Access Scope Label` | Visible Customer-country scope only; not Inventory/global DQ scope | Text |
| Context | `Refresh Context Label` | Sales-through date plus dataset refresh timestamp | Text |

## Interpretation rules

- Sales amounts are normalized and recalculated in Silver; do not describe `Total Sales` as the untouched source-recorded value.
- Source rows with malformed order dates are quarantined as `INVALID_ORDER_DATE`. `Invalid Order Date Lines` observes only the published Gold model and therefore does not count those rejects.
- Sales and Inventory relate to Product versions by persisted `product_key`; `product_number` is descriptive and is not unique across history.
- A Product business key may have zero or one current version. Retired Products legitimately have zero; more than one is invalid.
- Inventory value uses snapshot `on_hand_qty * unit_cost`, not available quantity and not Product master cost.
- Inventory snapshot measures are semi-additive over time. A current-state card must select one intentional snapshot date.
- Estimated sales profitability uses effective-dated Product master cost and is not an accounting posting.
- The access-scope label describes Customer-country visibility only. Country RLS independently protects Inventory through Inventory Locations, while Product, Date, global DQ, and Refresh Metadata remain global; the label is not a security proof.

## Ownership and approval

Repository component ownership is documented by role because no named accountable business or data owner is established. A real deployment must assign named owners and approvers before these definitions are used for management reporting. `kpi-catalog.json` is the detailed definition and automated reconciliation contract.
