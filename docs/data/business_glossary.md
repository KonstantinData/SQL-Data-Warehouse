# Business Glossary

Definitions apply to the synthetic reference implementation. Named business owners and enterprise policy approvals are not present.

| Term | Definition |
| --- | --- |
| Accepted row | A source row that passed the applicable parse, domain, key, date, mapping, and reconciliation gates and is eligible for publication. |
| Available inventory quantity | Snapshot on-hand quantity minus reserved quantity. It is semi-additive over time. |
| Batch | One audited core or full-pipeline execution attempt recorded in `control.pipeline_batch`. |
| Bronze | Raw-ingestion layer that preserves source provenance and source-shaped values before business cleansing. |
| Business key | A stable source-domain identifier used to match an entity independently of its warehouse surrogate key. |
| Current Product version | A Product row with an open effective interval. A Product business key can have zero or one current row; a retired Product has zero. |
| Data through date | Latest curated Sales order date visible in the current filter/RLS context; not a source-delivery watermark. |
| DQ check | A materialized assertion with scope, severity, evaluated rows, failed rows, status, and evaluation timestamp. |
| DQ reject | A source-row rule occurrence retained with provenance and raw evidence because the row cannot be published safely. |
| Effective interval | Half-open Product-version interval `[effective_from, effective_to)`; a null end denotes the current version. |
| Estimated COGS | Sold quantity multiplied by the effective-dated Product master cost. It is an analytical estimate, not an accounting posting. |
| Estimated gross margin | Estimated gross profit divided by normalized Total Sales; not an approved accounting margin. |
| Gold | Curated dimensional/analytical layer containing conformed dimensions, facts, and stable Inventory views. |
| Inventory snapshot | Full point-in-time stock state at the grain of snapshot date, warehouse, and Product version. |
| Inventory value | Inventory source snapshot `on_hand_qty * unit_cost`; it does not use available quantity or Product master cost. |
| Normalized price | Positive Silver price derived from the absolute source price, or from absolute source sales divided by absolute quantity when price is zero or missing. |
| Normalized sales amount | Silver recalculation of accepted positive quantity multiplied by normalized positive price. |
| Product master cost | Effective-dated CRM Product cost used for estimated sales profitability; separate from Inventory snapshot unit cost. |
| Published state | Data committed by a specific layer. Bronze, Silver, and Gold/Inventory have distinct publication boundaries; a later failure does not imply every prior layer rolled back. |
| Quarantine | Durable separation of invalid source rows from the published dataset while retaining diagnostic evidence. |
| RLS scope | Power BI country entitlement applied through Customers to Sales and independently through Inventory Locations to Inventory Snapshots. Product, Date, materialized DQ, and Refresh Metadata remain global. The `Access Scope Label` itself enumerates only visible Customer country codes. |
| SCD2 | Slowly changing dimension type 2: Product attribute history is represented by effective-dated version rows. |
| Semi-additive measure | A measure additive across dimensions such as warehouse or Product but not safely additive across snapshot dates. |
| Silver | Typed, normalized, deduplicated, validated, and reconciled publication layer. |
| SnapshotAsOf | Operator-supplied business date used for Product disappearance/version closure behavior; it is not persisted in the current batch audit contract. |
| Source version | Operator-supplied immutable delivery identifier used for idempotency within a pipeline scope. |
| Source watermark | Monotonically increasing delivery sequence. In `control.load_watermark` it represents successfully published core Silver delivery, not necessarily end-to-end full-pipeline success. |
| Superseded duplicate | Valid Inventory row excluded because a later extraction (then larger source-row ID) wins at the same business grain. |
| Total Sales | Sum of curated normalized sales amounts; not the untouched source-recorded `sls_sales` total. |
| Unknown member | Reserved dimension row with key `0` used to preserve accepted facts whose dimension reference cannot be resolved unambiguously. |

## Ownership boundary

Repository maintainers own the executable reference artifacts. Business meaning, targets, severity policy, accounting use, and production approval require named accountable owners in the deployment environment.
