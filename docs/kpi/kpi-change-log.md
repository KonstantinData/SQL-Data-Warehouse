# KPI Change Log

## Version 1.1.0 - Complete executable catalog

- Status: Source-controlled reference definition
- Effective scope: Synthetic CRM, ERP, and Inventory fixtures
- Prior definition: Version 1.0.0 documented 17 of the executable measures
- Changes: Catalog now covers every executable TMDL measure and uses exact TMDL format strings; no exclusions are declared
- Corrections: `Total Sales` is documented as a normalized Silver recalculation; `Inventory Value` uses snapshot `on_hand_qty * unit_cost`; `Invalid Order Date Lines` is explicitly limited to published Gold rows
- Additions: Sales-line, price, product, fulfillment, DQ severity/status, refresh, and context measures
- Comparability impact: Formulas are unchanged by this documentation revision except for separately implemented DQ status measures; reporting interpretation changed for the three corrected definitions
- Affected pages: Executive Overview, Sales Performance, Data Quality, semantic-model-only Inventory usage, and report context
- Evidence: Bidirectional KPI-to-TMDL name and format reconciliation plus Power BI project validation
- Approval: Not requested; named business owner, data owner, and approver remain environment-specific

## Version 1.0.0 - Reference baseline

- Status: Superseded documentation baseline
- Effective scope: Synthetic repository data only
- Prior definition: None
- Comparability impact: No production history or restatement is asserted
- Affected pages: Executive Overview, Sales Performance, Data Quality
- Evidence: Static PBIP/TMDL/PBIR validation and one-way catalog-to-measure reconciliation at that revision
- Approval: Not requested; business owner and approver were not assigned

Future revisions must record the measure name, prior and proposed formulas, reason, effective version, historical-comparability impact, affected pages and exports, automated evidence, Desktop evidence, owner, approver, and decision.
