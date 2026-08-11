# Integration handoff

## Integrated state

The Power BI model now reads the curated physical Gold sales model and the Inventory Gold views. The canonical SQLCMD and Python entry points populate both source domains; CI validates runtime, model, Inventory, negative contracts, and Power BI source structure.

## Remaining Desktop gate

1. Open `powerbi/SQLDataWarehouse.pbip` in the supported Power BI Desktop version.
2. Bind a least-privilege development connection and refresh all tables.
3. Compare sales/model counts and Inventory `14/10/4/10` source/accepted/rejected/Gold evidence with SQL gates.
4. Validate measures, relationships, sort order, inactive date relationships, interactions, tooltips, responsive and phone layouts.
5. Execute the full `CountrySalesViewer` matrix (allowed, multiple countries, inactive, expired, unknown, and blank) for both Sales and Inventory; approve the implemented country boundary or replace it with a warehouse entitlement model.
6. Inspect titles, alternative text, tab order, keyboard flow, contrast, and screen-reader names.
7. Record screenshots and the Desktop version; do not commit credentials or transient `.pbi` state.

No publication is authorized until gateway, credentials, role membership, privacy review, refresh schedule, capacity, release owner, and evidence are environment-specific and approved.

Any change to table/measure/page IDs, source grain, refresh parameters, or RLS requires coordinated updates to TMDL, PBIR, KPI catalog, report blueprint, validators, and documentation.

The handoff evidence must cover both domains: Sales counts/totals and Inventory `14/10/4/10` source/accepted/rejected/Gold reconciliation, Inventory quantity/value measures, protected country behaviour, and the intentionally global Product, Date, DQ, and refresh surfaces. A Sales-only screenshot or RLS result is not sufficient evidence for this combined model.
