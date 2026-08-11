# Power BI reference architecture

## Purpose and maturity

This source-controlled Power BI Project uses the repository's synthetic CRM, ERP, and Inventory data. It demonstrates semantic modelling, explicit DAX, report definitions, refresh/RLS design, data-quality communication, and automated source validation. It is not evidence of a production deployment or operational adoption.

Maturity: **source-authored reference; Power BI Desktop validation pending**. Power BI Desktop was not available in the implementation environment.

## Source format

- `SQLDataWarehouse.pbip`: project shortcut.
- `SQLDataWarehouse.SemanticModel`: TMDL semantic model.
- `SQLDataWarehouse.Report`: PBIR enhanced report definition.

PBIP/PBIR compatibility must be confirmed by opening and saving with the supported Desktop release. The repository validator checks important cross-file contracts but is not a substitute for Microsoft's complete schemas or Desktop parser.

## Curated warehouse contract

Core semantic tables import from the corrected physical Gold model:

- `Sales` from `gold.fact_sales`, enriched with the persisted customer business key;
- `Customers` from `gold.dim_customers`;
- `Products` from all effective-dated rows of `gold.dim_products`;
- `Inventory Snapshots` from `gold.fact_inventory_snapshots`;
- `Inventory Locations` from `gold.dim_inventory_locations`.

The former temporary Silver safety projection is retired. SQL runtime/model contracts now prevent product fan-out, unsafe dates, unstable surrogate keys, and silent fact loss before Power BI refresh.

## Model

```text
Customers (1) ---- (*) Sales (*) ---- (1) Product versions (1) ---- (*) Inventory Snapshots (*) ---- (1) Inventory Locations
                       |                                  |
                       *                                  *
                       |                                  |
                     (1) Date -------------------------- (1)
```

- Customer, Product-surrogate, order-date, Inventory-Product-surrogate, Inventory-location, and Inventory-date relationships are active and single-direction.
- Ship and due date relationships are inactive and activated only by dedicated measures.
- Inventory snapshot measures are semi-additive over time; current-stock cards must select one snapshot date.
- `Data Quality Checks`, `Refresh Metadata`, and `Security User Country` are supporting tables.
- `_Measures` owns all explicit sales, fulfillment, profitability, Inventory, quality, and context measures; implicit measures are discouraged.

## Evidence boundary

Static validation covers project structure, model references, relationships, DAX/KPI mapping, report/page/visual metadata, mobile layouts, refresh parameters, reserved synthetic RLS identities, and secret patterns. Desktop remains required for refresh, rendering, interaction, phone layout, accessibility inspection, role execution, and screenshots.

Microsoft references:

- <https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview>
- <https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report>
- <https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview>
