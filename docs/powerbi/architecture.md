# Power BI Reference Architecture

## Purpose and maturity

This Power BI Project is a source-controlled reference implementation built against the repository's synthetic CRM and ERP sample data. It demonstrates semantic modelling, explicit DAX, report authoring, refresh and RLS design, data-quality communication, and validation workflows. It is not evidence of a production deployment, operational adoption, or historical business performance.

Maturity: **source-authored reference; Power BI Desktop validation pending**.

## Source-controlled format

The project uses the current source-control-friendly Power BI Project approach:

- `SQLDataWarehouse.pbip`: project shortcut.
- `SQLDataWarehouse.SemanticModel`: TMDL semantic model.
- `SQLDataWarehouse.Report`: PBIR enhanced report definition.

PBIP, TMDL, and PBIR remain preview technologies and their schemas can evolve. The checked-in schema family and critical cross-file contracts are statically checked, but the validator is not a complete implementation of Microsoft's JSON schemas or the TMDL grammar. Opening and saving with the integration team's supported Desktop version is the required compatibility gate.

Microsoft references:

- <https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview>
- <https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report>
- <https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview>

## Semantic safety projection

The current Gold views are not safe as the direct analytical contract:

- `gold.dim_products` can contain multiple rows for one `product_number`.
- `gold.fact_sales` joins on that non-unique business key and can multiply source sales lines.
- strict date conversion in `gold.fact_sales` can fail on malformed nonzero dates.

The semantic model therefore imports a bounded projection from Silver:

1. `Products` keeps exactly one latest row per `product_number`, ordered by product start date and product ID.
2. `Sales` retains source sales-line grain and converts dates with `TRY_CONVERT`, preserving invalid dates as blanks.
3. `Customers` keeps one latest row per customer ID and normalizes known country aliases to `US` or `DE` for the illustrative RLS pattern.

This is a temporary correctness boundary, not a replacement for a curated warehouse contract. When the integration task fixes Gold grain, safe dates, and row reconciliation, switch the partitions back to Gold and compare row counts, sales, quantity, and KPI results before accepting the change.

## Model

```text
Customers (1) ---- (*) Sales (*) ---- (1) Products
                       |
                       *
                       |
                     (1) Date
```

- `Customers[customer_id]` to `Sales[customer_id]`: active, single direction.
- `Products[product_number]` to `Sales[product_number]`: active, single direction.
- `Date[Date]` to `Sales[order_date]`: active.
- Date to ship and due date: inactive; activate only in dedicated measures.
- `Data Quality Checks`, `Refresh Metadata`, and `Security User Country` are supporting tables.
- `_Measures` contains all explicit report measures; implicit measures are discouraged.

The Date table is fixed to 2010-2035 so malformed source dates do not make model creation circular. Desktop validation must confirm it is marked as the model date table and covers every valid business date.

## Known source limitations

- Sales order number is not a sales-line key.
- Product master cost is current/static and may be zero or missing.
- Product end dates are not usable for lifecycle reporting in the current sample.
- Gold inner joins can hide source orphans; semantic checks over surviving Gold rows cannot prove source completeness.
- The normal checked-in runners do not provide a verified populated-Gold sequence; the CI path is the only checked-in sequence that loads all Silver inputs and creates Gold.
- Current surrogate keys are generated with `ROW_NUMBER()` and are not stable cross-refresh identifiers.

No incremental refresh is defined because the warehouse rebuild is destructive, no durable change watermark exists, and keys can shift.
