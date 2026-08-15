# Report Design Contract

## Product framing

The report helps a sales leader or analyst answer, in order:

1. What changed?
2. Which product, market, or fulfilment driver explains it?
3. Is the result trustworthy enough to communicate?

Entry point: **Executive Overview**. The next best action is to inspect the largest contribution or open **Data Quality** when the trust state is not acceptable.

## Storyboard and information hierarchy

### Executive Overview

- Primary: Total Sales, Estimated Gross Profit, Estimated Gross Margin %, Distinct Orders.
- Secondary: monthly sales versus previous year; ranked product-line contribution.
- Tertiary: accessible country table with sales, year-over-year change, and on-time shipment.
- Decision: identify the dominant driver or hold communication when quality evidence is insufficient.

### Sales Performance

- Primary: Total Quantity, Customer Count, Average Order Value, On-Time Shipment %.
- Secondary: category contribution and monthly estimated gross-profit trend.
- Tertiary: product table with sales and estimated profitability.
- Decision: select a product driver and compare trend and product detail without exposing personal customer fields.

### Data Quality

- Primary: global overall DQ status, Error failures, Warning failures, unevaluated checks, pass rate, and last model refresh UTC.
- Secondary: exception counts for dates, amount reconciliation, country mapping, future customer dates, and product cost.
- Tertiary: check register with severity, evaluation state, and failed rows.
- Decision: hold release for Error failures; investigate Warnings; never treat an unevaluated check as a pass.

The global release status, register, Product-cost check, and refresh timestamp are not country-filtered. Sales, Customer, Inventory, and data-through measures follow `CountrySalesViewer`. Visual labels and stakeholder evidence must preserve that distinction even when global and protected measures appear on the same page.

## Interaction contract

- Charts cross-filter within their page. A selected state must remain visible by text or border, not colour alone.
- Date, permitted country, and product-line slicers are intended to synchronize across analysis pages after Desktop authoring verification.
- Reset Filters must use a documented default bookmark based on the latest available data, not today's date.
- Empty state: `No rows in selected scope. Clear filters or contact your access administrator.`
- Refresh failure: show the failure state and last successful data-through date; do not preserve a false green state.
- Missing RLS entitlement returns zero protected Sales and Inventory rows and must not expose out-of-scope protected totals or entitlement lists. Intentionally global Product, Date, data-quality, and refresh metadata remain visible and must be labelled as global.
- Wide tables are excluded from the phone overview; users open the desktop/table view for detail.

Bookmark, slicer synchronization, reset controls, tooltip, drillthrough, and page navigation require Desktop authoring and are explicitly pending. The source-authored pages already expose page title, synthetic-data status, access scope, data-through date, refresh context, headline measures, analysis visuals, and embedded visual titles/alt text. The files do not claim the pending interactions are already rendered.

## Visual system

- Font: Segoe UI.
- Page title: 24 pt semibold; section title: 16 pt semibold; body and axes: at least 11 pt desktop and 12 pt phone.
- Spacing: 8 px system; 32 px desktop outer margin; 16 px gutters.
- Colours: background `#F7F9FC`, surface `#FFFFFF`, primary `#146C94`, text `#1F2937`, muted `#52606D`, border `#667085`, positive `#16794D`, warning `#A15C00`, negative `#B42318`, focus `#005A9E`.
- Status always includes text and/or icon in addition to colour.
- No gradients, maps, custom visuals, decorative hero patterns, or undifferentiated card walls.

The PBIR pages use a 1280x720 canvas. The report blueprint in `powerbi/report-blueprint.json` is the machine-readable layout, accessibility, and interaction contract.

## Accessibility and constrained layouts

- Logical keyboard order: context, filters, reset, headline measures, main chart, secondary chart, detail, navigation/help.
- Decorative objects remain outside tab order.
- Normal text contrast target: 4.5:1. Large text and non-text controls: 3:1.
- Touch targets: at least 44x44 px. PBIR container checks do not prove the internal hitboxes of native visuals.
- Each authored visual has a meaningful title and alt-text contract in the report blueprint.
- Tables must expose headers, units, and sort state; charts require a companion table or Show Data path.
- Colour is never the sole status or comparison signal.

PBIR stores one phone layout, not separate 320 and 390 breakpoints. The supplied portrait layouts use the full 320-unit authored width, at least 100 units of height, and 8-unit gaps. They prioritize the headline, trend/driver, and trust state. Wide tables are intentionally available through the standard landscape report page instead of being compressed into unreadable portrait tables. The 320 and 390 values are runtime acceptance widths, not different authored layouts.

## Acceptance viewports and states

Desktop validation must capture 320, 390, 768, 1280, and 1440 px evidence for:

- default state;
- filtered state;
- no-data/RLS-denied state;
- data-quality failure state;
- any implemented drillthrough or overlay state.

Any clipping, horizontal phone overflow, unreachable filter, hidden context, focus trap, missing alt text, or colour-only state blocks the visual gate.
