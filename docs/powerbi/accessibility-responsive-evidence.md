# Accessibility and responsive evidence

## Evidence boundary

This record distinguishes source-verifiable PBIR contracts from runtime behavior. The source checks below can prevent regressions in authored metadata and geometry. They do not prove Power BI Desktop, Power BI Service, a mobile app, a screen reader, Windows High Contrast, or an actual touch target inside a visual.

The accessibility and responsive runtime matrix is recorded in `docs/powerbi/evidence/accessibility-responsive/2026-08-15/manifest.json`. Every uncaptured row is explicitly `NOT_EXECUTED`; no screenshot or runtime pass is implied.

## Product and layout contract

- Entry page: Executive Overview.
- Desktop: fixed 1280x720 canvas with `FitToPage` for 768, 1280, and 1440-wide containers.
- Phone: one independent 320-unit portrait layout. Power BI scales this grid across phone sizes; 320 and 390 are runtime acceptance widths, not separate PBIR breakpoints.
- Portrait purpose: triage the headline, driver, trend, and trust state. Wide detail tables remain available by rotating to the standard landscape report page.
- Phone visuals use the full 320-unit authored width, at least 100 units of height, and at least 8 units of vertical separation.
- Minimum interaction target contract: 44x44. Only visual-container geometry is statically measurable; data points, visual-header actions, table cells, and scrollbars require runtime touch testing.

## Source-verified results by page

| Page | Keyboard and focus order | Titles and alt text | Contrast | Non-color encoding | Mobile portrait |
|---|---|---|---|---|---|
| Executive Overview | Context, headline measures, monthly comparison, product-line driver, country detail | Explicit visible titles, screen-reader names, and descriptions of measure scope and comparison | `#1F2937` titles on white and `#667085` boundaries on white meet the declared 4.5:1 and 3:1 thresholds | The two-series trend persists a visible legend and markers; series names and Show data are documented fallbacks | Context, summary, trend, and product-line driver; country detail is an explicit landscape-only table |
| Sales Performance | Context, operating summary, category driver, profit trend, product detail | Technical `product_key` wording removed; measures and sortable table content are named | Same explicit title, surface, and boundary contract | Category/time labels and the accessible data table supplement chart color | Context, summary, category driver, and profit trend; product detail is an explicit landscape-only table |
| Data Quality | Context, global status, global register, selected-scope exceptions, mixed-scope exceptions | Global versus selected scope is stated in titles and descriptions | Same explicit title, surface, and boundary contract | Overall status, severity, evaluation state, and failure counts remain text, not color-only states | Context, global text status, sales exceptions, and mixed-scope exceptions; the diagnostic register is landscape-only |

## Static contrast calculations

Calculated against the persisted white visual surface:

| Role | Color | Ratio | Threshold | Result |
|---|---|---:|---:|---|
| Title text | `#1F2937` | 14.68:1 | 4.5:1 | PASS |
| Muted text | `#52606D` | 6.46:1 | 4.5:1 | PASS |
| Primary | `#146C94` | 5.83:1 | 4.5:1 | PASS |
| Positive | `#16794D` | 5.42:1 | 4.5:1 | PASS |
| Warning | `#A15C00` | 5.19:1 | 4.5:1 | PASS |
| Negative | `#B42318` | 6.57:1 | 4.5:1 | PASS |
| Focus | `#005A9E` | 7.10:1 | 3.0:1 | PASS |
| Visual boundary | `#667085` | 4.97:1 | 3.0:1 | PASS |

These calculations prove only the persisted/declared color pairs. High Contrast remapping and the colors used inside native visuals remain runtime checks.

## Reproduction

Run source validation and unit tests:

```text
python scripts/powerbi_validation/validate_powerbi_project.py --root .
python -m unittest discover scripts/powerbi_validation/tests -v
```

For each runtime row in the manifest:

1. Open `powerbi/SQLDataWarehouse.pbip` in the recorded Power BI Desktop version.
2. Confirm the active page, data state, display mode, Windows scale, and High Contrast state.
3. Validate Tab and Shift+Tab order, visible focus, Enter, arrow keys, Escape, and `Alt+Shift+F11` Show data.
4. Record the exact screen-reader announcement of visible title, native visual type, and alt text.
5. Capture the stated viewport or Power BI layout equivalent without clipping or horizontal overflow.
6. Save the screenshot below the dated evidence directory, calculate SHA-256, and change only that manifest row to `PASS` or `FAIL` with the path, hash, UTC time, surface, version, and note.

## Runtime attempt and known limits

Power BI Desktop 2.156.951.0 opened and refreshed the current PBIR against the synthetic loopback acceptance database. Executive Overview, Sales Performance, and Data Quality rendered without visual query errors; privacy-cropped report-canvas captures and hashes are recorded separately in `docs/powerbi/evidence/desktop-poc/2026-08-15/manifest.json`. This updates the earlier stale runtime-attempt note but does not broaden the accessibility claims:

- 320, 390, 768, 1280, and 1440 exact-width runtime rows remain `NOT_EXECUTED` because the captured Desktop host window does not match those declared viewport widths;
- keyboard focus inside native visuals, NVDA output, High Contrast, touch hitboxes, filtered/no-data states, and Power BI mobile-app behavior remain open runtime gates;
- the three default landscape captures prove current rendering only; source-level responsive and accessibility contracts remain the stronger evidence for authored metadata and geometry.

## External design basis

- Microsoft Power BI accessibility guidance: <https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-accessibility-creating-reports>
- Microsoft Power BI mobile layout: <https://learn.microsoft.com/en-us/power-bi/create-reports/power-bi-create-mobile-optimized-report-mobile-layout-view>
- Microsoft mobile layout best practices: <https://learn.microsoft.com/en-us/power-bi/create-reports/power-bi-create-mobile-optimized-report-best-practices>
- Microsoft report display settings: <https://learn.microsoft.com/en-us/power-bi/create-reports/power-bi-report-display-settings>
