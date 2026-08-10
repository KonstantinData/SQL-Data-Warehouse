# Power BI Validation

## Current evidence

Power BI Desktop was not available through PATH, standard MSI locations, or an installed Power BI Appx package in the implementation environment. `pbi-tools`, the Power BI authoring CLI, and Tabular Editor were also unavailable.

Therefore the current result can claim only **source validation**, not Desktop, refresh, rendering, interaction, accessibility, service, or production validation.

Run the repository checks:

```text
python scripts/powerbi_validation/validate_powerbi_project.py --root .
python -m unittest discover scripts/powerbi_validation/tests -v
```

Source validation checks JSON parsing, PBIP/PBIR paths and versions, TMDL inventory, semantic references, relationships, parameters, RLS, KPI catalog parity, visual bounds and IDs, mobile-layout coverage, prohibited transient files, credentials, and scope/honesty markers. It is a conservative custom validator, not a complete TMDL, DAX, M, or PBIR schema engine.

## Mandatory Desktop gate

1. Install the integration team's supported current Power BI Desktop.
2. Enable PBIP, TMDL, and PBIR preview options if that Desktop release still requires them.
3. Fully close Desktop, open `powerbi/SQLDataWarehouse.pbip`, and capture the Desktop version.
4. Resolve every blocking, non-blocking, or auto-fix warning. Inspect any upgrade diff before saving.
5. Set non-secret parameters and bind credentials outside source control.
6. Refresh all tables and reconcile 60,379 accepted Gold sales lines plus the Inventory `14/10/4/10` source/accepted/rejected/Gold evidence to the SQL gates. Derive order, quantity, and value totals from the same verified run; treat any difference as a hold until explained.
7. Confirm the six active customer/product/order-date/Inventory relationships and the two inactive Sales date relationships.
8. Evaluate every explicit DAX measure, including blank and zero denominator behaviour.
9. Execute the complete `CountrySalesViewer` matrix below for both Sales and Inventory; test service memberships separately.
10. Inspect every visual for binding errors, empty frames, slicers, cross-filtering, tooltips, reset behaviour, and any implemented navigation/drillthrough.
11. Validate keyboard order, focus, screen-reader names, alt text, High Contrast, non-colour states, and touch targets.
12. Capture 320, 390, 768, 1280, and 1440 px screenshots for default, filtered, no-data, quality-failure, and detail states.
13. Run Performance Analyzer and record material DAX/visual latency.
14. Save, restart Desktop, reopen, rerun the source validator, and review the complete Git diff for generated or upgraded content.

Passing source validation does not prove that Desktop can open, refresh, render, or enforce the project. The phrase `Production validated` must not be used for this implementation.

## RLS validation matrix

Run every row with **View as** in Desktop and repeat the effective role-membership cases in the Power BI Service. Record the identity, UTC execution time, expected country set, actual Sales rows/totals, actual Inventory Snapshot rows/totals, and pass/fail evidence. Product, Date, Data Quality Checks, and Refresh Metadata are intentionally global and must be verified separately as such.

| Identity case | Synthetic setup | Expected Sales | Expected Inventory | Required evidence |
|---|---|---|---|---|
| Allowed | One active, currently valid country entitlement | Only that country's Sales rows and totals | Only that country's Inventory rows and totals | Row counts, representative country values, and total reconciliation |
| Multiple | Two or more active, currently valid country entitlements | Union of entitled countries only | Union of entitled countries only | Per-country and combined totals; no other country visible |
| Inactive | Matching entitlement with `IsActive = FALSE` | Zero protected rows | Zero protected rows | Empty-state screenshot and zero-row/zero-total evidence |
| Expired | Matching entitlement with `ValidToUtc <= UTCNOW()` | Zero protected rows | Zero protected rows | UTC validity evidence and empty-state screenshot |
| Unknown | No matching normalized user principal name | Zero protected rows | Zero protected rows | Tested principal and empty-state screenshot |
| Blank | Blank/null principal or blank entitlement country | Zero protected rows | Zero protected rows | Blank-input evidence and empty-state screenshot |

Also test a user with additive Service role memberships. Any additional role that exposes protected facts invalidates the reference security result even if `CountrySalesViewer` passes in isolation.
