# Power BI Validation

## Current evidence

Desktop refresh passed on 2026-08-15 with Power BI Desktop 2.156.951.0.

`powerbi/SQLDataWarehouse.pbip` opened from source, refreshed all tables from the loopback-only `127.0.0.1/DataWarehouse` acceptance database, saved, closed, and reopened with the refreshed state intact. Credentials were stored outside source control through Desktop's data-source settings.

The refreshed report reconciled the visible executive totals to the SQL acceptance evidence: Total Sales `29,351,258`, Germany `2,894,066`, and United States `9,162,225`. Executive Overview, Sales Performance, and Data Quality rendered without visual query errors. The Data Quality register evaluated nine checks. Every Error-severity gate passed; three declared warnings remained: `missing_customer_country`, `nonpositive_product_cost`, and `product_pre_history_coverage`. The last warning identifies sales dated before the first available product master-data version without misclassifying those known product numbers as unresolved source references.

The three rendered page captures and their SHA-256 hashes are recorded in `docs/powerbi/evidence/desktop-poc/2026-08-15/manifest.json`. The images contain only the report canvas; Power BI application chrome and the Windows identity were excluded.

Closeout verification on 2026-08-15 passed 66 Power BI tests, 6 repository tests, 8 Inventory fixture tests, 12 documentation-analysis tests, the documentation and CI-wiring validators, the versioned performance-evidence validator, and the complete isolated SQL Server CI runner. The latter passed runtime, pipeline, model, Inventory, quality, negative, restart, idempotency, reproducibility, and `powerbi_rls_data_contract.sql` gates and removed its temporary container.

Performance Analyzer acceptance also passed on 2026-08-15. The versioned raw exports, hashes, thresholds, before/after values, result-row contracts, and reproduction steps are documented in `docs/powerbi/performance-analyzer.md` and `powerbi/performance/performance-evidence.json`.

The accessibility/responsive source slice adds strict PBIR checks for focus order, visible titles, 20-to-250-character alt text, screen-reader naming, contrast, non-color cues, full-width phone geometry, gaps, mobile omissions, and an honest runtime evidence manifest. See `docs/powerbi/accessibility-responsive-evidence.md`. The current PBIR was rendered in Desktop for the three default landscape pages. Exact viewport widths, phone-device rendering, NVDA, High Contrast, touch hitboxes, and filtered/no-data variants remain `NOT_EXECUTED` and are not inferred from those captures.

The RLS source/fixture and SQL baseline contracts are recorded in `rls-acceptance.md`. The six identity cases are machine-validated for Sales and Inventory. Desktop opened the `View as` dialog and resolved the `CountrySalesViewer` role, but the automation bridge could not enter the synthetic identity text; no Desktop identity result is claimed.

This is **functional Desktop PoC acceptance evidence**, not production validation. It is sufficient to demonstrate the repository's end-to-end reporting capability to reviewers. Exhaustive interaction/accessibility sweeps, exact responsive viewport screenshots, Desktop `View as`, Power BI Service role memberships, gateway configuration, and Service refresh remain outside this local acceptance run.

Power BI Service release status on 2026-08-15: **UNKNOWN / HOLD**. No authenticated Service session, workspace/item mapping, gateway/data-source mapping, credential status, manual or scheduled refresh evidence, or RLS group membership was available for verification. Use `service-production-runbook.md` and the fail-closed service contract; do not infer Service state from Desktop acceptance or `.platform` logical IDs.

Run the repository checks:

```text
python scripts/powerbi_validation/validate_powerbi_project.py --root .
python scripts/powerbi_validation/validate_performance_evidence.py --root .
python -m unittest discover scripts/powerbi_validation/tests -v
```

The untouched Service example is a deliberate negative check: `python scripts/powerbi_validation/powerbi_service_contract.py validate --contract powerbi/service/service-contract.example.json` must return `UNKNOWN` and exit code `2`, because it contains no environment evidence.

Source validation checks JSON parsing, PBIP/PBIR paths and versions, Fabric `.platform` metadata and logical IDs, TMDL inventory, semantic references, relationships, parameters, RLS, KPI catalog parity, visual bounds and IDs, focus/mobile order, title/alt-text contracts, persisted contrast pairs, non-color cues, mobile-layout coverage, runtime and Service evidence contracts, prohibited transient files, credentials, and scope/honesty markers. It is a conservative custom validator, not a complete TMDL, DAX, M, or PBIR schema engine.

## Production or external-integration Desktop gate

The following expanded gate is required before a real environment is published or presented as production-ready. It is deliberately stricter than the completed portfolio PoC acceptance above.

1. Install the integration team's supported current Power BI Desktop.
2. Enable PBIP, TMDL, and PBIR preview options if that Desktop release still requires them.
3. Fully close Desktop, open `powerbi/SQLDataWarehouse.pbip`, and capture the Desktop version.
4. Resolve every blocking, non-blocking, or auto-fix warning. Inspect any upgrade diff before saving.
5. Set non-secret parameters and bind credentials outside source control.
6. Refresh all tables and reconcile 60,379 accepted Gold sales lines plus the Inventory `15/11/4/11` source/accepted/rejected/Gold evidence to the SQL gates. Derive order, quantity, and value totals from the same verified run; treat any difference as a hold until explained.
7. Confirm the six active customer/product/order-date/Inventory relationships and the two inactive Sales date relationships.
8. Evaluate every explicit DAX measure, including blank and zero denominator behaviour.
9. Execute the complete `CountrySalesViewer` matrix below for both Sales and Inventory; test service memberships separately.
10. Inspect every visual for binding errors, empty frames, slicers, cross-filtering, tooltips, reset behaviour, and any implemented navigation/drillthrough.
11. Validate keyboard order, focus, screen-reader names, alt text, High Contrast, non-colour states, and touch targets.
12. Capture 320, 390, 768, 1280, and 1440 px screenshots for default, filtered, no-data, quality-failure, and detail states.
13. Re-run the versioned Performance Analyzer method in `docs/powerbi/performance-analyzer.md` after material DAX, model, Power Query, or visual changes.
14. Save, restart Desktop, reopen, rerun the source validator, and review the complete Git diff for generated or upgraded content.

Passing source validation alone does not prove that Desktop can open, refresh, render, or enforce the project. The dated Desktop evidence above covers open, refresh, core rendering, save, and reopen only. The phrase `Production validated` must not be used for this implementation.

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
