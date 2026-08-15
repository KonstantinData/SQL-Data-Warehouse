# Integration handoff

## Integrated state

The Power BI model now reads the curated physical Gold sales model and the Inventory Gold views. The canonical SQLCMD and Python entry points populate both source domains; CI validates runtime, model, Inventory, negative contracts, and Power BI source structure.

## Desktop gate status

The dated 2026-08-15 Desktop PoC acceptance covers open, full refresh, visible core rendering, save, close, and reopen against the loopback acceptance database. `validation.md` remains authoritative for the accepted evidence and for Desktop items that were not fully exercised, including the complete RLS matrix, interaction/accessibility sweeps, responsive screenshots, and Performance Analyzer capture.

## Remaining Service production gate

Follow `service-production-runbook.md` and validate an environment-specific copy of `powerbi/service/service-contract.example.json`. The gate requires the authorized workspace and exact Service items, production parameters, Standard gateway/data-source mapping, managed credentials, a reconciled manual refresh, an approved schedule plus one successful scheduled run, governed entitlements, approved RLS group membership, effective Viewer tests, ownership, capacity, privacy approval, and current evidence.

The current source model is a release HOLD: it defaults to `127.0.0.1`/`Development`, its entitlement table contains only synthetic `.invalid` identities, and the dated Desktop refresh reports the global `gold_source_orphan_coverage` check as Error.

No publication is authorized until gateway, credentials, role membership, privacy review, refresh schedule, capacity, release owner, and evidence are environment-specific and approved.

Any change to table/measure/page IDs, source grain, refresh parameters, or RLS requires coordinated updates to TMDL, PBIR, KPI catalog, report blueprint, validators, and documentation.

The handoff evidence must cover both domains: Sales counts/totals and Inventory `14/10/4/10` source/accepted/rejected/Gold reconciliation, Inventory quantity/value measures, protected country behaviour, and the intentionally global Product, Date, DQ, and refresh surfaces. A Sales-only screenshot or RLS result is not sufficient evidence for this combined model.
