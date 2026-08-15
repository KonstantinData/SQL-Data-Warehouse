# Power BI RLS acceptance

## Evidence boundary

This record covers the repository's deterministic synthetic fixtures, the `CountrySalesViewer` source contract, and the Gold country baselines enforced by SQL tests. It does not claim Power BI Service role membership, gateway, production identity, or deployment evidence. Desktop `View as` is a separate Tabular-engine gate and must be recorded only when it has actually been executed.

The machine-readable contract is `powerbi/rls-acceptance-matrix.json`. Its evaluation instant falls within every current-valid fixture window and after the expired fixture window. All committed identities use the reserved `.invalid` domain.

## Matrix contract

Totals for an empty protected scope are expected to be DAX `BLANK`, while row-count measures are expected to be `0`. This preserves normal KPI blank semantics without weakening the fail-closed result.

| Case | Synthetic identity | Expected countries | Sales rows | Sales total | Inventory rows | Available quantity | Inventory value |
|---|---|---|---:|---:|---:|---:|---:|
| Allowed | `analyst.de@example.invalid` | DE | 5,625 | 2,894,066.00 | 10 | 335 | 4,826.00 |
| Multiple | `analyst.multiple@example.invalid` | DE, US | 26,091 | 12,056,291.00 | 11 | 355 | 5,138.50 |
| Inactive | `inactive@example.invalid` | none | 0 | BLANK | 0 | BLANK | BLANK |
| Expired | `expired@example.invalid` | none | 0 | BLANK | 0 | BLANK | BLANK |
| Unknown | `unknown@example.invalid` | none | 0 | BLANK | 0 | BLANK | BLANK |
| Blank | `blank.country@example.invalid` with empty `CountryCode` | none | 0 | BLANK | 0 | BLANK | BLANK |

The DE baseline is Sales `5,625 / 2,894,066.00` and Inventory `10 / 335 / 4,826.00`. The US baseline is Sales `20,466 / 9,162,225.00` and Inventory `1 / 20 / 312.50`. The Multiple case is their exact additive union; no other country is entitled.

## Reproducible checks

Run from the repository root:

```text
python scripts/powerbi_validation/validate_powerbi_project.py --root .
python -m unittest discover -s scripts/powerbi_validation/tests -p "test_*.py" -v
python -m unittest discover -s tests/source_inventory -p "test_*.py" -v
```

The container CI executes `tests/powerbi_rls_data_contract.sql`. That SQL test derives the DE/US Sales and Inventory baselines from curated Gold objects and fails on any row-count, total, country, or union drift. `tests/source_inventory/quality_checks.sql` independently enforces the Inventory country split and the `15 = 11 accepted + 4 rejected` reconciliation.

## Local execution record (2026-08-15)

The following evidence was produced from branch `codex/powerbi-rls-acceptance` at `2026-08-15T11:08:10Z`:

| Gate | Result | Evidence |
|---|---|---|
| Repository RLS contract | PASS | Source validator completed without errors. |
| Validator regressions | PASS | 28 tests passed, including Multiple, Blank, entitlement isolation, and metric-drift failures. |
| Inventory fixture contract | PASS | 7 tests passed; accepted DE and US fixtures are present. |
| End-to-end SQL CI | PASS | Runtime, pipeline, model, Inventory, quality, negative, restart, and `powerbi_rls_data_contract.sql` checks passed. |
| Desktop project load | PASS | `SQLDataWarehouse.pbip` opened and its local Analysis Services process was running and responsive. |
| Desktop `View as` matrix | NOT EXECUTED | The automation target resolved to the Codex window and then returned `foreground window did not report a process id`; no row-level result was observed or claimed. |
| Power BI Service | NOT EXECUTED | No workspace, role membership, gateway, or production identity was used. |

The PASS rows above are reproducible from repository commands. The two NOT EXECUTED rows remain runtime gates and must not be represented as acceptance evidence.

## Desktop View as procedure

1. Refresh `powerbi/SQLDataWarehouse.pbip` against the isolated acceptance database after the current fixture changes.
2. In **Modeling > View as**, select only `CountrySalesViewer`, enable **Other user**, and enter the identity for one matrix row.
3. Capture UTC execution time, visible Customer and Inventory Location country codes, `Sales Lines`, `Total Sales`, `Inventory Snapshot Lines`, `Available Inventory Quantity`, and `Inventory Value`.
4. Confirm the values against the matrix above and repeat for all six cases.
5. Confirm that `Security User Country` exposes only rows for the simulated identity; it must not reveal another identity's entitlement rows.
6. Stop viewing the role, save only reviewed source-generated changes, close, reopen, and rerun the source validator.

Do not treat an Allowed-US Inventory result of zero as sufficient denial evidence in isolation. The fixture includes one accepted US Inventory row specifically so Allowed/Multiple/denial cases exercise a positive Inventory boundary.

## External gates

Power BI Service role membership, additive-role behavior, Build permission, gateway binding, scheduled refresh, and production identity governance remain external integration-owner responsibilities. They are not inferred from Desktop or repository tests.
