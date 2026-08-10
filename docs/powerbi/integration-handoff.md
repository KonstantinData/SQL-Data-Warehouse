# Integration Handoff

## Delivered ownership slice

- `powerbi/**`
- `docs/kpi/**`
- `docs/powerbi/**`
- `scripts/powerbi_validation/**`

The implementation intentionally does not modify `README.md`, `scripts/run_pipeline.sql`, `scripts/orchestrate_pipeline.py`, SQL pipeline files, datasets, or CI. The integration task owns those connections.

## Integration steps

1. Cherry-pick the local Power BI reference commit.
2. Review the semantic safety projection against any concurrent Gold fixes. If Gold now guarantees one product row per business key, safe date conversion, and row reconciliation, replace the Silver partitions with curated Gold and re-run the baseline comparison.
3. Link the project and validation commands from integration-owned documentation.
4. Ensure the pipeline produces populated analytical sources before refresh. The checked-in Python runner currently stops before Gold creation, while the normal SQL runner does not load every Silver source.
5. Run both source-validation commands.
6. Complete and record the full Desktop gate in `validation.md`.
7. Do not publish until credentials, gateway, RLS memberships, privacy review, and screenshot evidence are environment-specific and approved.

## Expected source prerequisites

The reference queries require populated `silver.crm_cust_info`, `silver.crm_prd_info`, `silver.crm_sales_details`, `silver.erp_loc_a101`, and `silver.erp_px_cat_g1v2` objects. The SQL account must be read-only and scoped to the minimum objects required by the chosen source contract.

## Acceptance checklist

- Source validators pass and the worktree contains no transient `.pbi` state or credentials.
- Desktop opens and saves the PBIP without unresolved warnings.
- Refresh matches the checked synthetic baseline or a reviewed replacement contract.
- Measures, relationships, RLS, pages, interactions, phone layout, accessibility, and screenshots pass.
- Documentation still labels the result as a synthetic, source-controlled reference and lists remaining limitations.

Any integration change to table names, measure names, page IDs, schema versions, source grain, or RLS contract requires updating the validators, KPI catalog, report blueprint, and affected PBIR/TMDL files together.
