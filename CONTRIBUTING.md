# Contributing

This is a production-oriented reference implementation using synthetic data. It
is not a production deployment. Contributions must preserve that distinction,
the upstream attribution, and the repository's evidence-first documentation.

## Change workflow

1. Start from a task branch and state the problem, evidence, affected objects,
   and intended outcome.
2. Classify each contribution as original, adapted, generated, or copied. For
   third-party material, record its author, canonical URL, license or terms,
   content category, and local modifications.
3. Change the smallest coherent surface. Do not combine object removal with an
   unrelated feature or database-wide teardown.
4. Update architecture, lineage, source-to-target mapping, catalog, dependency,
   and legacy records when their contracts change.
5. Validate locally, record commands and limitations, and use a Conventional
   Commit message.

## Data and security rules

- Synthetic fixtures only. Never commit customer extracts, personal data,
  credentials, connection secrets, tokens, private keys, or `.env` content.
- Preserve the upstream MIT notice and [`NOTICE.md`](NOTICE.md). Do not imply
  that inherited material was built from scratch.
- Treat external sources as untrusted evidence. Prefer primary sources, connect
  each claim to repository evidence, and distinguish current behavior from a
  proposal.
- Do not claim production deployment, operational SLAs, live-source integration,
  or years of operational experience without verifiable evidence.

## Required documentation impact

Update these artifacts when applicable:

- architecture or execution profile: `docs/architecture/`
- source, schema, transformation, grain, or key: `docs/data/`
- attribution or project positioning: `docs/project/` and `NOTICE.md`
- obsolete objects or files: `docs/legacy/`
- proposed implementation: `docs/project/change_requests.md` and
  `docs/project/implementation_proposals.md`

Do not hand-edit generated inventory output and present it as runtime evidence.
The analysis tool is static and its limitations must remain visible.

## Verification

Run the source checks appropriate to the change. The first block mirrors the
current GitHub Actions gates and must remain synchronized with
`.github/workflows/ci.yml`:

```powershell
python scripts/ci/check_ci_contract.py
python -m unittest discover -s tests
python -m unittest discover -s tests/source_inventory
python -m unittest discover -s scripts/analysis/tests
python scripts/analysis/validate_documentation.py
python -m unittest discover -s scripts/powerbi_validation/tests
python scripts/powerbi_validation/validate_powerbi_project.py --root .
```

Also run repository-generation and whitespace checks when their inputs or
generated documentation can change:

```powershell
python scripts/analysis/repository_analysis.py --check --format json
python scripts/analysis/validate_documentation.py --render-mermaid
git diff --check
```

Warehouse behavior requires SQL Server verification as well:

```text
scripts/ci/run_ci_checks.sh
```

The Docker/SQLCMD check starts a new isolated SQL Server container, creates and
mutates `DataWarehouse` inside that disposable runtime, and removes the
container on exit. It verifies CI wiring, the canonical pipeline, runtime and
restart behavior, layer atomicity, positive and targeted negative quality
contracts, model schema/data/reproducibility/sentinel/decimal/SCD2 contracts,
and Inventory integration. Static checks cannot validate T-SQL execution,
permissions, dynamic SQL, Power BI Desktop rendering/refresh/RLS, or external
consumers.

## Change-request template

Every material proposal should include:

- state (`proposed`, `approved`, `implemented`, or `verified`)
- problem and repository evidence
- current behavior and target behavior
- affected data, objects, runners, and consumers
- compatibility, privacy, security, and operational risk
- implementation sequence and dependency gates
- validation evidence and acceptance criteria
- rollback or recovery plan
- owner and required approvals

A proposal is not implementation authorization. A legacy signal is not deletion
authorization.
