# CI and Data Quality Contract

## Purpose and scope

This repository is a production-oriented reference implementation of a Bronze,
Silver, and Gold SQL Server data warehouse pipeline. All committed CRM and ERP
datasets are synthetic fixtures.

Passing CI demonstrates that the checked-in scripts satisfy the encoded
contracts for those fixtures. It is not evidence of a production deployment,
production-scale operation, service-level performance, or prior live
operational experience.

## Fail-closed principle

CI fails when database initialization, ingestion, transformation, model
creation, or an enforced contract fails. A query that only returns violating
rows is not a gate: enforced checks must raise a SQL error, and the runner must
propagate the non-zero status.

| Stage | Policy | Expected behavior |
| --- | --- | --- |
| Pipeline execution | Enforced | Any SQL, file, schema, load, or transformation failure stops CI. |
| Bronze availability | Enforced | All six source tables must exist and match their synthetic CSV record counts. |
| Bronze data content | Diagnostic | Source anomalies are reported as aggregate warnings and do not fail CI. |
| Silver contracts | Enforced | Cleansed tables must satisfy structural, grain, domain, date, measure, lineage, and relationship rules. |
| Gold contracts | Enforced | Analytical views must preserve declared dimension and fact grains without dropped or multiplied facts. |
| Contract self-tests | Enforced | Deliberate Bronze, Silver, and Gold mutations must produce the expected pass/fail behavior. |

“Bronze is diagnostic” applies only to source-data imperfections. It does not
make missing files, failed or partial `BULK INSERT` calls, wrong fixture row
counts, empty tables, missing objects, or SQL errors non-blocking.

## Executable CI path

`.github/workflows/ci.yml` invokes `scripts/ci/run_ci_checks.sh`. The runner:

1. validates CI wiring statically;
2. starts one isolated, disposable SQL Server container;
3. runs `scripts/ci/run_ci_pipeline.sql` with fail-on-error SQLCMD settings;
4. executes the pipeline, Bronze diagnostics, Silver contract, Gold contract,
   and end-to-end semantic contract;
5. proves expected failure behavior with disposable negative fixtures; and
6. removes its container and credential file on normal and trappable exit paths.

The runner cannot target a shared or external SQL Server. It publishes no SQL
port and selects no pre-existing container. The SQL Server image is pinned by
version and digest; `sqlcmd` is executed from that same image.

`scripts/ci/run_ci_pipeline.sql` may adapt repository paths and the synthetic
fixture root for the Linux container. It must never contain CI-owned `INSERT`,
`UPDATE`, `DELETE`, `MERGE`, cleansing, filtering, or repair logic for Silver.
All transformations under test must be repository implementation scripts.

## Contracts

Bronze diagnostics report stable rule names and aggregate counts only. They do
not print customer, product, or transaction rows.

The Silver gate covers:

- all six required non-empty Silver tables;
- customer and product deduplication and Bronze-to-Silver lineage;
- normalized customer domains and future-date flag behavior;
- trimmed product attributes, non-null non-negative cost, and valid ranges;
- valid and unique sales grain, dates, amounts, quantities, and prices;
- customer resolution and exactly-one product resolution for every sale; and
- unique normalized ERP join keys.

The Gold gate covers:

- required non-empty dimension and fact views;
- non-null, unique surrogate and business keys;
- dimension row-count preservation from Silver;
- fact row-count preservation from Silver, detecting both dropped and multiplied
  facts;
- valid fact grain, dimension references, dates, and measures; and
- hashed rather than raw customer last names.

Product cost `0` is currently valid because the authoritative Product
transformation uses it as the remediation value for missing or negative source
cost. The quality contract must change in the same integration commit if that
business rule changes.

## Credential and log hygiene

The runner generates an ephemeral administrator password unless one is supplied
to the local process. It applies mode `600` to the temporary environment file,
masks the password in GitHub Actions, and supplies it through
`MSSQL_SA_PASSWORD` and `SQLCMDPASSWORD`. It never uses a committed default or a
`sqlcmd -P` argument.

## Local verification

Prerequisites are Docker with Linux container support, Bash, Python 3.9+, OpenSSL,
and access to the pinned Microsoft container image. From the repository root:

```bash
bash scripts/ci/run_ci_checks.sh
```

The runner detects Git Bash on Windows and disables MSYS argument rewriting for
its Docker bind mounts. Invoke it directly:

```powershell
& 'C:\Program Files\Git\bin\bash.exe' scripts/ci/run_ci_checks.sh
```

## Current integration blockers

The CI hardening deliberately does not hide gaps in the current runtime/model:

- authoritative Silver transformations for Sales and the three ERP tables are
  not yet present in the executable repository pipeline;
- the current Bronze loader drops the final record from `cst_info.csv` and
  `CST_AZ12.csv` because those files end without a row terminator while their
  final field is empty; the exact fixture-count gate exposes both lost rows;
- the Bronze procedure catches and prints errors without rethrowing them, so the
  CI Bronze availability contract is a required compensating control; and
- the current derived Product business key is non-unique and can multiply Gold
  facts. The Silver contract requires exactly one Product match, and the Gold
  contract requires exact fact-row preservation rather than accepting this
  fan-out.

Consequently, this isolated commit is expected to fail the full database gate
until the runtime/model work is reconciled. A green static wiring check alone is
not end-to-end verification.

## Runtime/model reconciliation hooks

The integration task owns `scripts/run_pipeline.sql` and
`scripts/orchestrate_pipeline.py`. When the runtime/model commits arrive:

1. add only the authoritative Sales and ERP Silver scripts at
   `CI_RUNTIME_MODEL_INTEGRATION_HOOK` in
   `scripts/ci/run_ci_pipeline.sql`;
2. update `REQUIRED_PIPELINE_INCLUDES` in
   `scripts/ci/check_ci_contract.py` to the identical reviewed order;
3. update `scripts/run_pipeline.sql` and `scripts/orchestrate_pipeline.py` to
   the same complete authoritative order, and add or extend a static parity
   check so CI cannot become green while a user-facing entry point is stale;
4. keep Customer and Product rules aligned with the Silver contracts;
5. resolve Product business-key uniqueness or implement a documented
   effective-date join so Gold preserves Sales grain;
6. reconcile Sales date data types and ERP key normalization with Gold joins;
7. define and assert explicit Bronze-to-Silver lineage rules for Sales and all
   ERP tables, including any intentional rejection counts;
8. preserve the final record of every CSV by reconciling fixture row terminators
   and the Bronze `BULK INSERT` contract;
9. make the Bronze loader rethrow errors when implementation ownership permits;
   and
10. rerun the complete local CI command after combining all commits.

## Limitations

Synthetic fixture CI does not establish production throughput, concurrency,
backup/recovery, high availability, privacy controls, authorization, retention,
Power BI refresh behavior, or correctness for rules not encoded in the tests.
The optional Python dependencies in `requirements.txt` are outside this SQL CI
job and remain unpinned. Pinned action and container dependencies also require
periodic reviewed updates; reproducibility does not replace maintenance.
