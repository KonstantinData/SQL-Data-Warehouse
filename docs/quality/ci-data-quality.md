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
| Gold contracts | Enforced | Physical dimensions and facts must preserve declared grains without dropped or multiplied facts. |
| Contract self-tests | Enforced | Deliberate Bronze, Silver, and Gold mutations must produce the expected pass/fail behavior. |

“Bronze is diagnostic” applies only to source-data imperfections. It does not
make missing files, failed or partial `BULK INSERT` calls, wrong fixture row
counts, empty tables, missing objects, or SQL errors non-blocking.

## Executable CI path

`.github/workflows/ci.yml` invokes `scripts/ci/run_ci_checks.sh`. The runner:

1. validates CI wiring statically;
2. starts one isolated, disposable SQL Server container;
3. runs `scripts/ci/run_ci_pipeline.sql` with fail-on-error SQLCMD settings;
4. executes runtime, Bronze diagnostics, Silver, physical Gold, Inventory,
   reproducibility, and end-to-end semantic contracts;
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
- customer deduplication, Product-version preservation, and Bronze-to-Silver lineage;
- normalized customer domains and future-date flag behavior;
- trimmed product attributes, no negative cost, explicit missing-cost handling, and valid ranges;
- valid and unique sales grain, dates, amounts, quantities, and prices;
- customer resolution and the existence of Product history for every sale; and
- unique normalized ERP join keys.

The Gold gate covers:

- required non-empty physical dimension and fact tables plus stable Inventory views;
- non-null, unique surrogate and business keys;
- dimension row-count preservation from Silver;
- fact row-count preservation from Silver, detecting both dropped and multiplied
  facts;
- valid fact grain, dimension references, dates, and measures; and
- hashed rather than raw customer last names.

Missing Product costs preserve the Product identity and are materialized as zero
in Gold so dependent sources remain mappable; Bronze and Power BI expose them as
DQ warnings. Negative costs are quarantined. Sales resolve to the Product version
that was effective on the order date; the resulting Gold surrogate key prevents
version multiplication and lets Power BI use transaction-dated master cost.

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

## Integrated runtime/model contract

The user-facing SQLCMD and Python entry points and the CI entry point call the
same fail-closed runtime procedure before publishing physical Gold and the
Inventory extension. The runtime records batch, step, source-file, watermark,
row-count, error, restart, and quarantine evidence. A source file is reconciled
as published Bronze rows plus distinct durable rejects. The two fixtures whose
empty final field previously exposed a Linux `BULK INSERT` edge case now carry a
final LF; their source attribution records both the current and upstream hashes.

CI additionally proves deliberate Bronze, Silver, and Gold contract failures,
runtime idempotency, rollback after injected failure, model reproducibility, and
Inventory double-run idempotency. Static checks ensure that CI does not contain
an alternative transformation implementation.

## Limitations

Synthetic fixture CI does not establish production throughput, concurrency,
backup/recovery, high availability, privacy controls, authorization, retention,
Power BI refresh behavior, or correctness for rules not encoded in the tests.
The optional Python dependencies in `requirements.txt` are outside this SQL CI
job and use compatible-version ranges. Pinned action and container dependencies require
periodic reviewed updates; reproducibility does not replace maintenance.
