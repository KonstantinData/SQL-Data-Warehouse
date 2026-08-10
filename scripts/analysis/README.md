# Repository analysis utilities

These standard-library Python tools make the repository's static object,
dependency, dataset, legacy-candidate, and documentation contracts reviewable.
They do not connect to SQL Server and do not mutate warehouse objects.

```powershell
python scripts/analysis/repository_analysis.py --check --format json
python scripts/analysis/repository_analysis.py --format markdown
python scripts/analysis/validate_documentation.py
python scripts/analysis/validate_documentation.py --render-mermaid
```

Use `--output <path>` to write an inventory atomically. Output is deterministic:
repository-relative paths, stable ordering, no timestamps, and SHA-256 hashes for
inputs and CSV sources. Exit codes are `0` for success, `1` for a contract drift,
`2` for invalid CLI arguments, and `3` for an incomplete scan or write failure.

The documentation validator checks every tracked or non-ignored Markdown file
for local-link and Mermaid structure, requires the column dictionary, business
glossary, and data-quality rule catalog, and reconciles every committed CSV
source column with the dictionary. It also keeps the structured Inventory
Product-mapping contract aligned with both Silver validation and the Gold view,
including the business key, half-open effective-date interval, cardinality, and
Unknown-member exclusion. `--render-mermaid` invokes `mmdc` when available and
otherwise reports that only structural diagram checks were performed.

Static pattern matching cannot validate SQL Server execution semantics,
permissions, dynamic object names, SQLCMD session behavior, or runtime
dependencies. Pair it with the SQL quality suite and live catalog queries before
approving a destructive change.
