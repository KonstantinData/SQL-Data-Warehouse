# Attribution and third-party notice

This repository is an adapted and extended work based substantially on **Data
With Baraa's SQL Data Warehouse Project**, authored by **Baraa Khatib Salkini**.

- Upstream project: <https://github.com/DataWithBaraa/sql-data-warehouse-project>
- Comparison snapshot used for this notice (retrieved 2026-08-10):
  [`92406686380cde6eca208c8b43e6fa40ecd26344`](https://github.com/DataWithBaraa/sql-data-warehouse-project/tree/92406686380cde6eca208c8b43e6fa40ecd26344)
- Upstream license at that snapshot: [MIT License](https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/LICENSE)
- Upstream copyright notice: `Copyright (c) 2024 Baraa Khatib Salkini`

The upstream project supplies the educational project brief, synthetic CRM and
ERP datasets, Bronze/Silver/Gold warehouse pattern, baseline T-SQL objects,
quality-check examples, and supporting learning materials. Of the six
upstream-derived CRM/ERP CSV files, four are byte-identical to the corresponding
files in the cited upstream snapshot. Two were renamed and normalized only with
a final line feed so SQL Server imports their last records consistently. The
seventh CSV is the repository-specific synthetic Inventory fixture. The precise
current and upstream SHA-256 hashes are recorded in
[`docs/project/attribution.md`](docs/project/attribution.md).

Repository-specific work by Konstantin Milonas includes adaptations and
extensions visible in Git history: local object and filename conventions,
configurable Bronze loading, focused customer and product cleansing, privacy-
oriented Gold projection changes, SQLCMD/Python/CI orchestration, CI-enforced
checks, and the analysis, lineage, catalog, change-governance, and professional
narrative artifacts in this repository. See
[`docs/project/attribution.md`](docs/project/attribution.md) for the detailed
boundary. These statements describe repository evidence, not a claim that the
baseline was independently created from scratch.

## Dataset and course-material caution

The upstream GitHub repository publishes an MIT license. Separately, Data With
Baraa's official [SQL course page](https://www.datawithbaraa.com/wiki/sql) states
that course and project materials are for learning and personal use, prohibits
commercial use, redistribution, or resale, and requests credit. The relationship
between those website terms and every file in the GitHub repository is not
clarified here. Because the bundled datasets are unchanged upstream materials,
do not assume unrestricted commercial dataset rights; obtain clarification or
replace them with independently licensed synthetic fixtures before commercial
reuse or redistribution.

No affiliation with or endorsement by Data With Baraa is implied. The complete
license text and preserved notices are in [`License.txt`](License.txt).
