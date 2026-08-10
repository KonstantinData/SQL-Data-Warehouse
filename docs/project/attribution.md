# Attribution and own-contribution boundary

## Source baseline

This repository is an adapted and extended derivative of **Data With Baraa's
SQL Data Warehouse Project** by **Baraa Khatib Salkini**.

Primary sources:

- [upstream repository](https://github.com/DataWithBaraa/sql-data-warehouse-project);
- [upstream README at comparison snapshot](https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/README.md);
- [upstream MIT license at comparison snapshot](https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/LICENSE);
- [upstream Bronze DDL](https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/scripts/bronze/ddl_bronze.sql);
- [upstream Silver DDL and procedure](https://github.com/DataWithBaraa/sql-data-warehouse-project/tree/92406686380cde6eca208c8b43e6fa40ecd26344/scripts/silver);
- [upstream Gold DDL](https://github.com/DataWithBaraa/sql-data-warehouse-project/blob/92406686380cde6eca208c8b43e6fa40ecd26344/scripts/gold/ddl_gold.sql);
- [official SQL-course usage notice](https://www.datawithbaraa.com/wiki/sql).

The comparison snapshot is the public upstream `main` tip observed on
2026-08-10. It is a reproducible comparison reference, not a claim that this
repository imported that exact commit. Local Git history begins on 2025-02-19;
the repository does not preserve a shared Git ancestry or an authoritative
import commit for the upstream source.

## Boundary by area

| Area | Upstream baseline | Repository-specific adaptation or extension | Evidence |
| --- | --- | --- | --- |
| Project brief | SQL Server warehouse integrating CRM and ERP CSVs with Bronze/Silver/Gold layers | More explicit maturity, non-goals, execution profiles, and professional narrative | this document and [`project_overview.md`](project_overview.md) |
| Datasets | Six synthetic CRM/ERP CSVs | Four remain byte-identical; two were renamed and normalized only with a final LF so SQL Server does not drop their final record | hashes below; repository paths |
| Object model | Database, three schemas, six Bronze and six Silver tables, Gold dimensions/fact | Local `cust_*` internal names, focused transformations, and Gold privacy-oriented last-name hash | current SQL under `scripts/` |
| Loading | Full-refresh Bronze procedure and Silver transformation baseline | Audited control plane, complete fail-closed Bronze/Silver loaders, one SQLCMD contract, Python wrapper, and CI adapter | loader and runner scripts |
| Quality | Upstream Silver and Gold diagnostic queries | CI-enforced Silver/Gold checks and repository-specific Bronze/CI checks | `tests/` and `scripts/ci/` |
| Engineering documentation | Upstream diagrams, catalog, naming notes, and README | Current architecture, lineage, mapping, catalog, dependency, legacy, change, and attribution artifacts | `docs/` in this repository |
| Static tooling | No corresponding upstream tool at the comparison snapshot | Deterministic standard-library repository inventory and documentation validator | `scripts/analysis/` |

The authoritative boundary is the repository history plus a file comparison to
the pinned upstream snapshot. This summary must be updated if either changes.

## Bundled source data

Four hashes match the pinned upstream snapshot. `cst_info.csv` and `CST_AZ12.csv`
differ only by an added final LF required for complete Linux SQL Server
`BULK INSERT`; their current hashes and the prior upstream hashes are recorded.

| Local file | Upstream filename | SHA-256 |
| --- | --- | --- |
| `datasets/source_crm/cst_info.csv` | `cust_info.csv` | current `b3322c62376048f98f6aea6d1b838101397bed351a01f692a93f33716179173c`; upstream `5e00eca4351886386f6dd01beea91cb17ff081b97b9a02d295b84505448c8040` |
| `datasets/source_crm/prd_info.csv` | `prd_info.csv` | `3db1d644b424aa42599c5a00a8b1297b7b099367c180a63b141f91efac0de45b` |
| `datasets/source_crm/sales_details.csv` | `sales_details.csv` | `0a1e565ae2e8accec217819226d7b71104cc6075590172ed855617a8930a4369` |
| `datasets/source_erp/CST_AZ12.csv` | `CUST_AZ12.csv` | current `b8c81b5ee3443affebac9e7227c444b3da9f96e1fc06894a87e5de75afb2a04f`; upstream `31b697a6a6022085e2f0b1a4d8fe55faae57a54a2d0c8ff91534253b8f3e64a0` |
| `datasets/source_erp/LOC_A101.csv` | `LOC_A101.csv` | `d46239d31bfa3a78e8a6cc8ee97a51e914b2db2db460bfb79a3aeadaf8e028c0` |
| `datasets/source_erp/PX_CAT_G1V2.csv` | `PX_CAT_G1V2.csv` | `ef8048319a2cc23907fc6caf215d31cca5e0bf908590fd6bd7dfa81b48ac9743` |

## Safe professional statement

> Extended and hardened a public SQL Server medallion-architecture learning
> project whose upstream repository carries an MIT license, with an audited
> runtime, physical star schema, new-source
> onboarding, Power BI project, performance evidence, automated validation,
> lineage, catalog, and change governance over synthetic data.

This statement describes demonstrable repository work. It does not claim an
independent greenfield baseline, production deployment, real customer data, or
multi-year warehouse operations.

## License and terms

[`License.txt`](../../License.txt) preserves the upstream MIT copyright notice,
identifies local modifications separately, and is the repository license for
its code and documentation. [`NOTICE.md`](../../NOTICE.md) records the material
and attribution boundary. Of the six upstream-derived CRM/ERP fixtures, four
are byte-identical and two contain only the documented rename/final-LF
normalization; Inventory is repository-specific. The official course-page usage
notice is more restrictive than the repository's MIT statement, and this
repository does not resolve whether those separate terms apply to each fixture.
The six upstream-derived fixtures therefore require clarification or
replacement before commercial reuse or redistribution.
