# Dependency analysis

## Static object inventory

At the current commit the repository defines one database, three schemas,
twelve tables, one stored procedure, and three views:

- `DataWarehouse`; schemas `bronze`, `silver`, `gold`;
- Bronze tables `bronze.crm_cust_info`, `bronze.crm_prd_info`,
  `bronze.crm_sales_details`, `bronze.erp_cust_az12`,
  `bronze.erp_loc_a101`, `bronze.erp_px_cat_g1v2`;
- same-named six `silver.*` tables;
- procedure `bronze.load_bronze`;
- views `gold.dim_customers`, `gold.dim_products`, `gold.fact_sales`.

Use `python scripts/analysis/repository_analysis.py --check --format markdown`
to reproduce the static inventory. Static success is not runtime evidence.

## Direct object dependencies

| Subject | Reads/references | Writes/defines | Profile |
| --- | --- | --- | --- |
| `bronze.load_bronze` | six CSV filenames through dynamic paths | truncates and bulk-loads six Bronze tables | all runtime profiles |
| customer cleansing | `bronze.crm_cust_info`, `INFORMATION_SCHEMA.COLUMNS` | alters/inserts `silver.crm_cust_info` | SQLCMD, Python, CI |
| product cleansing | `bronze.crm_prd_info` | inserts `silver.crm_prd_info` | SQLCMD and Python only |
| CI Silver loader | Bronze product/sales/ERP plus Silver customer/product | inserts five Silver tables | CI only |
| `gold.dim_customers` | three Silver customer/ERP tables | view result | SQLCMD and CI creation |
| `gold.dim_products` | Silver product and ERP category | view result | SQLCMD and CI creation |
| `gold.fact_sales` | Silver sales plus both Gold dimensions | view result | SQLCMD and CI creation |
| CI quality gate | all three layers | error status through `RAISERROR` | CI only |

## Entrypoint dependency matrix

| Ordered phase | SQLCMD runner | Python runner | CI runner |
| --- | :---: | :---: | :---: |
| database/schemas | yes | yes, separate process | yes |
| Bronze DDL/procedure/load | yes | yes, separate processes | yes |
| Silver DDL | yes | yes, separate process | yes |
| customer transform | yes | yes | yes |
| standard product transform | yes | yes | no |
| Silver sales | no | no | CI-only |
| three Silver ERP tables | no | no | CI-only |
| Gold views | yes | no | yes |
| enforced quality gate | no | no | yes |

The Python runner reuses one `-d` argument but starts a new `sqlcmd` process for
every file. Its default `master` context means the `USE DataWarehouse` statement
from bootstrap does not carry into Bronze DDL. Selecting `DataWarehouse` cannot
bootstrap a truly absent database because the first connection must succeed
before the script can create it. The Python surface is therefore a prototype,
not a verified one-command bootstrap.

## External and tool dependencies

| Dependency | Declaration/use | Status |
| --- | --- | --- |
| SQL Server 2019+ / 2022 CI image | T-SQL runtime, CI service | required for runtime validation |
| `sqlcmd` | SQLCMD includes, Python subprocess, CI tools image | required by all automated runtime paths |
| Docker | local/CI container runner | required by `scripts/ci/run_ci_checks.sh` when starting or targeting containers |
| Python 3.10+ | optional orchestrator, notebooks, static analysis | static analysis uses standard library only |
| `sqlalchemy`, `pandas`, `pyodbc` | `requirements.txt`, notebooks | notebook-only declared dependencies |
| `dbt-core` | `requirements.txt` and tracked log | declared but no `dbt_project.yml`; not an implemented warehouse path |

## Current dependency findings

1. **Incomplete standard flow:** four Silver tables required by Gold have no
   standard loader.
2. **Runner drift:** Python omits Gold and quality checks; CI uses different
   product semantics.
3. **Context drift:** Python's per-file sessions do not preserve bootstrap
   database context.
4. **Hidden failure risk:** Bronze errors are printed but not rethrown.
5. **Imperative schema dependency:** Gold requires `cust_is_future`, but Silver
   DDL does not define it.
6. **Test-reference defect:** the Silver sales date diagnostic reads
   `bronze.crm_sales_details` instead of the Silver table.
7. **Transient keys:** both dimension keys are view-calculated row numbers and
   can change as input ordering/data changes.
8. **No physical integrity:** keys and relationships are logical only.

These are findings and proposals, not proof of a live production incident.

## Runtime impact query set

Before changing or removing an object, supplement static output with SQL Server
catalog evidence:

```sql
SELECT s.name AS schema_name, o.name, o.type_desc, o.create_date, o.modify_date
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
WHERE s.name IN ('bronze', 'silver', 'gold');

SELECT referencing_schema_name, referencing_entity_name,
       referenced_schema_name, referenced_entity_name
FROM sys.sql_expression_dependencies
WHERE referenced_schema_name IN ('bronze', 'silver', 'gold');

SELECT OBJECT_SCHEMA_NAME(object_id) AS schema_name,
       OBJECT_NAME(object_id) AS object_name,
       definition
FROM sys.sql_modules
WHERE definition LIKE '%crm_cst_info%';
```

Catalog queries still do not prove the absence of external BI, notebooks, SQL
Agent jobs, or ad hoc consumers. Owner confirmation and an observation window
remain deprecation gates.
