# Project purpose, maturity, and boundaries

## Purpose

This repository exists to demonstrate a traceable SQL Server data-warehouse
workflow over six synthetic CRM and ERP CSV extracts. It moves data through raw
Bronze tables, selected Silver cleansing and standardization, and three Gold
analytical views, with orchestration, quality checks, static analysis, and
engineering documentation around that flow.

The repository is a **production-oriented reference implementation using
synthetic data**. “Production-oriented” describes the attention paid to
repeatability, validation, lineage, attribution, and change control. It is **not
a production deployment**, a managed data platform, or evidence of multi-year
operational experience.

## Intended audience and value

- data-engineering learners who need a complete, inspectable warehouse example;
- analytics engineers reviewing grain, keys, transformations, and testability;
- maintainers evaluating how a tutorial baseline can be hardened and governed;
- reviewers or hiring teams assessing repository-backed SQL, Python, CI,
  documentation, and systems-thinking capability.

The value is not the synthetic business result itself. It is the transparent
connection between source contracts, transformations, analytical outputs,
quality gates, limitations, and proposed next steps.

## Maturity

**Current label: active development / reference implementation.**

| Capability | Evidence-backed status |
| --- | --- |
| Synthetic CRM and ERP sources | Implemented; six bundled CSV files |
| Bronze schema and full-refresh loader | Implemented |
| Silver customer and product transformations | Implemented in the standard path |
| Silver sales and ERP loading | Implemented only in the CI-specific loader |
| Gold customer, product, and sales model | Implemented as views |
| Automated end-to-end path | CI profile only |
| SQLCMD local path | Creates all layers but does not populate four Silver tables |
| Python runner | Prototype; does not create Gold and does not preserve database context across its default per-file sessions |
| Quality enforcement | CI checks fail Silver/Gold violations; other SQL checks are diagnostic query sets |
| Production operations | Not implemented or evidenced |

The profile differences are documented in
[`system_architecture.md`](../architecture/system_architecture.md) and
[`dependency_analysis.md`](../data/dependency_analysis.md).

## Implemented scope

- SQL Server database plus `bronze`, `silver`, and `gold` schemas;
- six Bronze heap tables and a configurable `bronze.load_bronze` procedure;
- six Silver heap-table definitions;
- dedicated customer and product transformations;
- a CI-only loader for Silver sales and ERP enrichment tables;
- Gold customer dimension, product dimension, and sales fact views;
- SQLCMD, Python, and CI execution surfaces with explicitly different coverage;
- SQL quality checks and deterministic static repository analysis;
- architecture, lineage, mapping, catalog, dependency, attribution, and lifecycle documentation.

## Explicit non-goals

- real customer, employee, supplier, or operational data;
- live CRM/ERP connectivity, CDC, streaming, or event processing;
- historization, slowly changing dimensions, or multi-snapshot retention;
- production deployment, high availability, disaster recovery, SLAs, or on-call operations;
- enterprise IAM, row-level security, secrets management, observability, or cost controls;
- a Power BI dashboard, machine-learning model, or deployed reporting service;
- claiming the Data With Baraa baseline as independently designed from scratch.

## Reproduction and evidence

Execution prerequisites and commands remain in [`README.md`](../../README.md).
Because the main SQL path is destructive and profile coverage differs, reviewers
should use the CI profile on a disposable SQL Server instance for the strongest
available runtime evidence. Static artifact checks are documented in
[`scripts/analysis/README.md`](../../scripts/analysis/README.md).

Open maturity gaps are expressed as proposals, not completed features, in
[`implementation_proposals.md`](implementation_proposals.md).
