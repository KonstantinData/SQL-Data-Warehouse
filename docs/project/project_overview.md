# Project purpose, maturity, and boundaries

## Purpose

This repository demonstrates a traceable Microsoft SQL Server and Power BI warehouse workflow over synthetic CRM, ERP, and Inventory sources. It covers source analysis, audited ETL, data modelling, data quality, new-source integration, performance analysis, reporting, documentation, operations, and dependency-aware cleanup.

It is a **production-oriented reference implementation**, not a production deployment, managed service, or claim of multi-year operational experience.

## Evidence-backed maturity

| Capability | Status |
| --- | --- |
| Seven synthetic source files and machine-readable Inventory contract | implemented |
| Audited, fail-closed, layer-atomic Bronze/Silver core runtime | implemented and SQL Server 2022 tested |
| Batch/step/Silver-publication watermark/reject/restart control plane | implemented |
| Physical Gold customer/product/date/sales star schema | implemented and rerun-tested |
| New Inventory source from file to Power BI semantic model | implemented and double-run-tested |
| SQLCMD, Python, and CI execution parity | one canonical SQLCMD contract; implemented |
| Fail-closed positive and targeted negative quality tests | implemented |
| Million-row performance case and evidence | implemented, opt-in |
| Source-controlled Power BI semantic model and report | implemented; Desktop gate pending |
| Architecture, lineage, catalog, mapping, operations, KPI, legacy, and proposal documentation | implemented |
| Production scheduling, backup/restore, alerting, gateway, deployment, accountable approvals | not implemented |

## Intended audience

- hiring or technical reviewers assessing repository-backed SQL Server, data modelling, Power BI, quality, optimization, and communication capability;
- data/BI engineers studying a complete, inspectable reference;
- maintainers extending the warehouse with controlled sources and tests.

## Explicit boundaries

- no real customer or operational data;
- no live CRM/ERP/WMS connector, CDC, streaming, or scheduler;
- no Power BI Service deployment or claimed adoption;
- no enterprise IAM, backup/DR, on-call, SLA, or legal approval evidence;
- no claim that the upstream tutorial baseline was independently designed.
- Bronze, Silver, and downstream Gold-plus-Inventory are separate publication
  boundaries; an end-to-end failure does not imply that every earlier layer was
  rolled back.
- `SnapshotAsOf` affects Gold SCD2 retirement but is not stored in the control
  tables; operators must preserve and reuse it for a linked retry.

Runtime commands are in the repository `README.md`; architecture and verification evidence is linked from there. Open future work is separated from completed capabilities in `implementation_proposals.md`.
