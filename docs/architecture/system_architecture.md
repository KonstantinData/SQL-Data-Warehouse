# System architecture

## Scope and maturity

The repository is a production-oriented SQL Server and Power BI reference built entirely from synthetic files. It implements one canonical non-destructive execution path plus an explicitly guarded development reset. It is not evidence of a production deployment.

## System context

```mermaid
flowchart LR
    CRM["Synthetic CRM CSV files"] --> RT["Audited runtime"]
    ERP["Synthetic ERP CSV files"] --> RT
    RT --> B["Bronze tables"]
    RT --> Q["control.load_reject"]
    B --> S["Validated Silver tables"]
    S --> G["Physical Gold star schema"]
    INV["Synthetic Inventory snapshot"] --> IB["Inventory Bronze and Silver"]
    G --> IB
    IB --> IV["Inventory Gold views"]
    G --> PBI["Power BI semantic model and reports"]
    IV --> PBI
    CI["Fail-closed CI and negative tests"] --> RT
    CI --> G
    CI --> IV
```

## Components

| Component | Responsibility | Primary artifact |
| --- | --- | --- |
| Safe bootstrap | Create database and schemas without dropping published state | `scripts/init.database.sql` |
| Runtime control | Batch, step, watermark, reject, restart, lock, and failure contracts | `scripts/control/create_runtime_control.sql` |
| Bronze publication | Load six CSVs, validate shape/types, quarantine rejects, publish atomically | `bronze.load_bronze` |
| Silver publication | Clean and publish all CRM and ERP subjects atomically | `silver.load_silver` |
| Gold model | Persist customer/product/date dimensions and sales fact with constraints/indexes | `scripts/gold_layer/create_gold_views.sql` |
| New-source onboarding | Load, normalize, map, reject, and publish Inventory snapshots | `scripts/source_inventory/` |
| Power BI | Curated Gold imports, explicit measures, RLS example, report definitions | `powerbi/` |
| Verification | Runtime/model/data-quality/negative/Inventory/performance contracts | `tests/`, `scripts/ci/` |

## Warehouse object model

```mermaid
flowchart TB
    subgraph Control
        PB["pipeline_batch"]
        PS["pipeline_step"]
        WM["load_watermark"]
        RJ["load_reject"]
    end
    subgraph Core
        B["Six Bronze source tables"] --> S["Six Silver subject tables"]
        S --> DC["gold.dim_customers"]
        S --> DP["gold.dim_products"]
        S --> DD["gold.dim_date"]
        DC --> FS["gold.fact_sales"]
        DP --> FS
        DD --> FS
    end
    subgraph Inventory
        IR["inventory_snapshot_raw"] --> IS["silver.inventory_snapshot"]
        IS --> IF["gold.fact_inventory_snapshots"]
        IL["gold.dim_inventory_locations"] --> IF
        DP --> IF
    end
    PB --> PS
    PB --> WM
    PB --> RJ
```

Gold customer, product, date, and sales objects are physical tables with persisted surrogate keys and trusted constraints. Inventory is published as stable Gold views over typed Silver data and the shared product dimension.

## Execution profiles

| Profile | Behavior | Intended use |
| --- | --- | --- |
| `scripts/run_pipeline.sql` | Canonical CRM/ERP runtime, Gold model, Inventory | SQLCMD/SSMS execution |
| `scripts/orchestrate_pipeline.py` | Calls the same canonical SQLCMD file and keeps passwords out of argv | Local/operator automation |
| `scripts/ci/run_ci_checks.sh` | Pinned isolated SQL Server, positive and negative gates, cleanup | CI and reproducible local verification |
| `scripts/operations/reset_development.sql` | Guarded destructive reset | Disposable development database only |

## Integrity and operational boundaries

- CRM/ERP publication is serialized with an application lock and audited before mutation.
- Failed Bronze or Silver publication preserves the previous published layer and rethrows the original error.
- Every source row is reconciled to Bronze publication or durable quarantine.
- Successful immutable source versions replay as `SKIPPED`; linked restarts require a compatible failed batch.
- Gold rejects ambiguous grains, preserves Unknown members, and supports stable reruns.
- Inventory loading is independently transactional and idempotent.
- Power BI refresh must follow successful warehouse and quality gates.

Production services such as scheduling, alert routing, backups, disaster recovery, gateway binding, Power BI Service deployment, accountable approvals, and environment-specific secrets remain deployment responsibilities.

## Related evidence

- [Data lineage](data_lineage.md)
- [Source-to-target mapping](../data/source_to_target_mapping.md)
- [Data catalog](../data/data_catalog.md)
- [Dependency analysis](../data/dependency_analysis.md)
- [Runtime runbook](../operations/runtime-runbook.md)
