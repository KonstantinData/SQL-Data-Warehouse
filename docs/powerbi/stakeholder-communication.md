# Stakeholder Communication

## Audience and decisions

- Executive and portfolio reviewers: understand direction, estimated profitability, largest drivers, and whether evidence is sufficient for discussion.
- Commercial analysts: inspect product, category, country, and time drivers.
- BI maintainers: decide release, hold, or remediation from refresh and quality evidence.
- Governance and security reviewers: assess RLS design, data minimization, accessibility evidence, and validation gaps.

## Release update template

> Reference version `<version>` is available at commit `<hash>`. Repository source checks: `<status>`. Power BI Desktop validation: `<status>`. Data source: synthetic CRM/ERP and Inventory samples. Sales evidence: `<counts/totals>`. Inventory evidence: `<source/accepted/rejected/Gold counts and quantity/value totals>`. RLS evidence for Sales and Inventory: `<status/link>`. Global DQ and refresh evidence: `<status/link>`. KPI changes: `<summary>`. Known limitations: `<items>`. Decision requested: `<review/accept/hold>`. This update does not represent a production deployment.

## KPI revision template

> `<KPI>` changes from `<old definition>` to `<new definition>` effective in reference version `<version>`. Reason: `<reason>`. Historical comparability: `<unchanged/changed/not assessed>`. Affected pages and exports: `<list>`. Evidence: `<tests/reviewer>`. Approval status: `<proposed/approved/rejected>`.

## Quality alert template

> Validation status: `<failed/passed with warnings>`. Layer/check: `<layer>/<check>`. Evidence time: `<timestamp and timezone>`. Impact: `<affected model/KPIs/pages>`. Release decision: `<hold/continue with warning>`. Owner and next action: `<owner/action>`. Do not interpret this alert as evidence about production data.

## Review cadence

For each reference release, review KPI definitions, CRM/ERP and Inventory source/quality evidence, Desktop validation, Sales and Inventory RLS evidence, global-versus-protected scope labels, accessibility screenshots, known limitations, and the requested decision. Avoid status language such as live, production ready, deployed, or adopted unless separately evidenced in the target environment.
