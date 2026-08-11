# Data Quality Rule Catalog

## Contract

The catalog records implemented runtime reject codes and Power BI materialized check keys. Runtime `Error` means the row is not published; `Info` identifies deterministic supersession rather than malformed data. Power BI severities reproduce the executable query. Named data owners are unassigned, so ownership is stated as a repository role.

## Core ingestion and publication rules

| Rule/code | Severity | Scope | Condition | Disposition/response | Owner |
| --- | --- | --- | --- | --- | --- |
| `INVALID_INTEGER` | Error | Core Bronze typed fields | Nonblank text cannot convert to required integer | Record `control.load_reject`; exclude source row from publication | DWH maintainer role; named data owner unassigned |
| `INVALID_DATE` | Error | Customer/ERP date fields | Nonblank text cannot convert to date | Record reject; exclude row | DWH maintainer role; named data owner unassigned |
| `INVALID_DATETIME` | Error | Product effective timestamps | Nonblank text cannot convert to datetime | Record reject; exclude row | DWH maintainer role; named data owner unassigned |
| `VALUE_TOO_LONG` | Error | Core string fields | Value exceeds target length | Record reject with column/raw value; exclude row | DWH maintainer role; named data owner unassigned |
| `MISSING_REQUIRED_KEY` | Error | All six core sources | Required source business/reference key is blank | Record reject; exclude row | DWH maintainer role; named data owner unassigned |
| `RESERVED_CUSTOMER_KEY` | Error | Silver Customer | Customer ID is zero or negative | Record reject; reserve nonpositive IDs for warehouse members | DWH maintainer role; named data owner unassigned |
| `NEGATIVE_PRODUCT_COST` | Error | Silver Product | Product cost is negative | Record reject; exclude Product version | DWH maintainer role; named data owner unassigned |
| `RESERVED_PRODUCT_KEY` | Error | Silver Product | Product ID is zero or negative | Record reject; reserve nonpositive IDs for warehouse members | DWH maintainer role; named data owner unassigned |
| `MISSING_ORDER_KEY` | Error | Silver Sales | Order number is blank | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `MISSING_PRODUCT_KEY` | Error | Silver Sales | Product reference is blank | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `MISSING_CUSTOMER_KEY` | Error | Silver Sales | Customer reference is null | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `INVALID_ORDER_DATE` | Error | Silver Sales | Order date cannot be converted to a valid date | Record reject; exclude line; inspect rejects rather than Gold blank-date measure | DWH maintainer role; named data owner unassigned |
| `INVALID_SHIP_DATE` | Error | Silver Sales | Nonzero ship date cannot be converted | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `INVALID_DUE_DATE` | Error | Silver Sales | Nonzero due date cannot be converted | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `INVALID_DATE_SEQUENCE` | Error | Silver Sales | Ship precedes order, due precedes order, or due precedes ship | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `INVALID_QUANTITY` | Error | Silver Sales | Quantity is null or nonpositive | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `INVALID_PRICE` | Error | Silver Sales | Normalized price is null or nonpositive | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `AMOUNT_OVERFLOW` | Error | Silver Sales | Quantity times normalized price cannot fit target decimal | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `ORPHAN_CUSTOMER` | Error | Silver Sales | Customer does not exist in accepted Silver Customer set | Record reject; exclude line | DWH maintainer role; named data owner unassigned |
| `ORPHAN_PRODUCT` | Error | Silver Sales | Product does not exist in accepted Silver Product set | Record reject; exclude line | DWH maintainer role; named data owner unassigned |

## Inventory onboarding rules

| Rule/code | Severity | Scope | Condition | Disposition/response | Owner |
| --- | --- | --- | --- | --- | --- |
| `MISSING_SOURCE_ROW_ID` | Error | Inventory | Trimmed source row ID is blank | Quarantine in `silver.inventory_snapshot_reject` | DWH maintainer role; named data owner unassigned |
| `SOURCE_ROW_ID_TOO_LONG` | Error | Inventory | Source row ID exceeds 30 characters | Quarantine | DWH maintainer role; named data owner unassigned |
| `DUPLICATE_SOURCE_ROW_ID` | Error | Inventory | Source row ID occurs more than once | Quarantine | DWH maintainer role; named data owner unassigned |
| `UNSUPPORTED_SOURCE_SYSTEM` | Error | Inventory | Normalized source system is not `SYNTHETIC_WMS` | Quarantine | DWH maintainer role; named data owner unassigned |
| `SOURCE_SYSTEM_TOO_LONG` | Error | Inventory | Source-system code exceeds 30 characters | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_SNAPSHOT_DATE` | Error | Inventory | Snapshot date cannot convert using ISO date style | Quarantine | DWH maintainer role; named data owner unassigned |
| `MISSING_WAREHOUSE_CODE` | Error | Inventory | Normalized warehouse code is blank | Quarantine | DWH maintainer role; named data owner unassigned |
| `WAREHOUSE_CODE_TOO_LONG` | Error | Inventory | Warehouse code exceeds 30 characters | Quarantine | DWH maintainer role; named data owner unassigned |
| `WAREHOUSE_MAPPING_NOT_FOUND` | Error | Inventory | No active controlled warehouse mapping exists | Quarantine; do not infer from display name | DWH maintainer role; named data owner unassigned |
| `MISSING_WAREHOUSE_NAME` | Error | Inventory | Source warehouse label is blank | Quarantine | DWH maintainer role; named data owner unassigned |
| `WAREHOUSE_NAME_TOO_LONG` | Error | Inventory | Source warehouse label exceeds 100 characters | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_PRODUCT_ID` | Error | Inventory | Product ID cannot convert to integer | Quarantine | DWH maintainer role; named data owner unassigned |
| `RESERVED_PRODUCT_ID` | Error | Inventory | Product ID is zero or negative | Quarantine | DWH maintainer role; named data owner unassigned |
| `MISSING_PRODUCT_NUMBER` | Error | Inventory | Normalized Product number is blank | Quarantine | DWH maintainer role; named data owner unassigned |
| `PRODUCT_NUMBER_TOO_LONG` | Error | Inventory | Product number exceeds 50 characters | Quarantine | DWH maintainer role; named data owner unassigned |
| `PRODUCT_MAPPING_NOT_FOUND` | Error | Inventory | Business key plus snapshot date matches no non-Unknown Gold Product version | Quarantine | DWH maintainer role; named data owner unassigned |
| `PRODUCT_MAPPING_AMBIGUOUS` | Error | Inventory | Business key plus snapshot date matches more than one Gold Product version | Quarantine; fail closed | DWH maintainer role; named data owner unassigned |
| `INVALID_ON_HAND_QTY` | Error | Inventory | On-hand quantity cannot convert to integer | Quarantine | DWH maintainer role; named data owner unassigned |
| `NEGATIVE_ON_HAND_QTY` | Error | Inventory | On-hand quantity is negative | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_RESERVED_QTY` | Error | Inventory | Reserved quantity is null after conversion or negative | Quarantine | DWH maintainer role; named data owner unassigned |
| `RESERVED_EXCEEDS_ON_HAND_QTY` | Error | Inventory | Reserved quantity exceeds on-hand quantity | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_REORDER_POINT_QTY` | Error | Inventory | Reorder point is invalid or negative | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_UNIT_COST` | Error | Inventory | Snapshot unit cost is invalid or negative | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVENTORY_VALUE_OVERFLOW` | Error | Inventory | `on_hand_qty * unit_cost` cannot fit `decimal(19,2)` | Quarantine | DWH maintainer role; named data owner unassigned |
| `UNSUPPORTED_CURRENCY_CODE` | Error | Inventory | Normalized currency is not `EUR` | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_EXTRACTED_AT_UTC_FORMAT` | Error | Inventory | Extraction timestamp lacks required UTC `Z` suffix | Quarantine | DWH maintainer role; named data owner unassigned |
| `INVALID_EXTRACTED_AT_UTC` | Error | Inventory | Extraction timestamp cannot convert to UTC datetime | Quarantine | DWH maintainer role; named data owner unassigned |
| `DUPLICATE_SUPERSEDED` | Info | Inventory grain | A later extraction, then greater source row ID, wins at identical snapshot/warehouse/Product grain | Retain reject evidence; count as superseded, not malformed | DWH maintainer role; named data owner unassigned |

## Materialized Power BI checks

| Rule/code | Severity | Scope | Condition | Disposition/response | Owner |
| --- | --- | --- | --- | --- | --- |
| `invalid_order_date` | Error | Gold Sales dataset | `order_date_key = 0` | Mark check failed; Overall DQ Status becomes Failed | BI maintainer role; named data owner unassigned |
| `sales_equation_mismatch` | Warning | Gold Sales dataset | Amount differs from quantity times price, or a component is null | Mark warning failure; status is Passed with warnings unless an Error fails | BI maintainer role; named data owner unassigned |
| `missing_customer_country` | Warning | Gold Customer dataset | Country is null or `Unknown` | Mark warning failure | BI maintainer role; named data owner unassigned |
| `future_customer_record` | Error | Gold Customer dataset | Persisted future-date flag is true | Mark check failed | BI maintainer role; named data owner unassigned |
| `nonpositive_product_cost` | Warning | Current Gold Product rows | Product cost is null or nonpositive | Mark warning failure; note retired Products have no current row | BI maintainer role; named data owner unassigned |
| `sales_line_baseline_variance` | Error | Synthetic fixture | Gold Sales count differs from checked baseline `60,379` | Mark check failed; reference-fixture contract only | BI maintainer role; named data owner unassigned |
| `product_key_uniqueness` | Error | Current Gold Product rows | Current-row count exceeds distinct Product business-key count | Mark check failed; enforce at most one current row | BI maintainer role; named data owner unassigned |
| `gold_source_orphan_coverage` | Error | Gold Sales dataset | Customer or Product surrogate key is Unknown (`0`) | Mark check failed | BI maintainer role; named data owner unassigned |

## Status precedence and reconciliation

`Overall DQ Status` is `Failed` when an evaluated Error check fails; otherwise `Not run` when no check is evaluated or any check is unevaluated; otherwise `Passed with warnings` when a Warning check fails; otherwise `Passed`. Runtime reconciliation separately proves source rows equal accepted plus rejected/superseded rows where applicable. This catalog does not turn a reference severity into an approved production release policy.
