:ON ERROR EXIT

/*
================================================================================
Canonical end-to-end SQLCMD entry point
================================================================================
Run from the repository root. Required SQLCMD variables:
  BasePath, SourceVersion, SourceWatermark, MaxRejectRows, RestartOfBatchId

The script is non-destructive. Use scripts/operations/reset_development.sql only
for an explicitly confirmed reset of a disposable development database.
================================================================================
*/

:r .\scripts\init.database.sql
:r .\scripts\bronze_layer\create_table_bronze_layer.sql
:r .\scripts\bronze_layer\bulk_insert_crm_cust_info.sql
:r .\scripts\silver_layer\create_silver_table_structure.sql
:r .\scripts\silver_layer\load_silver.sql

/* Audited, fail-closed CRM/ERP Bronze and Silver publication. */
:r .\scripts\operations\run_operational_pipeline.sql

/* Materialized star schema, SCD2 product resolution, date dimension, indexes. */
:r .\scripts\gold_layer\create_gold_views.sql

/* Independently idempotent onboarding example for a new Inventory source. */
:r .\scripts\source_inventory\run_source_inventory.sql

USE DataWarehouse;
GO

SELECT
    (SELECT COUNT_BIG(*) FROM gold.dim_customers) AS gold_customer_rows,
    (SELECT COUNT_BIG(*) FROM gold.dim_products) AS gold_product_rows,
    (SELECT COUNT_BIG(*) FROM gold.fact_sales) AS gold_sales_rows,
    (SELECT COUNT_BIG(*) FROM gold.fact_inventory_snapshots) AS gold_inventory_rows;
GO
