:ON ERROR EXIT

/*
Canonical non-destructive SQLCMD entry point. Run from the repository root with
BasePath, SourceVersion, SourceWatermark, SnapshotAsOf, MaxRejectRows,
RestartOfBatchId.
*/

:r .\scripts\init.database.sql
:r .\scripts\bronze_layer\create_table_bronze_layer.sql
:r .\scripts\bronze_layer\bulk_insert_crm_cust_info.sql
:r .\scripts\silver_layer\create_silver_table_structure.sql
:r .\scripts\silver_layer\load_silver.sql

:r .\scripts\gold_layer\00_create_gold_tables.sql
:r .\scripts\gold_layer\10_load_gold.sql
:r .\scripts\gold_layer\20_create_gold_indexes.sql

:r .\scripts\source_inventory\00_create_objects.sql
:r .\scripts\source_inventory\10_load_bronze.sql
:r .\scripts\source_inventory\20_transform_silver.sql
:r .\scripts\source_inventory\30_create_gold_views.sql

:r .\scripts\operations\run_full_pipeline.sql

USE DataWarehouse;
GO
SELECT
    (SELECT COUNT_BIG(*) FROM gold.dim_customers) AS gold_customer_rows,
    (SELECT COUNT_BIG(*) FROM gold.dim_products) AS gold_product_rows,
    (SELECT COUNT_BIG(*) FROM gold.fact_sales) AS gold_sales_rows,
    (SELECT COUNT_BIG(*) FROM gold.fact_inventory_snapshots) AS gold_inventory_rows;
GO
