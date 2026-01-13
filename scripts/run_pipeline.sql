/*
================================================================================
Run the full pipeline in SQLCMD mode.
================================================================================
Usage:
  1) Enable SQLCMD mode in SSMS / Azure Data Studio.
  2) Run: :r .\scripts\run_pipeline.sql

Notes:
  - This script drops/recreates the DataWarehouse database.
  - BULK INSERT reads from the SQL Server host; ensure datasets/ is accessible.
================================================================================
*/

:r .\scripts\init.database.sql
:r .\scripts\bronze_layer\create_table_bronze_layer.SQL
:r .\scripts\bronze_layer\bulk_insert_crm_cust_info.sql
:r .\scripts\bronze_layer\bronze-load-bronze.sql
:r .\scripts\silver_layer\create-silver-table-structure.sql
:r .\scripts\silver_layer\cleansing_crm_cust_info.sql
:r .\scripts\silver_layer\cleansing_crm_prd_info.sql
:r .\scripts\gold_layer\create_gold_views.sql
