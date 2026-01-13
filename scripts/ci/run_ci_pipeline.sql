/*
================================================================================
CI pipeline runner (SQLCMD mode).
================================================================================
Notes:
  - Drops/recreates the DataWarehouse database.
  - Expects datasets to be available inside SQL Server at /datasets.
================================================================================
*/

:r /workspace/scripts/init.database.sql
:r /workspace/scripts/bronze_layer/create_table_bronze_layer.SQL
:r /workspace/scripts/bronze_layer/bulk_insert_crm_cust_info.sql

EXECUTE bronze.load_bronze @base_path = N'/datasets';

:r /workspace/scripts/silver_layer/create-silver-table-structure.sql
:r /workspace/scripts/silver_layer/cleansing_crm_cust_info.sql
:r /workspace/scripts/silver_layer/cleansing_crm_prd_info.sql
:r /workspace/scripts/gold_layer/create_gold_views.sql
