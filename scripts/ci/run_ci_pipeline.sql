/* Canonical container adapter for the audited end-to-end runtime. */
:ON ERROR EXIT

:r /workspace/scripts/init.database.sql
:r /workspace/scripts/bronze_layer/create_table_bronze_layer.sql
:r /workspace/scripts/bronze_layer/bulk_insert_crm_cust_info.sql
:r /workspace/scripts/silver_layer/create_silver_table_structure.sql
:r /workspace/scripts/silver_layer/load_silver.sql

:r /workspace/scripts/gold_layer/00_create_gold_tables.sql
:r /workspace/scripts/gold_layer/10_load_gold.sql
:r /workspace/scripts/gold_layer/20_create_gold_indexes.sql

:r /workspace/scripts/source_inventory/00_create_objects.sql
:r /workspace/scripts/source_inventory/10_load_bronze.sql
:r /workspace/scripts/source_inventory/20_transform_silver.sql
:r /workspace/scripts/source_inventory/30_create_gold_views.sql

:r /workspace/scripts/operations/run_full_pipeline.sql
:r /workspace/tests/ci_bronze_load_contract.sql
