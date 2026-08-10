/*
================================================================================
CI SQLCMD pipeline adapter
================================================================================
Purpose:
  Execute repository-owned transformation scripts without CI-only cleansing or
  repair logic. The explicit /workspace paths adapt the repository pipeline to
  the isolated Linux container used by CI.

Integration hook:
  The runtime/model integration must add the authoritative Sales and ERP Silver
  transformation scripts after Product and before Gold. Until those scripts are
  present, fail-closed pipeline and quality contracts intentionally reject the
  incomplete model.
================================================================================
*/

:On Error exit

:r /workspace/scripts/init.database.sql
:r /workspace/scripts/bronze_layer/create_table_bronze_layer.sql
:r /workspace/scripts/bronze_layer/bulk_insert_crm_cust_info.sql

USE DataWarehouse;
GO

EXECUTE bronze.load_bronze @base_path = N'/datasets';
GO

:r /workspace/tests/ci_bronze_load_contract.sql
:r /workspace/scripts/silver_layer/create_silver_table_structure.sql
:r /workspace/scripts/silver_layer/cleansing_crm_cust_info.sql
:r /workspace/scripts/silver_layer/cleansing_crm_prd_info.sql

-- CI_RUNTIME_MODEL_INTEGRATION_HOOK
-- Add only authoritative repository transformations here. CI-owned INSERT,
-- UPDATE, MERGE, DELETE, or repair logic for Silver is prohibited.

:r /workspace/scripts/gold_layer/create_gold_views.sql
