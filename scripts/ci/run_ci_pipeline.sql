/* Canonical container adapter for the repository-owned runtime and model. */
:ON ERROR EXIT

:r /workspace/scripts/init.database.sql
:r /workspace/scripts/bronze_layer/create_table_bronze_layer.sql
:r /workspace/scripts/bronze_layer/bulk_insert_crm_cust_info.sql
:r /workspace/scripts/silver_layer/create_silver_table_structure.sql
:r /workspace/scripts/silver_layer/load_silver.sql

USE DataWarehouse;
GO

DECLARE @batch_id BIGINT;
EXEC control.run_pipeline
    @base_path = N'/datasets',
    @source_version = N'ci-reviewed-synthetic-snapshot-v1',
    @source_watermark = 1,
    @max_reject_rows = 23,
    @restart_of_batch_id = NULL,
    @batch_id = @batch_id OUTPUT;

IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id = @batch_id AND status IN ('SUCCEEDED', 'SKIPPED')
)
    THROW 51900, 'The operational runtime did not publish a successful CI batch.', 1;

GO

:r /workspace/tests/ci_bronze_load_contract.sql
:r /workspace/scripts/gold_layer/create_gold_views.sql
:r /workspace/scripts/source_inventory/run_source_inventory_ci.sql
