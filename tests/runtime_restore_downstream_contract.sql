:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;

IF N'$(ConfirmRuntimeTests)' <> N'RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE'
    THROW 52505, 'Mutating downstream tests require the exact disposable-database confirmation token.', 1;

IF OBJECT_ID(N'gold.CK_runtime_downstream_failure', N'C') IS NOT NULL
    ALTER TABLE gold.fact_sales DROP CONSTRAINT CK_runtime_downstream_failure;

DECLARE @batch_id BIGINT = (
    SELECT TOP (1) batch_id
    FROM control.pipeline_batch
    WHERE source_version = N'ci-downstream-failure-v1'
    ORDER BY batch_id DESC
);

IF @batch_id IS NULL
    THROW 52501, 'Downstream failure batch evidence is missing.', 1;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch
    WHERE batch_id = @batch_id AND status = 'FAILED'
      AND completed_at_utc IS NOT NULL
      AND error_message LIKE N'%CK_runtime_downstream_failure%'
)
    THROW 52502, 'Gold failure did not mark the end-to-end batch FAILED.', 1;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_step
    WHERE batch_id = @batch_id AND step_name = N'gold.star_schema'
      AND status = 'FAILED' AND completed_at_utc IS NOT NULL
)
    THROW 52503, 'Gold failure did not mark the downstream step FAILED.', 1;
IF EXISTS (
    SELECT 1 FROM control.pipeline_step
    WHERE batch_id = @batch_id AND step_name = N'inventory.snapshot'
)
    THROW 52504, 'Inventory ran after the injected Gold failure.', 1;

PRINT 'End-to-end downstream failure audit checks passed.';
GO
