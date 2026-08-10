:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;

DECLARE @batch_id BIGINT;
SELECT TOP (1) @batch_id = batch_id
FROM control.pipeline_batch
WHERE pipeline_name = N'sql-data-warehouse-full-snapshot'
  AND source_version = N'ci-downstream-failure-v1'
  AND status = 'SUCCEEDED'
ORDER BY batch_id DESC;

IF @batch_id IS NULL
    THROW 52510, 'Linked downstream restart did not complete successfully.', 1;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_batch restarted
    JOIN control.pipeline_batch failed ON failed.batch_id = restarted.restart_of_batch_id
    WHERE restarted.batch_id = @batch_id
      AND failed.status = 'FAILED'
      AND failed.source_version = restarted.source_version
      AND failed.source_watermark = restarted.source_watermark
)
    THROW 52511, 'Linked downstream restart lineage is incomplete.', 1;
IF (SELECT COUNT(*) FROM control.pipeline_step
    WHERE batch_id = @batch_id AND status = 'SUCCEEDED'
      AND step_name IN (N'bronze.full_snapshot', N'silver.full_snapshot', N'gold.star_schema', N'inventory.snapshot')) <> 4
    THROW 52512, 'Linked downstream restart lacks successful end-to-end step evidence.', 1;
IF EXISTS (
    SELECT 1 FROM control.load_watermark
    WHERE pipeline_name = N'sql-data-warehouse-full-snapshot' AND batch_id <> @batch_id
)
    THROW 52513, 'Watermarks were not rebound to the successful linked restart.', 1;

PRINT 'Linked downstream restart checks passed.';
GO
