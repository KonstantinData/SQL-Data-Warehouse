:ON ERROR EXIT

/*
================================================================================
Supported non-destructive operational entrypoint (SQLCMD mode)
================================================================================
Required variables:
  BasePath        Absolute path visible to the SQL Server service account.
  SourceVersion   Immutable identifier for the exact six-file snapshot.
  SourceWatermark Positive, monotonically increasing delivery sequence.
  MaxRejectRows   Explicit quarantine ceiling (0 is strictest).

Required RestartOfBatchId: use 0 for a new snapshot; a positive value must
reference a matching FAILED batch.
================================================================================
*/

USE DataWarehouse;
GO

DECLARE @batch_id BIGINT;
DECLARE @restart_raw NVARCHAR(100) = N'$(RestartOfBatchId)';
DECLARE @restart_of BIGINT = TRY_CONVERT(BIGINT, @restart_raw);
DECLARE @source_watermark BIGINT = TRY_CONVERT(BIGINT, N'$(SourceWatermark)');
DECLARE @max_reject_rows BIGINT = TRY_CONVERT(BIGINT, N'$(MaxRejectRows)');
IF @restart_of IS NULL OR @restart_of < 0
    THROW 51310, 'RestartOfBatchId must be 0 or a positive integer.', 1;
IF @source_watermark IS NULL OR @source_watermark < 1
    THROW 51311, 'SourceWatermark must be a positive integer.', 1;
IF @max_reject_rows IS NULL OR @max_reject_rows < 0
    THROW 51312, 'MaxRejectRows must be zero or greater.', 1;
IF @restart_of = 0 SET @restart_of = NULL;

EXEC control.run_pipeline
    @base_path = N'$(BasePath)',
    @source_version = N'$(SourceVersion)',
    @source_watermark = @source_watermark,
    @max_reject_rows = @max_reject_rows,
    @restart_of_batch_id = @restart_of,
    @batch_id = @batch_id OUTPUT;

SELECT batch_id, pipeline_name, source_version, source_watermark, status,
       restart_of_batch_id, started_at_utc, completed_at_utc,
       error_number, error_message
FROM control.pipeline_batch
WHERE batch_id = @batch_id;
GO
