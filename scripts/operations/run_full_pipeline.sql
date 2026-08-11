:ON ERROR EXIT

/*
Canonical audited execution after all core, Gold, and Inventory modules exist.
The batch remains RUNNING and retains the session lock until every downstream
publication succeeds. Any Gold or Inventory error marks the same batch FAILED.
*/

USE DataWarehouse;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

DECLARE @batch_id BIGINT;
DECLARE @restart_raw NVARCHAR(100) = N'$(RestartOfBatchId)';
DECLARE @restart_of BIGINT = TRY_CONVERT(BIGINT, @restart_raw);
DECLARE @source_watermark BIGINT = TRY_CONVERT(BIGINT, N'$(SourceWatermark)');
DECLARE @max_reject_rows BIGINT = TRY_CONVERT(BIGINT, N'$(MaxRejectRows)');
DECLARE @snapshot_as_of DATE = TRY_CONVERT(DATE, N'$(SnapshotAsOf)', 23);
DECLARE @batch_status VARCHAR(20);
DECLARE @gold_step_id BIGINT;
DECLARE @inventory_step_id BIGINT;
DECLARE @active_step_name NVARCHAR(128);

IF @restart_of IS NULL OR @restart_of < 0
    THROW 51310, 'RestartOfBatchId must be 0 or a positive integer.', 1;
IF @source_watermark IS NULL OR @source_watermark < 1
    THROW 51311, 'SourceWatermark must be a positive integer.', 1;
IF @max_reject_rows IS NULL OR @max_reject_rows < 0
    THROW 51312, 'MaxRejectRows must be zero or greater.', 1;
IF @snapshot_as_of IS NULL
    THROW 51314, 'SnapshotAsOf must be an ISO date (YYYY-MM-DD).', 1;
IF @restart_of = 0 SET @restart_of = NULL;

EXEC control.run_pipeline
    @base_path = N'$(BasePath)',
    @source_version = N'$(SourceVersion)',
    @source_watermark = @source_watermark,
    @max_reject_rows = @max_reject_rows,
    @restart_of_batch_id = @restart_of,
    @defer_completion = 1,
    @batch_id = @batch_id OUTPUT;

SELECT @batch_status = status
FROM control.pipeline_batch
WHERE batch_id = @batch_id;

IF @batch_status = 'RUNNING'
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        SET @active_step_name = N'gold.star_schema';
        INSERT control.pipeline_step(batch_id, step_name, status, target_name)
        VALUES (@batch_id, N'gold.star_schema', 'RUNNING', N'gold.dim_customers|gold.dim_products|gold.dim_date|gold.fact_sales');
        SET @gold_step_id = SCOPE_IDENTITY();

        EXEC gold.usp_load_gold @snapshot_as_of = @snapshot_as_of;

        UPDATE control.pipeline_step
        SET status = 'SUCCEEDED',
            rows_read = (SELECT COUNT_BIG(*) FROM silver.crm_sales_details),
            rows_accepted = (SELECT COUNT_BIG(*) FROM gold.fact_sales),
            rows_rejected = 0,
            rows_superseded = 0,
            rows_published = (SELECT COUNT_BIG(*) FROM gold.fact_sales),
            completed_at_utc = SYSUTCDATETIME()
        WHERE step_id = @gold_step_id;

        SET @active_step_name = N'inventory.snapshot';
        INSERT control.pipeline_step(batch_id, step_name, status, source_name, target_name)
        VALUES (@batch_id, N'inventory.snapshot', 'RUNNING', N'inventory_snapshots.csv', N'gold.fact_inventory_snapshots');
        SET @inventory_step_id = SCOPE_IDENTITY();

        EXEC bronze.load_inventory_snapshot @base_path = N'$(BasePath)';
        EXEC silver.load_inventory_snapshot;

        UPDATE control.pipeline_step
        SET status = 'SUCCEEDED',
            rows_read = (SELECT COUNT_BIG(*) FROM bronze.inventory_snapshot_raw),
            rows_accepted = (SELECT COUNT_BIG(*) FROM silver.inventory_snapshot),
            rows_rejected = (SELECT COUNT_BIG(*) FROM silver.inventory_snapshot_reject WHERE reason_code <> N'DUPLICATE_SUPERSEDED'),
            rows_superseded = (SELECT COUNT_BIG(*) FROM silver.inventory_snapshot_reject WHERE reason_code = N'DUPLICATE_SUPERSEDED'),
            rows_published = (SELECT COUNT_BIG(*) FROM gold.fact_inventory_snapshots),
            completed_at_utc = SYSUTCDATETIME()
        WHERE step_id = @inventory_step_id;

        UPDATE control.pipeline_batch
        SET status = 'SUCCEEDED', completed_at_utc = SYSUTCDATETIME()
        WHERE batch_id = @batch_id AND status = 'RUNNING';
        IF @@ROWCOUNT <> 1
            THROW 51313, 'End-to-end batch did not transition from RUNNING to SUCCEEDED.', 1;

        COMMIT TRANSACTION;

        EXEC sys.sp_releaseapplock
            @Resource = N'SQL-Data-Warehouse:operational-pipeline',
            @LockOwner = N'Session';
    END TRY
    BEGIN CATCH
        DECLARE @error_number INT = ERROR_NUMBER();
        DECLARE @error_severity INT = ERROR_SEVERITY();
        DECLARE @error_state INT = ERROR_STATE();
        DECLARE @error_procedure NVARCHAR(256) = ERROR_PROCEDURE();
        DECLARE @error_line INT = ERROR_LINE();
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();

        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

        INSERT control.pipeline_step
        (
            batch_id, step_name, status, completed_at_utc,
            error_number, error_severity, error_state, error_procedure,
            error_line, error_message
        )
        SELECT
            @batch_id, COALESCE(@active_step_name, N'downstream.publication'), 'FAILED', SYSUTCDATETIME(),
            @error_number, @error_severity, @error_state, @error_procedure,
            @error_line, @error_message
        WHERE NOT EXISTS (
            SELECT 1 FROM control.pipeline_step
            WHERE batch_id = @batch_id AND step_name = COALESCE(@active_step_name, N'downstream.publication')
        );

        UPDATE control.pipeline_batch
        SET status = 'FAILED', completed_at_utc = SYSUTCDATETIME(),
            error_number = @error_number, error_severity = @error_severity,
            error_state = @error_state, error_procedure = @error_procedure,
            error_line = @error_line, error_message = @error_message
        WHERE batch_id = @batch_id AND status = 'RUNNING';

        BEGIN TRY
            EXEC sys.sp_releaseapplock
                @Resource = N'SQL-Data-Warehouse:operational-pipeline',
                @LockOwner = N'Session';
        END TRY
        BEGIN CATCH
            /* Preserve the downstream failure. */
        END CATCH;

        THROW;
    END CATCH;
END;

SELECT batch_id, pipeline_name, source_version, source_watermark, status,
       restart_of_batch_id, started_at_utc, completed_at_utc,
       error_number, error_message
FROM control.pipeline_batch
WHERE batch_id = @batch_id;
GO
