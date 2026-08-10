/*
================================================================================
Runtime control plane
================================================================================
Idempotent DDL for batch, step, watermark, and reject evidence plus the
coordinating pipeline procedure. Timestamps are UTC. The six CSV inputs are
treated as one immutable, versioned full snapshot; source_watermark is the
operator-supplied monotonically increasing delivery sequence.
================================================================================
*/

USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'control.pipeline_batch', N'U') IS NULL
BEGIN
    CREATE TABLE control.pipeline_batch (
        batch_id BIGINT IDENTITY(1, 1) NOT NULL
            CONSTRAINT PK_control_pipeline_batch PRIMARY KEY,
        pipeline_name NVARCHAR(128) NOT NULL,
        source_version NVARCHAR(255) NOT NULL,
        source_watermark BIGINT NOT NULL,
        source_base_path NVARCHAR(4000) NOT NULL,
        max_reject_rows BIGINT NOT NULL
            CONSTRAINT DF_control_pipeline_batch_max_rejects DEFAULT (0),
        restart_of_batch_id BIGINT NULL,
        status VARCHAR(20) NOT NULL,
        started_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_control_pipeline_batch_started DEFAULT SYSUTCDATETIME(),
        completed_at_utc DATETIME2(3) NULL,
        error_number INT NULL,
        error_severity INT NULL,
        error_state INT NULL,
        error_procedure NVARCHAR(256) NULL,
        error_line INT NULL,
        error_message NVARCHAR(4000) NULL,
        CONSTRAINT CK_control_pipeline_batch_status
            CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
        CONSTRAINT FK_control_pipeline_batch_restart
            FOREIGN KEY (restart_of_batch_id)
            REFERENCES control.pipeline_batch(batch_id)
    );
END;
GO

IF COL_LENGTH(N'control.pipeline_batch', N'max_reject_rows') IS NULL
    ALTER TABLE control.pipeline_batch ADD max_reject_rows BIGINT NOT NULL
        CONSTRAINT DF_control_pipeline_batch_max_rejects_upgrade DEFAULT (0);
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'control.pipeline_batch')
      AND name = N'UX_control_pipeline_batch_success_version'
)
BEGIN
    CREATE UNIQUE INDEX UX_control_pipeline_batch_success_version
        ON control.pipeline_batch(pipeline_name, source_version)
        WHERE status = 'SUCCEEDED';
END;
GO

IF OBJECT_ID(N'control.pipeline_step', N'U') IS NULL
BEGIN
    CREATE TABLE control.pipeline_step (
        step_id BIGINT IDENTITY(1, 1) NOT NULL
            CONSTRAINT PK_control_pipeline_step PRIMARY KEY,
        batch_id BIGINT NOT NULL,
        step_name NVARCHAR(128) NOT NULL,
        attempt_no INT NOT NULL CONSTRAINT DF_control_pipeline_step_attempt DEFAULT (1),
        status VARCHAR(20) NOT NULL,
        source_name NVARCHAR(128) NULL,
        target_name NVARCHAR(256) NULL,
        rows_read BIGINT NULL,
        rows_accepted BIGINT NULL,
        rows_rejected BIGINT NULL,
        rows_superseded BIGINT NULL,
        rows_published BIGINT NULL,
        watermark_before BIGINT NULL,
        watermark_after BIGINT NULL,
        started_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_control_pipeline_step_started DEFAULT SYSUTCDATETIME(),
        completed_at_utc DATETIME2(3) NULL,
        error_number INT NULL,
        error_severity INT NULL,
        error_state INT NULL,
        error_procedure NVARCHAR(256) NULL,
        error_line INT NULL,
        error_message NVARCHAR(4000) NULL,
        CONSTRAINT CK_control_pipeline_step_status
            CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
        CONSTRAINT FK_control_pipeline_step_batch
            FOREIGN KEY (batch_id) REFERENCES control.pipeline_batch(batch_id),
        CONSTRAINT UQ_control_pipeline_step_batch_name_attempt
            UNIQUE (batch_id, step_name, attempt_no)
    );
END;
GO

IF COL_LENGTH(N'control.pipeline_step', N'rows_superseded') IS NULL
    ALTER TABLE control.pipeline_step ADD rows_superseded BIGINT NULL;
GO

IF OBJECT_ID(N'control.load_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE control.load_watermark (
        pipeline_name NVARCHAR(128) NOT NULL,
        source_name NVARCHAR(128) NOT NULL,
        watermark_value BIGINT NOT NULL,
        source_version NVARCHAR(255) NOT NULL,
        batch_id BIGINT NOT NULL,
        updated_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_control_load_watermark_updated DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_control_load_watermark
            PRIMARY KEY (pipeline_name, source_name),
        CONSTRAINT FK_control_load_watermark_batch
            FOREIGN KEY (batch_id) REFERENCES control.pipeline_batch(batch_id)
    );
END;
GO

IF OBJECT_ID(N'control.load_reject', N'U') IS NULL
BEGIN
    CREATE TABLE control.load_reject (
        reject_id BIGINT IDENTITY(1, 1) NOT NULL
            CONSTRAINT PK_control_load_reject PRIMARY KEY,
        batch_id BIGINT NOT NULL,
        step_id BIGINT NULL,
        source_name NVARCHAR(128) NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number BIGINT NULL,
        business_key NVARCHAR(512) NULL,
        column_name NVARCHAR(128) NULL,
        rule_code NVARCHAR(128) NOT NULL,
        raw_value NVARCHAR(4000) NULL,
        raw_payload NVARCHAR(MAX) NULL,
        error_message NVARCHAR(4000) NOT NULL,
        rejected_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_control_load_reject_rejected DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_control_load_reject_batch
            FOREIGN KEY (batch_id) REFERENCES control.pipeline_batch(batch_id),
        CONSTRAINT FK_control_load_reject_step
            FOREIGN KEY (step_id) REFERENCES control.pipeline_step(step_id)
    );
END;
GO

CREATE OR ALTER PROCEDURE control.run_pipeline
    @base_path NVARCHAR(4000),
    @source_version NVARCHAR(255),
    @source_watermark BIGINT,
    @max_reject_rows BIGINT = 0,
    @restart_of_batch_id BIGINT = NULL,
    @batch_id BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @pipeline_name NVARCHAR(128) = N'sql-data-warehouse-full-snapshot';
    DECLARE @lock_result INT;
    DECLARE @lock_acquired BIT = 0;
    DECLARE @existing_batch_id BIGINT;
    DECLARE @latest_watermark BIGINT;
    DECLARE @error_number INT;
    DECLARE @error_severity INT;
    DECLARE @error_state INT;
    DECLARE @error_procedure NVARCHAR(256);
    DECLARE @error_line INT;
    DECLARE @error_message NVARCHAR(4000);
    DECLARE @stale_batches TABLE (batch_id BIGINT PRIMARY KEY);

    IF @@TRANCOUNT <> 0
        THROW 51000, 'control.run_pipeline cannot run inside a caller-owned transaction.', 1;

    SET @base_path = NULLIF(TRIM(@base_path), N'');
    SET @source_version = NULLIF(TRIM(@source_version), N'');

    IF @base_path IS NULL
        THROW 51001, 'base_path is required.', 1;
    IF @source_version IS NULL
        THROW 51002, 'source_version is required.', 1;
    IF @source_watermark IS NULL OR @source_watermark < 1
        THROW 51003, 'source_watermark must be a positive integer.', 1;
    IF @max_reject_rows IS NULL OR @max_reject_rows < 0
        THROW 51010, 'max_reject_rows must be zero or greater.', 1;
    IF NOT (
        @base_path LIKE N'[A-Za-z]:\%'
        OR @base_path LIKE N'\\%'
        OR LEFT(@base_path, 1) = N'/'
    )
        THROW 51004, 'base_path must be an absolute path visible to the SQL Server host.', 1;

    IF @restart_of_batch_id IS NOT NULL
       AND NOT EXISTS (
            SELECT 1
            FROM control.pipeline_batch
            WHERE batch_id = @restart_of_batch_id
              AND pipeline_name = @pipeline_name
              AND source_version = @source_version
              AND source_watermark = @source_watermark
              AND status = 'FAILED'
       )
        THROW 51005, 'restart_of_batch_id must reference a matching FAILED batch.', 1;

    SELECT @existing_batch_id = batch_id
    FROM control.pipeline_batch
    WHERE pipeline_name = @pipeline_name
      AND source_version = @source_version
      AND status = 'SUCCEEDED';

    IF @existing_batch_id IS NOT NULL
    BEGIN
        IF EXISTS (
            SELECT 1 FROM control.pipeline_batch
            WHERE batch_id = @existing_batch_id
              AND source_watermark <> @source_watermark
        )
            THROW 51011, 'source_version already succeeded with a different source_watermark.', 1;

        INSERT control.pipeline_batch (
            pipeline_name, source_version, source_watermark, source_base_path, max_reject_rows,
            restart_of_batch_id, status, completed_at_utc
        )
        VALUES (
            @pipeline_name, @source_version, @source_watermark, @base_path, @max_reject_rows,
            @restart_of_batch_id, 'SKIPPED', SYSUTCDATETIME()
        );
        SET @batch_id = SCOPE_IDENTITY();
        RETURN;
    END;

    IF @restart_of_batch_id IS NULL
       AND EXISTS (
            SELECT 1
            FROM control.pipeline_batch
            WHERE pipeline_name = @pipeline_name
              AND source_version = @source_version
              AND status = 'FAILED'
       )
        THROW 51006, 'A failed attempt exists for this source_version; provide restart_of_batch_id.', 1;

    INSERT control.pipeline_batch (
        pipeline_name, source_version, source_watermark, source_base_path, max_reject_rows,
        restart_of_batch_id, status
    )
    VALUES (
        @pipeline_name, @source_version, @source_watermark, @base_path, @max_reject_rows,
        @restart_of_batch_id, 'RUNNING'
    );
    SET @batch_id = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'SQL-Data-Warehouse:operational-pipeline',
            @LockMode = N'Exclusive',
            @LockOwner = N'Session',
            @LockTimeout = 0;

        IF @lock_result < 0
            THROW 51007, 'Another operational pipeline run currently owns the runtime lock.', 1;
        SET @lock_acquired = 1;

        INSERT @stale_batches(batch_id)
        SELECT batch_id
        FROM control.pipeline_batch
        WHERE pipeline_name = @pipeline_name
          AND status = 'RUNNING'
          AND batch_id <> @batch_id;

        UPDATE s
        SET status = 'FAILED',
            completed_at_utc = SYSUTCDATETIME(),
            error_number = 51008,
            error_message = N'Reconciled stale RUNNING step after exclusive runtime lock acquisition.'
        FROM control.pipeline_step s
        INNER JOIN @stale_batches stale ON stale.batch_id = s.batch_id
        WHERE s.status = 'RUNNING';

        UPDATE b
        SET status = 'FAILED',
            completed_at_utc = SYSUTCDATETIME(),
            error_number = 51008,
            error_message = N'Reconciled stale RUNNING batch after exclusive runtime lock acquisition.'
        FROM control.pipeline_batch b
        INNER JOIN @stale_batches stale ON stale.batch_id = b.batch_id;

        IF @restart_of_batch_id IS NULL
           AND EXISTS (
                SELECT 1
                FROM @stale_batches stale
                INNER JOIN control.pipeline_batch b ON b.batch_id = stale.batch_id
                WHERE b.source_version = @source_version
           )
            THROW 51012, 'A stale matching batch was reconciled; retry with restart_of_batch_id.', 1;

        SET @existing_batch_id = NULL;
        SELECT @existing_batch_id = batch_id
        FROM control.pipeline_batch
        WHERE pipeline_name = @pipeline_name
          AND source_version = @source_version
          AND status = 'SUCCEEDED';

        IF @existing_batch_id IS NOT NULL
        BEGIN
            UPDATE control.pipeline_batch
            SET status = 'SKIPPED', completed_at_utc = SYSUTCDATETIME()
            WHERE batch_id = @batch_id AND status = 'RUNNING';

            EXEC sys.sp_releaseapplock
                @Resource = N'SQL-Data-Warehouse:operational-pipeline',
                @LockOwner = N'Session';
            RETURN;
        END;

        SELECT @latest_watermark = MAX(watermark_value)
        FROM control.load_watermark
        WHERE pipeline_name = @pipeline_name;

        IF @latest_watermark IS NOT NULL AND @source_watermark <= @latest_watermark
            THROW 51009, 'A new source snapshot requires a watermark greater than the published watermark.', 1;

        EXEC bronze.load_bronze
            @batch_id = @batch_id,
            @base_path = @base_path;

        EXEC silver.load_silver
            @batch_id = @batch_id;

        BEGIN TRY
            EXEC sys.sp_releaseapplock
                @Resource = N'SQL-Data-Warehouse:operational-pipeline',
                @LockOwner = N'Session';
        END TRY
        BEGIN CATCH
            -- Batch is already committed SUCCEEDED; the session lock releases on disconnect.
        END CATCH;
        SET @lock_acquired = 0;
    END TRY
    BEGIN CATCH
        SELECT
            @error_number = ERROR_NUMBER(),
            @error_severity = ERROR_SEVERITY(),
            @error_state = ERROR_STATE(),
            @error_procedure = ERROR_PROCEDURE(),
            @error_line = ERROR_LINE(),
            @error_message = ERROR_MESSAGE();

        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

        BEGIN TRY
            UPDATE control.pipeline_step
            SET status = 'FAILED',
                completed_at_utc = SYSUTCDATETIME(),
                error_number = COALESCE(error_number, @error_number),
                error_severity = COALESCE(error_severity, @error_severity),
                error_state = COALESCE(error_state, @error_state),
                error_procedure = COALESCE(error_procedure, @error_procedure),
                error_line = COALESCE(error_line, @error_line),
                error_message = COALESCE(error_message, @error_message)
            WHERE batch_id = @batch_id AND status = 'RUNNING';

            UPDATE control.pipeline_batch
            SET status = 'FAILED',
                completed_at_utc = SYSUTCDATETIME(),
                error_number = @error_number,
                error_severity = @error_severity,
                error_state = @error_state,
                error_procedure = @error_procedure,
                error_line = @error_line,
                error_message = @error_message
            WHERE batch_id = @batch_id AND status = 'RUNNING';
        END TRY
        BEGIN CATCH
            -- Best-effort audit persistence must not mask the original error.
        END CATCH;

        IF @lock_acquired = 1
        BEGIN TRY
            EXEC sys.sp_releaseapplock
                @Resource = N'SQL-Data-Warehouse:operational-pipeline',
                @LockOwner = N'Session';
        END TRY
        BEGIN CATCH
            -- Preserve the original pipeline error.
        END CATCH;

        THROW;
    END CATCH;
END;
GO
