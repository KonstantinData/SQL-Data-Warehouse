/* Define the fail-closed full-refresh loader for the inventory CSV. */
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_inventory_snapshot
    @base_path NVARCHAR(4000) = N'datasets'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @path_separator NCHAR(1);
    DECLARE @source_path NVARCHAR(4000);
    DECLARE @escaped_source_path NVARCHAR(MAX);
    DECLARE @bulk_sql NVARCHAR(MAX);
    DECLARE @load_batch_id UNIQUEIDENTIFIER = NEWID();

    IF @base_path IS NULL OR LEN(TRIM(@base_path)) = 0
        THROW 51010, 'Inventory base path must not be empty.', 1;

    IF @base_path LIKE '%' + NCHAR(10) + '%'
       OR @base_path LIKE '%' + NCHAR(13) + '%'
        THROW 51011, 'Inventory base path contains an invalid control character.', 1;

    SET @base_path = TRIM(@base_path);
    SET @path_separator = CASE WHEN CHARINDEX(N'/', @base_path) > 0 THEN N'/' ELSE N'\' END;
    IF RIGHT(@base_path, 1) NOT IN (N'/', N'\')
        SET @base_path += @path_separator;

    SET @source_path = @base_path + N'source_inventory' + @path_separator + N'inventory_snapshots.csv';
    SET @escaped_source_path = REPLACE(@source_path, N'''', N'''''' );

    BEGIN TRY
        BEGIN TRANSACTION;

        TRUNCATE TABLE bronze.inventory_snapshot_stage;

        SET @bulk_sql = N'BULK INSERT bronze.inventory_snapshot_stage
FROM ''' + @escaped_source_path + N'''
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '','',
    ROWTERMINATOR = ''0x0a'',
    TABLOCK
);';
        EXEC sys.sp_executesql @bulk_sql;

        IF NOT EXISTS (SELECT 1 FROM bronze.inventory_snapshot_stage)
            THROW 51012, 'Inventory source file loaded zero data rows.', 1;

        TRUNCATE TABLE bronze.inventory_snapshot_raw;

        INSERT INTO bronze.inventory_snapshot_raw (
            source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
            product_id, product_number, on_hand_qty, reserved_qty, reorder_point_qty,
            unit_cost, currency_code, extracted_at_utc, load_batch_id, source_file_name
        )
        SELECT
            source_row_id, source_system, snapshot_date, warehouse_code, warehouse_name,
            product_id, product_number, on_hand_qty, reserved_qty, reorder_point_qty,
            unit_cost, currency_code, REPLACE(extracted_at_utc, CHAR(13), N''),
            @load_batch_id, N'inventory_snapshots.csv'
        FROM bronze.inventory_snapshot_stage;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
