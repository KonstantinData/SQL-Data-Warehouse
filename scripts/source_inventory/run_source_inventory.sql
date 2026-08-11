:on error exit

/* Run from the repository root in SQLCMD mode after the core Gold model exists. */
:r .\scripts\source_inventory\00_create_objects.sql
:r .\scripts\source_inventory\10_load_bronze.sql
:r .\scripts\source_inventory\20_transform_silver.sql
:r .\scripts\source_inventory\30_create_gold_views.sql

USE DataWarehouse;
GO
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;
    EXEC bronze.load_inventory_snapshot @base_path = N'$(BasePath)';
    EXEC silver.load_inventory_snapshot;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
