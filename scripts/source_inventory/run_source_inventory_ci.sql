:on error exit

/* Container-oriented entrypoint for the repository CI topology. */
:r /workspace/scripts/source_inventory/00_create_objects.sql
:r /workspace/scripts/source_inventory/10_load_bronze.sql
:r /workspace/scripts/source_inventory/20_transform_silver.sql
:r /workspace/scripts/source_inventory/30_create_gold_views.sql

USE DataWarehouse;
GO
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;
    EXEC bronze.load_inventory_snapshot @base_path = N'/datasets';
    EXEC silver.load_inventory_snapshot;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
