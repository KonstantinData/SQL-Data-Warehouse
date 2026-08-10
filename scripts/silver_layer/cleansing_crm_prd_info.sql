/* Legacy compatibility shim.
   Silver cleansing is now one atomic six-table operation implemented by
   scripts/silver_layer/load_silver.sql and invoked by the operational runner. */
USE DataWarehouse;
GO
PRINT 'Legacy per-table product cleansing is disabled; use silver.load_silver.';
GO
