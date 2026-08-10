/*
================================================================================
Safe database bootstrap
================================================================================
Creates the DataWarehouse database and required schemas when they do not exist.
This script is intentionally non-destructive and is safe to run repeatedly.

For a disposable development reset, use:
  scripts/operations/reset_development.sql
================================================================================
*/

USE master;
GO

IF DB_ID(N'DataWarehouse') IS NULL
BEGIN
    CREATE DATABASE DataWarehouse;
END;
GO

USE DataWarehouse;
GO

IF SCHEMA_ID(N'bronze') IS NULL EXEC(N'CREATE SCHEMA bronze AUTHORIZATION dbo;');
IF SCHEMA_ID(N'silver') IS NULL EXEC(N'CREATE SCHEMA silver AUTHORIZATION dbo;');
IF SCHEMA_ID(N'gold') IS NULL EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo;');
IF SCHEMA_ID(N'control') IS NULL EXEC(N'CREATE SCHEMA control AUTHORIZATION dbo;');
GO

:r scripts/control/create_runtime_control.sql
