:ON ERROR EXIT

/*
================================================================================
DESTRUCTIVE DEVELOPMENT RESET ONLY
================================================================================
Drops the entire DataWarehouse database. This is never an operational restart
or recovery action. The exact SQLCMD confirmation token is mandatory.
================================================================================
*/

USE master;
GO

IF N'$(ConfirmReset)' <> N'RESET_DATAWAREHOUSE_FOR_DEVELOPMENT'
BEGIN
    THROW 51300, 'Development reset refused: exact ConfirmReset token is required.', 1;
END;
GO

IF DB_ID(N'DataWarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

PRINT 'Development DataWarehouse reset completed.';
GO
