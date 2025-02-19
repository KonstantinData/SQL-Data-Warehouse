/*
==============================================================================================================
Create Database and Schemas
==============================================================================================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped an recreated. Additionally, the script isets up three schemas
  within the database: 'bronze','silver','gold'.

Warning:
  Running this scrit will drop the entire 'DataWarehouse' database if it exists. All data in the database will 
  be permanently deleted. Proceed with caution an enszure you have propper backups before running this script.
*/

USE master;
GO

-- Überprüfen, ob die Datenbank existiert und löschen
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Neue Datenbank erstellen
CREATE DATABASE DataWarehouse;
GO

-- Sicherstellen, dass die Datenbank korrekt verwendet wird
USE DataWarehouse;
GO

-- Schemas erstellen
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

