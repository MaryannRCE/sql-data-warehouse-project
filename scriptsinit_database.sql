/*
===============================
Create Database and Schemas
===============================

Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exixts.
	If the database exists, it is dropped and recreated.
  Additionally, the script sets up three schemas within the database: 'bronze', 'silver', 'gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure
  you have proper backups before running this script.
*/

-- To allow creation of new database:
USE master;
GO

-- Best practice: Drop and recreate the database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create your new database:
CREATE DATABASE DataWarehouse;

-- Switch to  your database:
USE DataWarehouse;

-- Create your new schema:
CREATE SCHEMA bronze;

-- Refresh 'Object Explorer', check under Security>Schemas

-- Create other schema:
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Refresh 'Object Explorer', check under Security>Schemas
-- To execute multiple CREATE SCHEMA at once, insert 'GO':
	-- CREATE SCHEMA bronze;
	-- GO
	-- CREATE SCHEMA silver;
	-- GO
	-- CREATE SCHEMA gold;
	-- GO


