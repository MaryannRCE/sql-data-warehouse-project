/*
===========================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===========================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
===========================================================================================================
*/
-- BULK INSERT
-- Best practice: TRUNCATE TABLE to reset it to empty state, to avoid duplicates
-- Best practice: Use STORED PROCEDURES
-- Best practice: Add TRY-CATCH for error handling, data integrity and issue logging for easier debugging
-- Track ETL Duration using DECLARE, SET, CAST, DATEDIFF

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN -- beginning of Stored Procedure
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
		-- declare variables
	BEGIN TRY -- beginning of TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===========================================================';

		-- ================
		-- CRM TABLES
		-- ================
		PRINT '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -';
		PRINT 'Loading CRM Tables';
		PRINT '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -';

		SET @start_time = GETDATE(); -- to get the datetime of start loading
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- to skip the row headers
			FIELDTERMINATOR = ',', -- specify the delimiter
			TABLOCK
		);
		SET @end_time = GETDATE(); -- to get the datetime of finish loading
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.' -- calculate and display the ETL duration
		PRINT '>> ---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd.info';
		TRUNCATE TABLE [bronze].[crm_prd_info];

		PRINT '>> Inserting Data Into: bronze.crm_prd.info';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_Details';
		TRUNCATE TABLE [bronze].[crm_sales_Details];

		PRINT '>> Inserting Data Into: bronze.crm_sales_Details';
		BULK INSERT [bronze].[crm_sales_Details]
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ---------------'

		-- ================
		-- ERP TABLES
		-- ================
		PRINT '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -';
		PRINT 'Loading ERP Tables';
		PRINT '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_aZ12';
		TRUNCATE TABLE [bronze].[erp_cust_aZ12];

		PRINT '>> Inserting Data Into: bronze.erp_cust_aZ12';
		BULK INSERT [bronze].[erp_cust_aZ12]
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_erp\cust_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101];

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ---------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\SQL practice\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
		PRINT '>> ---------------'

		SET @batch_end_time = GETDATE();
		PRINT '============================================'
		PRINT 'BRONZE LAYER LOAD COMPLETE.'
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.'
		PRINT '============================================'
	END TRY -- End of TRY
	BEGIN CATCH -- Beginning of CATCH
		PRINT '============================================';
		PRINT 'Error occured during loading Bronze Layer';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================';
	END CATCH -- End of CATCH
END -- End of Stored Procedure
