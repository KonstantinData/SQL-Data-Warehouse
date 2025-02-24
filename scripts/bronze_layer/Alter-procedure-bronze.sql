/*
**Script Description:**  
===============================================================================================================================================================================
The `bronze.load_bronze` stored procedure is responsible for loading data into the Bronze Layer of a data warehouse. It performs the following key operations:

1. **Batch Processing Initialization**  
   - Captures the start time of the batch process.

2. **CRM Data Loading**  
   - Truncates and reloads customer (`crm_cst_info`), product (`crm_prd_info`), and sales details (`crm_sales_details`) tables from CSV files located in a specified directory.
   - Uses `BULK INSERT` to efficiently load data.
   - Logs the duration of each table load.

3. **ERP Data Loading**  
   - Truncates and reloads ERP-related tables (`erp_cst_az12`, `erp_loc_a101`, and `erp_px_cat_g1v2`).
   - Similar `BULK INSERT` operations ensure efficient data ingestion.
   - Logs the duration for each ERP table load.

4. **Error Handling**  
   - If any error occurs, the procedure captures and prints the error message, number, and state.

5. **Completion Logging**  
   - Records and prints the total duration of the batch processing.

This procedure is designed for efficient and structured ingestion of CRM and ERP datasets into the Bronze Layer while ensuring error logging and execution tracking.
===============================================================================================================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();

	BEGIN TRY	
		PRINT '=======================================================================================';
		PRINT 'Loading Bronze Layer'
		PRINT '=======================================================================================';

		PRINT '=======================================================================================';
		PRINT 'Loading CRM Tables'
		PRINT '=======================================================================================';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cst_info';
		TRUNCATE TABLE bronze.crm_cst_info;
	
		PRINT '>>Inserting Data into: bronze.crm_cst_info';
		BULK INSERT bronze.crm_cst_info
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_crm\cst_info.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT '>>Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_crm\prd_info.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		PRINT '>>Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_crm\sales_details.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 

		PRINT '=======================================================================================';
		PRINT 'Loading ERP Tables'
		PRINT '=======================================================================================';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cst_az12';
		TRUNCATE TABLE bronze.erp_cst_az12;

		PRINT '>>Inserting Data into: bronze.erp_cst_az12';
		BULK INSERT bronze.erp_cst_az12
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_erp\CST_AZ12.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_erp\LOC_A101.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM "D:\Repositories\Git_GitHub\SQL-Data-Warehouse\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 

		SET @batch_end_time = GETDATE();
		PRINT '=======================================================================================';
		PRINT 'Loading Bronze Layer is completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'; 
		PRINT '=======================================================================================';

	END TRY
	BEGIN CATCH
		PRINT '=======================================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_Number() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================================================';


	END CATCH
END
