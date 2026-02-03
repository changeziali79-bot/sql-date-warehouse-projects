/*
======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================================
Script purpose :
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Turncates the bronze tables before loading data.
  - Uses 'BULK INSERT' command to load data from csv Files ti bronze tables. 

Parameters:
  NONE.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
==========================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY 
		PRINT'===================================';
		PRINT'Loading Bronze Layers';
		PRINT'===================================';
	
		PRINT'-----------------------------------';
		PRINT 'Loading CRM TABLES'
		PRINT'-----------------------------------';

		SET @start_time =GETDATE();
		PRINT'>> Truncating Table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT'>> Inserting data into bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'

		SET @start_time =GETDATE();
		PRINT'>> Truncating Table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT'>> Inserting data into bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);	 
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'


		SET @start_time=GETDATE()
			PRINT'>> Truncating Table:bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details

			PRINT'>> Inserting Data into bronze.crm_sales_details'

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'

	
		PRINT'-----------------------------------';
		PRINT 'Loading ERP TABLES'
		PRINT'-----------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table:bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT'>> Inserting Data into bronze.erp_cust_az12'

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'

		SET @start_time = GETDATE();
			PRINT'>> Truncating Table:bronze.erp_Loc_a101';
			TRUNCATE TABLE bronze.erp_Loc_a101

			PRINT'>> Inserting Data into bronze.erp_Loc_a101'

		BULK INSERT bronze.erp_Loc_a101
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\Loc_A101.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'

		SET @start_time = GETDATE()

				PRINT'>> Truncating Table:bronze.erp_px_cat_g1v2';
				TRUNCATE TABLE bronze.erp_px_cat_g1v2

				PRINT'>> Inserting Data into bronze.erp_Loc_a101'

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\chang\OneDrive\Desktop\WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH ( 
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
			);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>...................'

		SET @batch_end_time = GETDATE();
		PRINT '================================'
		PRINT'Loading Bronze layer is completed'
		PRINT' Total load duration: ' + Cast(Datediff(Second,@batch_start_time , @batch_end_time) AS NVARCHAR) + ' Seconds'
		PRINT '================================'


		END TRY
		BEGIN CATCH
			PRINT '====================================='
			PRINT 'ERROR OCCARED DURING THE BRONZE LAYER'
			PRINT 'Error Message' + Error_message ();
			PRINT 'Error Number' + CAST( Error_number() AS NVARCHAR);
			PRINT 'Error Line' +CAST( Error_line() AS NVARCHAR);
			PRINT 'Error state' +CAST( Error_state() AS NVARCHAR);
		END CATCH

END
