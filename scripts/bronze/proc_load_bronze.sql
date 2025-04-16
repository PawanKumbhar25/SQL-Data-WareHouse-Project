/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

DROP PROCEDURE IF EXISTS bronze.load_bronze;
GO
create  procedure bronze.load_bronze as 
begin
	DECLARE @batch_start_time DATETIME;
	DECLARE @batch_end_time DATETIME;
	DECLARE @start_time DATETIME;
	DECLARE @end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE()
			print '============================================================';
			print 'Loading Bronze Layer';
			print '============================================================';

			print '------------------------------------------------------------';
			print 'Loading CRM Tables';
			print '------------------------------------------------------------';


			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.crm_cust_info';
			truncate table bronze.crm_cust_info;
			print '>> Inserting Data Into: bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info 
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW=2,
				FIELDTERMINATOR =',',
				TABLOCK
			);
			SET @end_time= GETDATE()
			--NANOSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR, WEEK, QUARTER
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM bronze.crm_cust_info;
			SELECT count(*) FROM bronze.crm_cust_info;



			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.crm_prd_info';
			truncate table bronze.crm_prd_info;
			print '>> Inserting Data Into: bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_crm\prd_info.csv'
			WITH(
				FIRSTROW=2,
				FIELDTERMINATOR =',',
				TABLOCK
			);
			SET @end_time= GETDATE()
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM  bronze.crm_prd_info;
			SELECT COUNT(*) FROM  bronze.crm_prd_info;

		
			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
			print '>> Inserting Data Into: bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_crm\sales_details.csv'
			WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK	
			);
			SET @end_time= GETDATE()
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM bronze.crm_sales_details;
			SELECT COUNT(*) FROM bronze.crm_sales_details;

	
			print '------------------------------------------------------------';
			print 'Loading ERP Tables';
			print '------------------------------------------------------------';

		
			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12
			print '>> Inserting Data Into: bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_erp\CUST_AZ12.CSV'
			WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK	
			);
			SET @end_time= GETDATE()
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM bronze.erp_cust_az12;
			SELECT COUNT(*) FROM bronze.erp_cust_az12;

		
			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101
			print '>> Inserting Data Into: bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_erp\LOC_A101.CSV'
			WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK	
			);
			SET @end_time= GETDATE()
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM bronze.erp_loc_a101;
			SELECT COUNT(*) FROM bronze.erp_loc_a101;

		
			SET @start_time = GETDATE()
			print '>> Truncating Table: bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'E:\Data Visualization Data With Baraa\Data WareHouse Project\sql-data-warehouse-project - Copy\datasets\source_erp\PX_CAT_G1V2.CSV'
			WITH(
				FIRSTROW=2,
				FIELDTERMINATOR=',',
				TABLOCK	
			);
			SET @end_time= GETDATE()
			PRINT '>> Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @start_time, @end_time)  AS NVARCHAR)+ ' MilliSeconds';
			PRINT '-------------------';
			SELECT * FROM bronze.erp_px_cat_g1v2;
			SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
	
		SET @batch_end_time= GETDATE()
		print '================================================';
		print 'Loading Bronze Layer is Compleated!!!';
		PRINT '>> Total Load Duration: '+ CAST( DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time)  AS NVARCHAR)+ 'MilliSeconds';
		PRINT '-------------------';
		print '================================================';
	END TRY

	BEGIN CATCH

		PRINT '====================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message'+ERROR_MESSAGE();
		PRINT 'Error Message'+CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '====================================================';

	END CATCH

END;

GO

EXECUTE bronze.load_bronze;
