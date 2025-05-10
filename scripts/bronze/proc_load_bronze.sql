/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '==================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==================================================================';

		PRINT '------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.crm_cst_info' 
		TRUNCATE TABLE bronze.crm_cst_info;

		PRINT '>>> Inserting Data Into :bronze.crm_cst_info' 
		BULK INSERT bronze.crm_cst_info
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';


		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>> Inserting Data Into :bronze.crm_prd_info' 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>> Inserting Data Into :bronze.crm_sales_details' 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

	
		PRINT '------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------------------';


		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>> Inserting Data Into :bronze.erp_cust_az12' 
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';


		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>> Inserting Data Into :bronze.erp_loc_a101' 
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';


		SET @start_time=GETDATE();
		PRINT '>>> Truncating table :bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>> Inserting Data Into :bronze.erp_px_cat_g1v2' 
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Devi\OneDrive\Desktop\SQL Datawerehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

		SET @batch_end_time=GETDATE();
		PRINT '=========================================================================';
		PRINT 'Loading Bronze Layer Is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

		END TRY
		BEGIN CATCH
			PRINT '======================================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error message' + ERROR_MESSAGE();
			PRINT 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '======================================================================';
		END CATCH
END


--execution
EXEC bronze.load_bronze;
