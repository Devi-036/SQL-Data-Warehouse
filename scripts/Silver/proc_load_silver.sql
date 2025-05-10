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

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '==================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '==================================================================';

		PRINT '------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------------------';

		--Loading silver.crm_cst_info
		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.crm_cst_info';
		TRUNCATE TABLE silver.crm_cst_info;
		print '>> Inserting Data Into : silver.crm_cst_info';
		INSERT INTO silver.crm_cst_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gender,
		cst_create_date)

		SELECT 
		cst_id,
		cst_key,
		TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME,
		TRIM(CST_LASTNAME) AS CST_LASTNAME,
		CASE WHEN UPPER(TRIM(CST_MATERIAL_STATUS)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(CST_MATERIAL_STATUS)) = 'M' THEN 'Married'
			 ELSE 'N/A'  --Normalize material status to readable formate
		END CST_MATERIAL_STATUS,
		CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'FEMALE'
			 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'MALE'
			 ELSE 'N/A'   --Normalize gender to readable formate
		END CST_GENDER,
		CST_CREATE_DATE
		FROM (
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG_LAST
		FROM bronze.crm_cst_info
		WHERE CST_ID IS NOT NULL)T  --select the most recent record per customer
		WHERE FLAG_LAST = 1;
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

		--Loading Silver.crm_prd_info
		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting Data Into : silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,   --Extract catergory ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,			 --Extract Product ID
		prd_nm,
		ISNULL(PRD_COST, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,  --Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(PRD_START_DT) OVER(PARTITION BY PRD_KEY ORDER BY PRD_START_DT)-1 AS DATE) AS prd_end_dt   --Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

		--Loading silver.crm_sales_details
		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting Data Into : silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,  --Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
			 ELSE sls_price
		END AS sls_price   --Drive price if original values is invalid
		FROM bronze.crm_sales_details
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';


		--Loading silver.erp_loc_a101
		PRINT '------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------------------';

		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting Data Into : silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid, cntry)
		SELECT 
		REPLACE (CID, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry   --normalize and handle missing or blank country codes
		FROM bronze.erp_loc_a101
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';


		--loading silver.erp_cust_az12
		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting Data Into : silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
		SELECT
		CASE WHEN cid like 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid)) 
			 ELSE cid
		END cid,    --remove NAS prefix is present
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END bdate,  --set future set birthdates to null
		CASE WHEN UPPER(TRIM(GEN)) IN ('F' , 'Female') THEN 'Female'
			 WHEN UPPER(TRIM(GEN)) IN ('M' , 'Male') THEN 'Male'
			 ELSE 'n/a'
		END AS GEN  --normalize gender values and handles unknow cases
		FROM bronze.erp_cust_az12
		SET @end_time=GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------------------';

		--loading silver.erp_px_cat_g1v2
		SET @start_time=GETDATE();
		print '>> Truncating Data Into : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into : silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintance)
		SELECT 
		id,
		cat,
		subcat,
		maintance
		from bronze.erp_px_cat_g1v2
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

--Execution
EXEC Silver.load_silver;
