--NOTE--
--EXECT bronze.load_bronze
--Full load no history
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '=====================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=====================';

		PRINT '---------------------';
		PRINT 'LOADING CRM';
		PRINT '---------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>Inserting Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)	AS NVARCHAR) + 's';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT '>>Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)	AS NVARCHAR) + 's';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)	AS NVARCHAR) + 's';


		PRINT '---------------------';
		PRINT 'LOADING ERP';
		PRINT '---------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>>Inserting Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)	AS NVARCHAR) + 's';


		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>>Inserting Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>>Inserting Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\AAA\Project\Data_warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)	AS NVARCHAR) + 's';

	END TRY
	BEGIN CATCH
		PRINT	'========================================'
		PRINT	'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT	'Error Message' + ERROR_MESSAGE();
		PRINT	'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT	'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT	'========================================'
	END CATCH
END
