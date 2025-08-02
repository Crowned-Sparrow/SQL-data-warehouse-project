--NOTE--
--EXEC silver.load_silver
--Full load, no history
--Drop table and data existed
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
		-- Customer_info
	PRINT('>> Truncating table silver.crm_cust_info')
	TRUNCATE TABLE silver.crm_cust_info
	PRINT('>> Inserting table silver.crm_cust_info')
	IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
		DROP TABLE silver.crm_cust_info;
	CREATE TABLE silver.crm_cust_info (
		cst_id				INT,
		cst_key				NVARCHAR(50),
		cst_firstname		NVARCHAR(50),
		cst_lastname		NVARCHAR (50),
		cst_material_status NVARCHAR(50),
		cst_gndr			NVARCHAR(50),
		cst_create_date		DATE,
		dwh_create_date		DATETIME2 DEFAULT GETDATE()
	);
	INSERT INTO silver.crm_cust_info(
		cst_id					,
		cst_key					,
		cst_firstname			,
		cst_lastname			,
		cst_gndr				,
		cst_material_status 	,
		cst_create_date
	)
	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
	END cst_gndr,

	CASE	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
	END cst_material_status,
	cst_create_date
	FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	)t WHERE flag_last =1 AND cst_id IS NOT NULL -- Danh dau duplicate (moi nhat la 1) chi lay 

  
	  --Product_info
	PRINT('>> Truncating table silver.crm_prd_info')
	TRUNCATE TABLE silver.crm_prd_info
	PRINT('>> Inserting table silver.crm_prd_info')
	IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
		DROP TABLE silver.crm_prd_info;
	CREATE TABLE silver.crm_prd_info (
		pdr_id			INT,
		cat_id			NVARCHAR(50),
		pdr_key			NVARCHAR(50),
		prd_nm			NVARCHAR(50),
		prd_cost		INT,
		prd_line		NVARCHAR(50),
		prd_start_date	DATE,
		prd_end_date	DATE,
		dwh_create_date		DATETIME2 DEFAULT GETDATE()
	);

	INSERT INTO silver.crm_prd_info (
		pdr_id			,
		cat_id			,
		pdr_key			,
		prd_nm			,
		prd_cost		,
		prd_line		,
		prd_start_date	,
		prd_end_date	
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_' ) AS cat_id,
	SUBSTRING(prd_key, 7 ,LEN(prd_key)) AS prd_key,
	TRIM(prd_nm) AS prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE	TRIM(UPPER(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_date AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) -1 AS DATE) AS prd_end_dt
	FROM	bronze.crm_prd_info

	--Sales_info
	PRINT('>> Truncating table silver.crm_sales_details')
	TRUNCATE TABLE silver.crm_sales_details
	PRINT('>> Inserting table silver.crm_sales_details')
	IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
		DROP TABLE silver.crm_sales_details;
	CREATE TABLE silver.crm_sales_details (
		sls_ord_num		NVARCHAR(50),
		sls_prd_key		NVARCHAR(50),	
		sls_cust_id		INT,	
		sls_order_dt	DATE,
		sls_ship_dt		DATE,
		sls_due_dt		DATE,
		sls_sales		INT,
		sls_quantity	INT,
		sls_price		INT,
		dwh_create_date		DATETIME2 DEFAULT GETDATE()
	);
	INSERT INTO silver.crm_sales_details(
		sls_ord_num		,
		sls_prd_key		,	
		sls_cust_id		,	
		sls_order_dt	,
		sls_ship_dt		,
		sls_due_dt		,
		sls_sales		,
		sls_quantity	,
		sls_price		
	)
	SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity*sls_price)
		THEN ABS(sls_quantity*sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE 
		WHEN sls_quantity IS NULL OR sls_quantity < 0
		THEN sls_sales / NULLIF(sls_price,0)
		ELSE sls_quantity	
	END AS sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price	
	END AS sls_price
		FROM bronze.crm_sales_details

	-- Customer
	PRINT('>> Truncating table silver.erp_cust_az12')
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT('>> Inserting table silver.erp_cust_az12')
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT
	CASE 
		WHEN cid LIKE 'NA%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
	END AS cid,
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(gen)) ='M' THEN 'Male'
		WHEN UPPER(TRIM(gen)) ='F' THEN 'Female'
		WHEN UPPER(TRIM(gen)) != 'Male' AND UPPER(TRIM(gen)) != 'Female' THEN NULL
		ELSE gen
	END AS gen
	FROM bronze.erp_cust_az12
	-- Loc
	PRINT('>> Truncating table silver.erp_loc_a101')
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT('>> Inserting table silver.erp_loc_a101')
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT 
	REPLACE(cid,'-','') AS cid,
	CASE  
		WHEN UPPER(TRIM(cntry)) = 'US'	THEN 'United States'
		WHEN UPPER(TRIM(cntry)) = 'USA'	THEN 'United States'
		WHEN UPPER(TRIM(cntry)) = 'DE'	THEN 'Germany'
		WHEN cntry IS NULL				THEN 'n/a'
		WHEN UPPER(TRIM(cntry)) =''		THEN 'n/a'
		ELSE cntry
	END AS cntry
	FROM bronze.erp_loc_a101

	--Categories
	PRINT('>> Truncating table silver.erp_px_cat_g1v2')
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT('>> Inserting table silver.erp_px_cat_g1v2')
	INSERT INTO silver.erp_px_cat_g1v2
	(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		*
	FROM
		bronze.erp_px_cat_g1v2
END
