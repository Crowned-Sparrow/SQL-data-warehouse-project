-- Customer_info
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



