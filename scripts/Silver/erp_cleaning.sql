-- Customer
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
INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
SELECT 
cid,
CASE  
	WHEN UPPER(TRIM(cntry)) = 'US'	THEN 'United States'
	WHEN UPPER(TRIM(cntry)) = 'USA'	THEN 'United States'
	WHEN UPPER(TRIM(cntry)) = 'DE'	THEN 'Germany'
	WHEN cntry IS NULL				THEN 'n/a'
	WHEN UPPER(TRIM(cntry)) =''		THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101
