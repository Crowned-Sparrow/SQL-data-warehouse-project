CREATE VIEW gold.dim_products AS
SELECT
	pn.pdr_id			AS	product_id,
	pn.pdr_key			AS	product_number,
	pn.prd_nm			AS	product_name,		

	pn.cat_id			AS	category_id,
	pc.cat				AS	category,
	pc.subcat			AS	subcategory,
	pc.maintenance		,

	pn.prd_cost			AS	cost,
	pn.prd_line			AS	product_line,
	pn.prd_start_date	AS	start_date
	
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_date IS NULL -- Only current products
