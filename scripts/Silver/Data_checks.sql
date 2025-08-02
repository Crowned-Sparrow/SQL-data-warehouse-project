---ERP---
SELECT *
FROM (
    SELECT 
        id,
		cat,
        subcat,
	CASE
		WHEN LEN(TRIM(subcat)) - LEN(REPLACE(TRIM(subcat), ' ', '')) + 1 = 2 THEN 
        (
            SELECT 
                LEFT(value, 1)
            FROM STRING_SPLIT(subcat, ' ')
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(50)')
		WHEN LEN(TRIM(subcat)) - LEN(REPLACE(TRIM(subcat), ' ', '')) + 1 = 1 THEN UPPER(LEFT(subcat,2))
		WHEN LEN(TRIM(subcat)) - LEN(REPLACE(TRIM(subcat), ' ', '')) + 1 = 3 THEN 
		(
            SELECT 
                LEFT(value, 1)
            FROM STRING_SPLIT(REPLACE(subcat,'and',''),' ')
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(50)')
		ELSE subcat
		END AS shorted_subcat,
		UPPER(LEFT(cat,2)) AS shorted_cat
    FROM bronze.erp_px_cat_g1v2
) AS derived
WHERE LEFT(id,2) != shorted_cat OR RIGHT(id,2) !=shorted_subcat

SELECT --Seem fine naming rule with 1 word subcat is little confusing, however there is no eror in datas
	*
FROM bronze.erp_px_cat_g1v2
WHERE subcat ='Bib-Shorts' 
	OR subcat ='Cranksets'
	OR subcat ='Handlebars'
	OR subcat ='Headsets'
	OR subcat ='Pedals'
