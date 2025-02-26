USE DataWarehouse;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_prd_info'
AND TABLE_SCHEMA = 'bronze'

SELECT
	prd_id,
	COUNT(*) AS count_prd_key
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL -- Checked no duplicates

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key, -- prd_key is now a double? ow to handle
	prd_nm,
	ISNULL(prd_cost,0) as prod_cost,
	CASE UPPER(TRIM(prd_line)) -- ONLY WITH VALUES!!! Not with complex conditions
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;
-- CHECK if all cat_id´s are available in bronze.erp_px_cat_g1v2
-- WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN
-- (SELECT distinct id from bronze.erp_px_cat_g1v2) -- cat_id CO_PE is not available in bronze.erp_px_cat_g1v2 (How to handle) 

--Check if all prd_key´s are available in bronze.crm_sales-details
--WHERE SUBSTRING(prd_key, 7, len(prd_key)) IN ( -- with NOT IN we checkefd first the entrance question
--SELECT sls_prd_key FROM bronze.crm_sales_details)-- Theire are a lott of prd_key´s not aveilable

-- SELECT sls_prd_key FROM bronze.crm_sales_details WHERE sls_prd_key LIKE 'FR-%' --theese are prd_key´s which have no orders.
