/*
**Script Description:**  
=======================================================================================================================================================
This SQL script inserts cleaned and deduplicated customer information from the `bronze.crm_cst_info` table into the `silver.crm_cust_info` table.  

1. **Data Selection and Cleaning:**  
	-	`TRIM()` is applied to `cst_firstname` and `cst_lastname` to remove leading and trailing spaces.  
	-	The `cst_marital_status` column is standardized by converting single-letter codes (`S`, `M`) into full descriptions (`Single`, `Married`). 
		If the value does not match, it is set to `'n/a'`.  
	-	The `cst_gender` column is also standardized by converting `F` to `Female` and `M` to `Male`, with unmatched values set to `'n/a'`.  

2. **Handling Duplicates:**  
	-	The inner query assigns a row number (`flag_last`) using the `ROW_NUMBER()` function, partitioned by `cst_id` and ordered by `cst_create_date` 
		in descending order.  
	-	Only the most recent record (`flag_last = 1`) for each `cst_id` is selected to ensure that duplicate or outdated entries are not inserted into 
		the `silver` layer.  

This process ensures data consistency, integrity, and standardization before moving data to the `silver.crm_cust_info` table.
=======================================================================================================================================================
*/


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
	TRIM(cst_firstname) AS cst_fistname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE	
		WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END	cst_gender,
	CASE	
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END	cst_gender,
	cst_create_date
FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cst_info
	WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1; 
