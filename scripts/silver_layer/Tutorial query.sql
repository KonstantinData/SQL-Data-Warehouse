/*
**Script Description:**  
=======================================================================================================================================================
This SQL script inserts cleaned and deduplicated customer information from the `bronze.crm_cst_info` table into the `silver.crm_cst_info` table.  

1. **Data Selection and Cleaning:**  
	-	`TRIM()` is applied to `cst_firstname` and `cst_lastname` to remove leading and trailing spaces.  
	-	The `cst_material_status` column is standardized by converting single-letter codes (`S`, `M`) into full descriptions (`Single`, `Married`). 
		If the value does not match, it is set to `'n/a'`.  
	-	The `cst_gender` column is also standardized by converting `F` to `Female` and `M` to `Male`, with unmatched values set to `'n/a'`.  

2. **Handling Duplicates:**  
	-	The inner query assigns a row number (`flag_last`) using the `ROW_NUMBER()` function, partitioned by `cst_id` and ordered by `cst_create_date` 
		in descending order.  
	-	Only the most recent record (`flag_last = 1`) for each `cst_id` is selected to ensure that duplicate or outdated entries are not inserted into 
		the `silver` layer.  

3. **Future Date Handling:**  
	-	The `is_future` column is derived using a `CASE` statement:  
		-	If `cst_create_date` is `NULL`, it is assigned `0` (not in the future).  
		-	If `cst_create_date` is greater than the current date (`GETDATE()`), it is assigned `1` (future record).  
		-	Otherwise, it is assigned `0`.  

4. **Schema Adjustments:**  
	-	The script ensures that the `is_future` column exists in the `silver.crm_cst_info` table before inserting data.  
	-	If necessary, the column is added as a `BIT` field (`0` for past/present, `1` for future records).  

This process ensures data consistency, integrity, and standardization before moving data to the `silver.crm_cst_info` table.
=======================================================================================================================================================
*/

ALTER TABLE silver.crm_cst_info 
ADD is_future BIT NOT NULL DEFAULT 0;


INSERT INTO silver.crm_cst_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gender,
	cst_create_date,
	is_future
	);

SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_material_status,
	CASE 
		WHEN cst_gender = 'M' THEN 'Male'
		WHEN cst_gender = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gender,
	CASE 
        WHEN cst_create_date IS NULL THEN 0 
        WHEN cst_create_date > GETDATE() THEN 1 
        ELSE 0 
    END AS is_future
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cst_info
	WHERE cst_id IS NOT NULL
	) latest_record
WHERE flag_last = 1;

