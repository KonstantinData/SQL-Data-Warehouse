/*
**Script Description:**  
==================================================================================================================================
This SQL script verifies the effectiveness of the data cleansing process in the `silver.crm_cst_info` table by checking for 
any remaining `NULL` values or duplicate entries in the primary key column `cst_id`. It also focuses on identifying and 
validating data consistency for a specific `cst_id` to ensure proper deduplication.

### Steps:

1. **Checking Data Integrity:**
   - The script first groups records by `cst_id` and filters results to detect duplicates (`COUNT(*) > 1`) or `NULL` values.
   - If no results are returned, the primary key column is confirmed as unique and non-null.

2. **Investigating a Specific `cst_id`:**
   - The script retrieves all records where `cst_id = 29466`, identified from the previous integrity check.

3. **Ranking Duplicate Entries:**
   - The `ROW_NUMBER()` window function assigns a ranking (`flag_last`) to each record with the same `cst_id`, ordered by 
     `cst_create_date` in descending order. This step helps in identifying the latest record.

4. **Filtering for the Latest Records Only:**
   - The final query selects only the latest entry for each `cst_id`, ensuring that only the most recent data is retained 
     by filtering `flag_last = 1`. This step removes outdated duplicates while keeping the most relevant record.
==================================================================================================================================
*/


SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cst_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO


SELECT 
*
FROM silver.crm_cst_info
WHERE cst_id = 29466; -- Is picked from the check_null_duplicates script
GO


SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last -- With the Window function we rank the Rows
FROM silver.crm_cst_info
WHERE cst_id = 29466;
GO


SELECT 
*
FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM silver.crm_cst_info
	WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1; -- With flag_ast = 1 we ensure that only the latest data is shown
GO

