/*
**Script Description:**  
==================================================================================================================================
This SQL script checks for data integrity issues in the `bronze.crm_cst_info` table by identifying any `NULL` values or duplicate 
entries in the primary key column `cst_id`. The script groups records by `cst_id` and filters the results to display only those 
with a count greater than 1 (indicating duplicates) or where `cst_id` is `NULL`. If the query returns no results, it confirms that 
the primary key column has unique and non-null values, as expected.
==================================================================================================================================
*/

SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cst_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO


/*
**Script Description:**  
====================================================================================================================================
This SQL script focuses on identifying and resolving duplicate entries for a specific `cst_id` in the `bronze.crm_cst_info` table.  

1. **Initial Investigation:**  
   - The first query retrieves all records where `cst_id = 29466`, which was identified from the previous null/duplicate check.  

2. **Ranking Duplicates:**  
   - The second query applies the `ROW_NUMBER()` window function to assign a ranking (`flag_last`) to each record with the same `cst_id`, 
   ordered by `cst_create_date` in descending order. This helps in identifying the latest record.  

3. **Filtering for Latest Records:**  
   - The final query selects only the latest entry for each `cst_id`, ensuring that only the most recent data is retained by 
   filtering `flag_last = 1`. This step removes older duplicates while keeping the most relevant record.
   ====================================================================================================================================
*/

-- 1st we focusing on the issue and take 
SELECT 
*
FROM bronze.crm_cst_info
WHERE cst_id = 29466; -- Is picked from the check_null_duplicates script
GO

SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last -- With the Window function we rank the Rows
FROM bronze.crm_cst_info
WHERE cst_id = 29466;
GO

SELECT 
*
FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cst_info
	WHERE cst_id IS NOT NULL
) t WHERE flag_last = 1; -- With flag_ast = 1 we ensure that only the latest data is shown
GO

