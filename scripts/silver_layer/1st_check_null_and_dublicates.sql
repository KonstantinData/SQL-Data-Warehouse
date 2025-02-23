/*
**Script Description:**  
==================================================================================================================================
This SQL script checks for data integrity issues in the table by identifying any `NULL` values or duplicate entries in the primary 
key column `cst_id`. The script groups records by `cst_id` and filters the results to display only those with a count greater 
than 1 (indicating duplicates) or where `cst_id` is `NULL`. If the query returns no results, it confirms that the primary key 
column has unique and non-null values, as expected.
==================================================================================================================================
*/

SELECT*
FROM bronze.crm_prd_info;
GO

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL