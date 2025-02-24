/*
**Script Overview:**
======================================================================================================================
This SQL script updates the `silver.crm_cst_info` table by adding an `is_future` column and inserting the latest 
cleaned customer records from `bronze.crm_cst_info`. It ensures data consistency by:  

- Selecting the most recent record per customer (`cst_id`) using `ROW_NUMBER()`.  
- Cleaning names and standardizing marital status and gender values.  
- Flagging future-dated records in the `is_future` column.  

This script is part of an ETL process, ensuring accurate and up-to-date customer data for analytics and reporting. 🚀
=======================================================================================================================
*/

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' 
    AND TABLE_NAME = 'crm_cst_info' 
    AND COLUMN_NAME = 'cst_is_future'
)
BEGIN
    ALTER TABLE silver.crm_cst_info 
    ADD cst_is_future BIT NOT NULL DEFAULT 0;
END
GO

INSERT INTO DataWarehouse.silver.crm_cst_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gender,
	cst_create_date,
	cst_is_future
	)

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
		WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gender,
	cst_create_date,
	CASE 
		WHEN cst_create_date IS NULL THEN 0
		WHEN cst_create_date > GETDATE() THEN 1
		ELSE 0
	END AS cst_is_future
FROM(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cst_info
	WHERE cst_id IS NOT NULL
	) latest_record
WHERE flag_last = 1;

