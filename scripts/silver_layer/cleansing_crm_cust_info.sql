/*
**Script Overview:**
======================================================================================================================
This SQL script updates the `silver.crm_cust_info` table by adding an `is_future` column and inserting the latest 
cleaned customer records from `bronze.crm_cust_info`. It ensures data consistency by:  

- Selecting the most recent record per customer (`cust_id`) using `ROW_NUMBER()`.  
- Cleaning names and standardizing marital status and gender values.  
- Flagging future-dated records in the `is_future` column.  

This script is part of an ETL process, ensuring accurate and up-to-date customer data for analytics and reporting. 🚀
=======================================================================================================================
*/

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' 
    AND TABLE_NAME = 'crm_cust_info' 
    AND COLUMN_NAME = 'cust_is_future'
)
BEGIN
    ALTER TABLE silver.crm_cust_info 
    ADD cust_is_future BIT NOT NULL DEFAULT 0;
END
GO

INSERT INTO DataWarehouse.silver.crm_cust_info(
	cust_id,
	cust_key,
	cust_firstname,
	cust_lastname,
	cust_marital_status,
	cust_gender,
	cust_create_date,
	cust_is_future
	)

SELECT
	cust_id,
	cust_key,
	TRIM(cust_firstname) AS cust_firstname,
	TRIM(cust_lastname) AS cust_lastname,
	CASE 
		WHEN UPPER(TRIM(cust_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cust_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cust_marital_status,
	CASE 
		WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cust_gender,
	cust_create_date,
	CASE 
		WHEN cust_create_date IS NULL THEN 0
		WHEN cust_create_date > GETDATE() THEN 1
		ELSE 0
	END AS cust_is_future
FROM(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cust_id IS NOT NULL
	) latest_record
WHERE flag_last = 1;

