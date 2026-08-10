SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

IF NOT EXISTS (SELECT 1 FROM silver.crm_cust_info)
    THROW 51000, 'Cannot create Silver negative fixture because crm_cust_info is empty.', 1;

INSERT INTO silver.crm_cust_info (
    cust_id, cust_key, cust_firstname, cust_lastname,
    cust_marital_status, cust_gender, cust_create_date,
    dwh_create_date, cust_is_future
)
SELECT TOP (1)
    cust_id, cust_key, cust_firstname, cust_lastname,
    cust_marital_status, cust_gender, cust_create_date,
    dwh_create_date, cust_is_future
FROM silver.crm_cust_info
ORDER BY cust_id;
