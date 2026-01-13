/*
**Script Description: Silver Layer Table Management**  

This script manages the **silver layer** tables in the **DataWarehouse** by checking for existing tables, 
dropping them if they exist, and recreating them with a standardized schema.  

**Key Functions:**  
- Ensures fresh table structures by **dropping and recreating** them.  
- Stores **customer, product, sales, location, and category data** for ETL processes.  
- Includes a **`dwh_create_date` column** to track data insertion timestamps.  

This script keeps the silver layer **consistent, clean, and ready for analysis**. 🚀
*/


IF OBJECT_ID ('silver.crm_cust_info', 'U' ) IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cust_id INT,
    cust_key NVARCHAR(50),
    cust_firstname NVARCHAR(50),
    cust_lastname NVARCHAR(50),
    cust_marital_status NVARCHAR(50),
    cust_gender NVARCHAR(10),
    cust_create_date DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U' ) IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U' ) IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_cust_az12', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE, 
gen NVARCHAR(10),
dwh_create_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_loc_a101', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE()
);
GO
