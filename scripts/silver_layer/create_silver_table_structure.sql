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
    cust_id INT NOT NULL,
    cust_key NVARCHAR(50) NOT NULL,
    cust_firstname NVARCHAR(50),
    cust_lastname NVARCHAR(50),
    cust_marital_status NVARCHAR(50),
    cust_gender NVARCHAR(10),
    cust_create_date DATE NOT NULL,
	dwh_create_date DATETIME DEFAULT GETDATE(),
    CONSTRAINT pk_silver_crm_cust_info PRIMARY KEY (cust_id),
    CONSTRAINT uq_silver_crm_cust_info_cust_id UNIQUE (cust_id)
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U' ) IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT NOT NULL,
prd_key NVARCHAR(50) NOT NULL,
prd_nm NVARCHAR(50),
prd_cost INT NOT NULL,
prd_line NVARCHAR(50),
prd_start_dt DATETIME NOT NULL,
prd_end_dt DATETIME,
dwh_create_date DATETIME DEFAULT GETDATE(),
CONSTRAINT pk_silver_crm_prd_info PRIMARY KEY (prd_id),
CONSTRAINT uq_silver_crm_prd_info_prd_key UNIQUE (prd_key)
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U' ) IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50) NOT NULL,
sls_prd_key NVARCHAR(50) NOT NULL,
sls_cust_id INT NOT NULL,
sls_order_dt INT NOT NULL,
sls_ship_dt INT NOT NULL,
sls_due_dt INT NOT NULL,
sls_sales INT NOT NULL,
sls_quantity INT NOT NULL,
sls_price INT NOT NULL,
dwh_create_date DATETIME DEFAULT GETDATE(),
CONSTRAINT pk_silver_crm_sales_details PRIMARY KEY (sls_ord_num),
CONSTRAINT uq_silver_crm_sales_details_ord_num UNIQUE (sls_ord_num)
);
GO

IF OBJECT_ID ('silver.erp_cust_az12', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
cid NVARCHAR(50) NOT NULL,
bdate DATE NOT NULL, 
gen NVARCHAR(10),
dwh_create_date DATETIME DEFAULT GETDATE(),
CONSTRAINT pk_silver_erp_cust_az12 PRIMARY KEY (cid),
CONSTRAINT uq_silver_erp_cust_az12_cid UNIQUE (cid)
);
GO

IF OBJECT_ID ('silver.erp_loc_a101', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
cid NVARCHAR(50) NOT NULL,
cntry NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE(),
CONSTRAINT pk_silver_erp_loc_a101 PRIMARY KEY (cid),
CONSTRAINT uq_silver_erp_loc_a101_cid UNIQUE (cid)
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U' ) IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
id NVARCHAR(50) NOT NULL,
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE(),
CONSTRAINT pk_silver_erp_px_cat_g1v2 PRIMARY KEY (id),
CONSTRAINT uq_silver_erp_px_cat_g1v2_id UNIQUE (id)
);
GO
