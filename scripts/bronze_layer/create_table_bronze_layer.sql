/*
============================================================================================================
DDL Script: Create Bronze Tables
============================================================================================================
Script Purpose:
	This script creates tables in the 'bronze' schema, removing existing ones if they already exist.
	Execute this script to redefine the DDL (Data Definition Language) structure of the 'bronze' tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;
============================================================================================================
*/




IF OBJECT_ID ('bronze.crm_cust_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cust_id INT NOT NULL,
    cust_key NVARCHAR(50) NOT NULL,
    cust_firstname NVARCHAR(50),
    cust_lastname NVARCHAR(50),
    cust_marital_status NVARCHAR(50),
    cust_gender NVARCHAR(10),
    cust_create_date DATE NOT NULL,
    CONSTRAINT pk_bronze_crm_cust_info PRIMARY KEY (cust_id) WITH (IGNORE_DUP_KEY = ON)
);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT NOT NULL,
prd_key NVARCHAR(50) NOT NULL,
prd_nm NVARCHAR(50),
prd_cost INT NOT NULL,
prd_line NVARCHAR(50),
prd_start_dt DATETIME NOT NULL,
prd_end_dt DATETIME,
CONSTRAINT pk_bronze_crm_prd_info PRIMARY KEY (prd_id)
);
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50) NOT NULL,
sls_prd_key NVARCHAR(50) NOT NULL,
sls_cust_id INT NOT NULL,
sls_order_dt INT NOT NULL,
sls_ship_dt INT NOT NULL,
sls_due_dt INT NOT NULL,
sls_sales INT NOT NULL,
sls_quantity INT NOT NULL,
sls_price INT NOT NULL,
CONSTRAINT pk_bronze_crm_sales_details PRIMARY KEY (sls_ord_num)
);
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50) NOT NULL,
bdate DATE NOT NULL, 
gen NVARCHAR(10),
CONSTRAINT pk_bronze_erp_cust_az12 PRIMARY KEY (cid)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50) NOT NULL,
cntry NVARCHAR(50),
CONSTRAINT pk_bronze_erp_loc_a101 PRIMARY KEY (cid)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50) NOT NULL,
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
CONSTRAINT pk_bronze_erp_px_cat_g1v2 PRIMARY KEY (id)
);
GO
