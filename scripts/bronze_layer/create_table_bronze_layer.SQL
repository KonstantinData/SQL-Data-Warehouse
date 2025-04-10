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




IF OBJECT_ID ('bronze.crm_cst_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_cst_info;
CREATE TABLE bronze.crm_cst_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gender NVARCHAR(10),
    cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
GO

IF OBJECT_ID ('bronze.erp_cst_az12', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_cst_az12;
CREATE TABLE bronze.erp_cst_az12(
cid NVARCHAR(50),
bdate DATE, 
gen NVARCHAR(10)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);
GO
