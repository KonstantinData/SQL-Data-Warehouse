/*
================================================================================
Idempotent Bronze table bootstrap
================================================================================
Operational DDL never drops published data. Destructive reset is isolated in
scripts/operations/reset_development.sql.
================================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID(N'bronze.crm_cust_info', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_cust_info (
        cust_id INT NULL,
        cust_key NVARCHAR(50) NULL,
        cust_firstname NVARCHAR(50) NULL,
        cust_lastname NVARCHAR(50) NULL,
        cust_marital_status NVARCHAR(50) NULL,
        cust_gender NVARCHAR(10) NULL,
        cust_create_date DATE NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_crm_cust_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.crm_cust_info', N'load_batch_id') IS NULL ALTER TABLE bronze.crm_cust_info ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_cust_info', N'source_file') IS NULL ALTER TABLE bronze.crm_cust_info ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.crm_cust_info', N'source_row_number') IS NULL ALTER TABLE bronze.crm_cust_info ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_cust_info', N'loaded_at_utc') IS NULL ALTER TABLE bronze.crm_cust_info ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_crm_cust_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO

IF OBJECT_ID(N'bronze.crm_prd_info', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_prd_info (
        prd_id INT NULL,
        prd_key NVARCHAR(50) NULL,
        prd_nm NVARCHAR(50) NULL,
        prd_cost INT NULL,
        prd_line NVARCHAR(50) NULL,
        prd_start_dt DATETIME NULL,
        prd_end_dt DATETIME NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_crm_prd_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.crm_prd_info', N'load_batch_id') IS NULL ALTER TABLE bronze.crm_prd_info ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_prd_info', N'source_file') IS NULL ALTER TABLE bronze.crm_prd_info ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.crm_prd_info', N'source_row_number') IS NULL ALTER TABLE bronze.crm_prd_info ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_prd_info', N'loaded_at_utc') IS NULL ALTER TABLE bronze.crm_prd_info ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_crm_prd_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO

IF OBJECT_ID(N'bronze.crm_sales_details', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num NVARCHAR(50) NULL,
        sls_prd_key NVARCHAR(50) NULL,
        sls_cust_id INT NULL,
        sls_order_dt INT NULL,
        sls_ship_dt INT NULL,
        sls_due_dt INT NULL,
        sls_sales INT NULL,
        sls_quantity INT NULL,
        sls_price INT NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_crm_sales_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.crm_sales_details', N'load_batch_id') IS NULL ALTER TABLE bronze.crm_sales_details ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_sales_details', N'source_file') IS NULL ALTER TABLE bronze.crm_sales_details ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.crm_sales_details', N'source_row_number') IS NULL ALTER TABLE bronze.crm_sales_details ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.crm_sales_details', N'loaded_at_utc') IS NULL ALTER TABLE bronze.crm_sales_details ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_crm_sales_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO

IF OBJECT_ID(N'bronze.erp_cust_az12', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_cust_az12 (
        cid NVARCHAR(50) NULL,
        bdate DATE NULL,
        gen NVARCHAR(10) NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_erp_cust_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.erp_cust_az12', N'load_batch_id') IS NULL ALTER TABLE bronze.erp_cust_az12 ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_cust_az12', N'source_file') IS NULL ALTER TABLE bronze.erp_cust_az12 ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.erp_cust_az12', N'source_row_number') IS NULL ALTER TABLE bronze.erp_cust_az12 ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_cust_az12', N'loaded_at_utc') IS NULL ALTER TABLE bronze.erp_cust_az12 ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_erp_cust_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO

IF OBJECT_ID(N'bronze.erp_loc_a101', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_loc_a101 (
        cid NVARCHAR(50) NULL,
        cntry NVARCHAR(50) NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_erp_loc_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.erp_loc_a101', N'load_batch_id') IS NULL ALTER TABLE bronze.erp_loc_a101 ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_loc_a101', N'source_file') IS NULL ALTER TABLE bronze.erp_loc_a101 ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.erp_loc_a101', N'source_row_number') IS NULL ALTER TABLE bronze.erp_loc_a101 ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_loc_a101', N'loaded_at_utc') IS NULL ALTER TABLE bronze.erp_loc_a101 ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_erp_loc_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO

IF OBJECT_ID(N'bronze.erp_px_cat_g1v2', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id NVARCHAR(50) NULL,
        cat NVARCHAR(50) NULL,
        subcat NVARCHAR(50) NULL,
        maintenance NVARCHAR(50) NULL,
        load_batch_id BIGINT NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number BIGINT NULL,
        loaded_at_utc DATETIME2(3) NOT NULL
            CONSTRAINT DF_bronze_erp_cat_loaded DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF COL_LENGTH(N'bronze.erp_px_cat_g1v2', N'load_batch_id') IS NULL ALTER TABLE bronze.erp_px_cat_g1v2 ADD load_batch_id BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_px_cat_g1v2', N'source_file') IS NULL ALTER TABLE bronze.erp_px_cat_g1v2 ADD source_file NVARCHAR(4000) NULL;
IF COL_LENGTH(N'bronze.erp_px_cat_g1v2', N'source_row_number') IS NULL ALTER TABLE bronze.erp_px_cat_g1v2 ADD source_row_number BIGINT NULL;
IF COL_LENGTH(N'bronze.erp_px_cat_g1v2', N'loaded_at_utc') IS NULL ALTER TABLE bronze.erp_px_cat_g1v2 ADD loaded_at_utc DATETIME2(3) NULL CONSTRAINT DF_bronze_erp_cat_loaded_upgrade DEFAULT SYSUTCDATETIME();
GO
