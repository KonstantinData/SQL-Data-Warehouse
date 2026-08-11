/*
================================================================================
Idempotent Silver table bootstrap
================================================================================
Preserves the source-facing columns consumed by the physical Gold model. Operational metadata
is nullable so the isolated legacy CI fixture loader remains insert-compatible.
================================================================================
*/

USE DataWarehouse;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'silver.crm_cust_info', N'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_cust_info (
        cust_id INT NULL,
        cust_key NVARCHAR(50) NULL,
        cust_firstname NVARCHAR(50) NULL,
        cust_lastname NVARCHAR(50) NULL,
        cust_marital_status NVARCHAR(50) NULL,
        cust_gender NVARCHAR(10) NULL,
        cust_create_date DATE NULL,
        cust_is_future BIT NOT NULL CONSTRAINT DF_silver_cust_future DEFAULT (0),
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_cust_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.crm_cust_info', N'cust_is_future') IS NULL ALTER TABLE silver.crm_cust_info ADD cust_is_future BIT NOT NULL CONSTRAINT DF_silver_cust_future_upgrade DEFAULT (0);
IF COL_LENGTH(N'silver.crm_cust_info', N'dwh_batch_id') IS NULL ALTER TABLE silver.crm_cust_info ADD dwh_batch_id BIGINT NULL;
GO

IF OBJECT_ID(N'silver.crm_prd_info', N'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_prd_info (
        prd_id INT NULL,
        prd_key NVARCHAR(50) NULL,
        prd_nm NVARCHAR(50) NULL,
        prd_cost INT NULL,
        prd_line NVARCHAR(50) NULL,
        prd_start_dt DATETIME NULL,
        prd_end_dt DATETIME NULL,
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_prd_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.crm_prd_info', N'dwh_batch_id') IS NULL ALTER TABLE silver.crm_prd_info ADD dwh_batch_id BIGINT NULL;
GO

IF OBJECT_ID(N'silver.crm_sales_details', N'U') IS NULL
BEGIN
    CREATE TABLE silver.crm_sales_details (
        sls_ord_num NVARCHAR(50) NULL,
        sls_prd_key NVARCHAR(50) NULL,
        sls_cust_id INT NULL,
        sls_order_dt INT NULL,
        sls_ship_dt INT NULL,
        sls_due_dt INT NULL,
        sls_sales DECIMAL(18, 2) NULL,
        sls_quantity INT NULL,
        sls_price DECIMAL(18, 2) NULL,
        order_date DATE NULL,
        ship_date DATE NULL,
        due_date DATE NULL,
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_sales_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.crm_sales_details', N'order_date') IS NULL ALTER TABLE silver.crm_sales_details ADD order_date DATE NULL;
IF COL_LENGTH(N'silver.crm_sales_details', N'ship_date') IS NULL ALTER TABLE silver.crm_sales_details ADD ship_date DATE NULL;
IF COL_LENGTH(N'silver.crm_sales_details', N'due_date') IS NULL ALTER TABLE silver.crm_sales_details ADD due_date DATE NULL;
IF COL_LENGTH(N'silver.crm_sales_details', N'dwh_batch_id') IS NULL ALTER TABLE silver.crm_sales_details ADD dwh_batch_id BIGINT NULL;
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'silver.crm_sales_details') AND name = N'sls_sales'
      AND (system_type_id <> TYPE_ID(N'decimal') OR precision <> 18 OR scale <> 2)
)
    ALTER TABLE silver.crm_sales_details ALTER COLUMN sls_sales DECIMAL(18, 2) NULL;
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'silver.crm_sales_details') AND name = N'sls_price'
      AND (system_type_id <> TYPE_ID(N'decimal') OR precision <> 18 OR scale <> 2)
)
    ALTER TABLE silver.crm_sales_details ALTER COLUMN sls_price DECIMAL(18, 2) NULL;
GO

IF OBJECT_ID(N'silver.erp_cust_az12', N'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_cust_az12 (
        cid NVARCHAR(50) NULL,
        bdate DATE NULL,
        gen NVARCHAR(10) NULL,
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_erp_cust_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.erp_cust_az12', N'dwh_batch_id') IS NULL ALTER TABLE silver.erp_cust_az12 ADD dwh_batch_id BIGINT NULL;
GO

IF OBJECT_ID(N'silver.erp_loc_a101', N'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_loc_a101 (
        cid NVARCHAR(50) NULL,
        cntry NVARCHAR(50) NULL,
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_erp_loc_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.erp_loc_a101', N'dwh_batch_id') IS NULL ALTER TABLE silver.erp_loc_a101 ADD dwh_batch_id BIGINT NULL;
GO

IF OBJECT_ID(N'silver.erp_px_cat_g1v2', N'U') IS NULL
BEGIN
    CREATE TABLE silver.erp_px_cat_g1v2 (
        id NVARCHAR(50) NULL,
        cat NVARCHAR(50) NULL,
        subcat NVARCHAR(50) NULL,
        maintenance NVARCHAR(50) NULL,
        dwh_create_date DATETIME NOT NULL CONSTRAINT DF_silver_erp_cat_created DEFAULT GETDATE(),
        dwh_batch_id BIGINT NULL
    );
END;
GO
IF COL_LENGTH(N'silver.erp_px_cat_g1v2', N'dwh_batch_id') IS NULL ALTER TABLE silver.erp_px_cat_g1v2 ADD dwh_batch_id BIGINT NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'silver.crm_cust_info') AND name = N'UX_silver_cust_operational')
    CREATE UNIQUE INDEX UX_silver_cust_operational ON silver.crm_cust_info(dwh_batch_id, cust_id) WHERE dwh_batch_id IS NOT NULL;
IF EXISTS (
    SELECT 1
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(N'silver.crm_prd_info')
      AND i.name = N'UX_silver_prd_operational'
      AND NOT EXISTS (
          SELECT 1
          FROM sys.index_columns ic
          JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
          WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
            AND ic.key_ordinal = 2 AND c.name = N'prd_id'
      )
)
    DROP INDEX UX_silver_prd_operational ON silver.crm_prd_info;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'silver.crm_prd_info') AND name = N'UX_silver_prd_operational')
    CREATE UNIQUE INDEX UX_silver_prd_operational ON silver.crm_prd_info(dwh_batch_id, prd_id) WHERE dwh_batch_id IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'silver.crm_sales_details') AND name = N'UX_silver_sales_operational')
    CREATE UNIQUE INDEX UX_silver_sales_operational ON silver.crm_sales_details(dwh_batch_id, sls_ord_num, sls_prd_key) WHERE dwh_batch_id IS NOT NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'silver.crm_cust_info') AND name = N'CK_silver_cust_positive_id')
    ALTER TABLE silver.crm_cust_info WITH CHECK ADD CONSTRAINT CK_silver_cust_positive_id CHECK (cust_id IS NULL OR cust_id > 0);
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'silver.crm_prd_info') AND name = N'CK_silver_prd_positive_id')
    ALTER TABLE silver.crm_prd_info WITH CHECK ADD CONSTRAINT CK_silver_prd_positive_id CHECK (prd_id IS NULL OR prd_id > 0);
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'silver.crm_sales_details') AND name = N'CK_silver_sales_positive_customer_id')
    ALTER TABLE silver.crm_sales_details WITH CHECK ADD CONSTRAINT CK_silver_sales_positive_customer_id CHECK (sls_cust_id IS NULL OR sls_cust_id > 0);
ALTER TABLE silver.crm_cust_info WITH CHECK CHECK CONSTRAINT CK_silver_cust_positive_id;
ALTER TABLE silver.crm_prd_info WITH CHECK CHECK CONSTRAINT CK_silver_prd_positive_id;
ALTER TABLE silver.crm_sales_details WITH CHECK CHECK CONSTRAINT CK_silver_sales_positive_customer_id;
GO
