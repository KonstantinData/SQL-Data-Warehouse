/*
  Create isolated objects for the synthetic inventory snapshot source.
  Prerequisite: DataWarehouse and the bronze, silver, and gold schemas exist.
*/
USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;

IF SCHEMA_ID('bronze') IS NULL OR SCHEMA_ID('silver') IS NULL OR SCHEMA_ID('gold') IS NULL
    THROW 51000, 'source_inventory requires the bronze, silver, and gold schemas.', 1;

IF OBJECT_ID('bronze.inventory_snapshot_stage') IS NOT NULL
   AND OBJECT_ID('bronze.inventory_snapshot_stage', 'U') IS NULL
    THROW 51001, 'bronze.inventory_snapshot_stage exists with an incompatible object type.', 1;

IF OBJECT_ID('bronze.inventory_snapshot_stage', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.inventory_snapshot_stage (
        source_row_id NVARCHAR(200) NULL,
        source_system NVARCHAR(200) NULL,
        snapshot_date NVARCHAR(200) NULL,
        warehouse_code NVARCHAR(200) NULL,
        warehouse_name NVARCHAR(200) NULL,
        product_id NVARCHAR(200) NULL,
        product_number NVARCHAR(200) NULL,
        on_hand_qty NVARCHAR(200) NULL,
        reserved_qty NVARCHAR(200) NULL,
        reorder_point_qty NVARCHAR(200) NULL,
        unit_cost NVARCHAR(200) NULL,
        currency_code NVARCHAR(200) NULL,
        extracted_at_utc NVARCHAR(200) NULL
    );
END;

IF OBJECT_ID('bronze.inventory_snapshot_raw') IS NOT NULL
   AND OBJECT_ID('bronze.inventory_snapshot_raw', 'U') IS NULL
    THROW 51002, 'bronze.inventory_snapshot_raw exists with an incompatible object type.', 1;

IF OBJECT_ID('bronze.inventory_snapshot_raw', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.inventory_snapshot_raw (
        bronze_row_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_inventory_snapshot_raw PRIMARY KEY,
        source_row_id NVARCHAR(200) NULL,
        source_system NVARCHAR(200) NULL,
        snapshot_date NVARCHAR(200) NULL,
        warehouse_code NVARCHAR(200) NULL,
        warehouse_name NVARCHAR(200) NULL,
        product_id NVARCHAR(200) NULL,
        product_number NVARCHAR(200) NULL,
        on_hand_qty NVARCHAR(200) NULL,
        reserved_qty NVARCHAR(200) NULL,
        reorder_point_qty NVARCHAR(200) NULL,
        unit_cost NVARCHAR(200) NULL,
        currency_code NVARCHAR(200) NULL,
        extracted_at_utc NVARCHAR(200) NULL,
        load_batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file_name NVARCHAR(260) NOT NULL,
        dwh_loaded_at_utc DATETIME2(3) NOT NULL CONSTRAINT df_bronze_inventory_loaded_at DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('silver.inventory_warehouse_map') IS NOT NULL
   AND OBJECT_ID('silver.inventory_warehouse_map', 'U') IS NULL
    THROW 51003, 'silver.inventory_warehouse_map exists with an incompatible object type.', 1;

IF OBJECT_ID('silver.inventory_warehouse_map', 'U') IS NULL
BEGIN
    CREATE TABLE silver.inventory_warehouse_map (
        warehouse_code NVARCHAR(30) NOT NULL CONSTRAINT pk_inventory_warehouse_map PRIMARY KEY,
        warehouse_name NVARCHAR(100) NOT NULL,
        country_code CHAR(2) NOT NULL,
        is_active BIT NOT NULL CONSTRAINT df_inventory_warehouse_active DEFAULT (1)
    );
END;

UPDATE silver.inventory_warehouse_map
SET warehouse_name = source.warehouse_name,
    country_code = source.country_code,
    is_active = 1
FROM (VALUES
    (N'WH-BER-01', N'Berlin Demo Warehouse', 'DE'),
    (N'WH-HAM-01', N'Hamburg Demo Warehouse', 'DE')
) AS source(warehouse_code, warehouse_name, country_code)
WHERE silver.inventory_warehouse_map.warehouse_code = source.warehouse_code;

INSERT INTO silver.inventory_warehouse_map (warehouse_code, warehouse_name, country_code, is_active)
SELECT source.warehouse_code, source.warehouse_name, source.country_code, 1
FROM (VALUES
    (N'WH-BER-01', N'Berlin Demo Warehouse', 'DE'),
    (N'WH-HAM-01', N'Hamburg Demo Warehouse', 'DE')
) AS source(warehouse_code, warehouse_name, country_code)
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.inventory_warehouse_map target
    WHERE target.warehouse_code = source.warehouse_code
);

IF OBJECT_ID('silver.inventory_snapshot') IS NOT NULL
   AND OBJECT_ID('silver.inventory_snapshot', 'U') IS NULL
    THROW 51004, 'silver.inventory_snapshot exists with an incompatible object type.', 1;

IF OBJECT_ID('silver.inventory_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE silver.inventory_snapshot (
        source_row_id NVARCHAR(30) NOT NULL,
        source_system NVARCHAR(30) NOT NULL,
        snapshot_date DATE NOT NULL,
        warehouse_code NVARCHAR(30) NOT NULL,
        warehouse_name NVARCHAR(100) NOT NULL,
        product_id INT NOT NULL,
        product_number NVARCHAR(50) NOT NULL,
        on_hand_qty INT NOT NULL,
        reserved_qty INT NOT NULL,
        available_qty AS (on_hand_qty - reserved_qty) PERSISTED,
        reorder_point_qty INT NOT NULL,
        unit_cost DECIMAL(18,2) NOT NULL,
        inventory_value AS (CONVERT(DECIMAL(19,2), on_hand_qty * unit_cost)) PERSISTED,
        currency_code CHAR(3) NOT NULL,
        stock_status NVARCHAR(20) NOT NULL,
        extracted_at_utc DATETIME2(0) NOT NULL,
        load_batch_id UNIQUEIDENTIFIER NOT NULL,
        dwh_created_at_utc DATETIME2(3) NOT NULL CONSTRAINT df_silver_inventory_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_silver_inventory_snapshot PRIMARY KEY (snapshot_date, warehouse_code, product_id),
        CONSTRAINT uq_silver_inventory_source_row UNIQUE (source_row_id),
        CONSTRAINT ck_silver_inventory_product_id CHECK (product_id > 0),
        CONSTRAINT ck_silver_inventory_on_hand CHECK (on_hand_qty >= 0),
        CONSTRAINT ck_silver_inventory_reserved CHECK (reserved_qty >= 0 AND reserved_qty <= on_hand_qty),
        CONSTRAINT ck_silver_inventory_reorder CHECK (reorder_point_qty >= 0),
        CONSTRAINT ck_silver_inventory_cost CHECK (unit_cost >= 0),
        CONSTRAINT ck_silver_inventory_currency CHECK (currency_code = 'EUR'),
        CONSTRAINT ck_silver_inventory_status CHECK (stock_status IN (N'AVAILABLE', N'LOW_STOCK', N'OUT_OF_STOCK'))
    );
END;

IF OBJECT_ID('silver.inventory_snapshot_reject') IS NOT NULL
   AND OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NULL
    THROW 51005, 'silver.inventory_snapshot_reject exists with an incompatible object type.', 1;

IF OBJECT_ID('silver.inventory_snapshot_reject', 'U') IS NULL
BEGIN
    CREATE TABLE silver.inventory_snapshot_reject (
        bronze_row_id BIGINT NOT NULL,
        source_row_id NVARCHAR(200) NULL,
        reason_code NVARCHAR(60) NOT NULL,
        source_system NVARCHAR(200) NULL,
        snapshot_date NVARCHAR(200) NULL,
        warehouse_code NVARCHAR(200) NULL,
        product_id NVARCHAR(200) NULL,
        product_number NVARCHAR(200) NULL,
        on_hand_qty NVARCHAR(200) NULL,
        reserved_qty NVARCHAR(200) NULL,
        load_batch_id UNIQUEIDENTIFIER NOT NULL,
        rejected_at_utc DATETIME2(3) NOT NULL CONSTRAINT df_inventory_rejected_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_inventory_snapshot_reject PRIMARY KEY (bronze_row_id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'silver.inventory_snapshot') AND name = N'ck_silver_inventory_product_id')
    ALTER TABLE silver.inventory_snapshot WITH CHECK ADD CONSTRAINT ck_silver_inventory_product_id CHECK (product_id > 0);
ALTER TABLE silver.inventory_snapshot WITH CHECK CHECK CONSTRAINT ck_silver_inventory_product_id;
GO
