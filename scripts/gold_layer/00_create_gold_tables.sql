USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'gold') IS NULL
    EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo;');
GO

/* Remove the dependency chain from the legacy view model in safe order. */
IF OBJECT_ID(N'gold.fact_sales', N'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
IF OBJECT_ID(N'gold.dim_products', N'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
IF OBJECT_ID(N'gold.dim_customers', N'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

IF OBJECT_ID(N'gold.dim_customers', N'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_customers
    (
        customer_key        INT IDENTITY(1, 1) NOT NULL,
        customer_id         INT NOT NULL,
        customer_number     NVARCHAR(50) NOT NULL,
        first_name          NVARCHAR(50) NULL,
        last_name_hash      CHAR(64) NULL,
        marital_status      NVARCHAR(50) NULL,
        gender              NVARCHAR(20) NULL,
        birth_date          DATE NULL,
        country             NVARCHAR(50) NULL,
        customer_create_date DATE NULL,
        customer_is_future  BIT NOT NULL CONSTRAINT DF_dim_customers_future DEFAULT (0),
        dwh_updated_at      DATETIME2(0) NOT NULL CONSTRAINT DF_dim_customers_updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_dim_customers PRIMARY KEY CLUSTERED (customer_key),
        CONSTRAINT UQ_dim_customers_customer_id UNIQUE (customer_id),
        CONSTRAINT CK_dim_customers_reserved_member CHECK
        (
            (customer_key = 0 AND customer_id = -1 AND customer_number = N'UNKNOWN')
            OR (customer_key > 0 AND customer_id > 0)
        )
    );
END;
GO

IF OBJECT_ID(N'gold.dim_products', N'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_products
    (
        product_key         INT IDENTITY(1, 1) NOT NULL,
        product_id          INT NOT NULL,
        product_number      NVARCHAR(50) NOT NULL,
        product_name        NVARCHAR(100) NULL,
        product_cost        DECIMAL(18, 2) NULL,
        product_line        NVARCHAR(50) NULL,
        category            NVARCHAR(50) NULL,
        subcategory         NVARCHAR(50) NULL,
        maintenance         NVARCHAR(50) NULL,
        effective_from      DATE NOT NULL,
        effective_to        DATE NULL,
        source_end_date     DATE NULL,
        is_current          BIT NOT NULL,
        dwh_updated_at      DATETIME2(0) NOT NULL CONSTRAINT DF_dim_products_updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_dim_products PRIMARY KEY CLUSTERED (product_key),
        CONSTRAINT UQ_dim_products_product_id UNIQUE (product_id),
        CONSTRAINT UQ_dim_products_product_number_effective_from UNIQUE (product_number, effective_from),
        CONSTRAINT CK_dim_products_effective_range CHECK (effective_to IS NULL OR effective_to > effective_from),
        CONSTRAINT CK_dim_products_current_interval CHECK
        (
            (is_current = 1 AND effective_to IS NULL)
            OR (is_current = 0 AND effective_to IS NOT NULL)
        ),
        CONSTRAINT CK_dim_products_reserved_member CHECK
        (
            (product_key = 0 AND product_id = -1 AND product_number = N'UNKNOWN')
            OR (product_key > 0 AND product_id > 0)
        )
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_customers') AND name = N'CK_dim_customers_reserved_member')
    ALTER TABLE gold.dim_customers WITH CHECK ADD CONSTRAINT CK_dim_customers_reserved_member CHECK
    (
        (customer_key = 0 AND customer_id = -1 AND customer_number = N'UNKNOWN')
        OR (customer_key > 0 AND customer_id > 0)
    );
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_products') AND name = N'CK_dim_products_reserved_member')
    ALTER TABLE gold.dim_products WITH CHECK ADD CONSTRAINT CK_dim_products_reserved_member CHECK
    (
        (product_key = 0 AND product_id = -1 AND product_number = N'UNKNOWN')
        OR (product_key > 0 AND product_id > 0)
    );
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_products') AND name = N'CK_dim_products_current_interval')
    ALTER TABLE gold.dim_products WITH CHECK ADD CONSTRAINT CK_dim_products_current_interval CHECK
    (
        (is_current = 1 AND effective_to IS NULL)
        OR (is_current = 0 AND effective_to IS NOT NULL)
    );
ALTER TABLE gold.dim_customers WITH CHECK CHECK CONSTRAINT CK_dim_customers_reserved_member;
ALTER TABLE gold.dim_products WITH CHECK CHECK CONSTRAINT CK_dim_products_reserved_member;
ALTER TABLE gold.dim_products WITH CHECK CHECK CONSTRAINT CK_dim_products_current_interval;
GO

IF OBJECT_ID(N'gold.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE gold.dim_date
    (
        date_key            INT NOT NULL,
        calendar_date       DATE NULL,
        calendar_year       SMALLINT NULL,
        calendar_quarter    TINYINT NULL,
        calendar_month      TINYINT NULL,
        month_name          NVARCHAR(20) NULL,
        day_of_month        TINYINT NULL,
        day_of_week_iso     TINYINT NULL,
        day_name            NVARCHAR(20) NULL,
        iso_week            TINYINT NULL,
        is_weekend          BIT NULL,
        month_start_date    DATE NULL,
        CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_key),
        CONSTRAINT UQ_dim_date_calendar_date UNIQUE (calendar_date),
        CONSTRAINT CK_dim_date_key CHECK
        (
            (date_key = 0 AND calendar_date IS NULL)
            OR
            (
                date_key <> 0
                AND calendar_date IS NOT NULL
                AND date_key = CONVERT(INT, CONVERT(CHAR(8), calendar_date, 112))
            )
        )
    );
END;
GO

IF OBJECT_ID(N'gold.fact_sales', N'U') IS NULL
BEGIN
    CREATE TABLE gold.fact_sales
    (
        sales_key           BIGINT IDENTITY(1, 1) NOT NULL,
        order_number        NVARCHAR(50) NOT NULL,
        sales_order_line_number INT NOT NULL,
        product_number      NVARCHAR(50) NOT NULL,
        customer_key        INT NOT NULL,
        product_key         INT NOT NULL,
        order_date_key      INT NOT NULL,
        ship_date_key       INT NOT NULL,
        due_date_key        INT NOT NULL,
        order_date          DATE NULL,
        ship_date           DATE NULL,
        due_date            DATE NULL,
        sales_amount        DECIMAL(18, 2) NULL,
        quantity            INT NULL,
        price               DECIMAL(18, 2) NULL,
        measure_quality_code NVARCHAR(32) NOT NULL,
        dwh_updated_at      DATETIME2(0) NOT NULL CONSTRAINT DF_fact_sales_updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_fact_sales PRIMARY KEY CLUSTERED (sales_key),
        CONSTRAINT UQ_fact_sales_order_line UNIQUE (order_number, sales_order_line_number),
        CONSTRAINT UQ_fact_sales_source_line UNIQUE (order_number, product_number),
        CONSTRAINT FK_fact_sales_dim_customers FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key),
        CONSTRAINT FK_fact_sales_dim_products FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key),
        CONSTRAINT FK_fact_sales_order_date FOREIGN KEY (order_date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_sales_ship_date FOREIGN KEY (ship_date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT FK_fact_sales_due_date FOREIGN KEY (due_date_key) REFERENCES gold.dim_date(date_key),
        CONSTRAINT CK_fact_sales_line_number CHECK (sales_order_line_number > 0),
        CONSTRAINT CK_fact_sales_measure_quality CHECK
        (
            measure_quality_code IN (N'VALID', N'SOURCE_ADJUSTMENT', N'INVALID_SOURCE_MEASURE')
        )
    );
END;
GO

/* Unknown members preserve source rows that cannot be resolved safely. */
IF NOT EXISTS (SELECT 1 FROM gold.dim_customers WHERE customer_key = 0)
BEGIN
    SET IDENTITY_INSERT gold.dim_customers ON;
    INSERT gold.dim_customers
    (
        customer_key, customer_id, customer_number, first_name, last_name_hash,
        marital_status, gender, birth_date, country, customer_create_date,
        customer_is_future
    )
    VALUES (0, -1, N'UNKNOWN', N'Unknown', NULL, N'Unknown', N'Unknown', NULL, N'Unknown', NULL, 0);
    SET IDENTITY_INSERT gold.dim_customers OFF;
END;
GO

IF NOT EXISTS (SELECT 1 FROM gold.dim_products WHERE product_key = 0)
BEGIN
    SET IDENTITY_INSERT gold.dim_products ON;
    INSERT gold.dim_products
    (
        product_key, product_id, product_number, product_name, product_cost,
        product_line, category, subcategory, maintenance, effective_from,
        effective_to, source_end_date, is_current
    )
    VALUES
    (
        0, -1, N'UNKNOWN', N'Unknown', 0, N'Unknown', N'Unknown', N'Unknown',
        N'Unknown', CONVERT(DATE, '19000101', 112), NULL, NULL, 1
    );
    SET IDENTITY_INSERT gold.dim_products OFF;
END;
GO

IF NOT EXISTS (SELECT 1 FROM gold.dim_date WHERE date_key = 0)
BEGIN
    INSERT gold.dim_date (date_key, calendar_date)
    VALUES (0, NULL);
END;
GO
