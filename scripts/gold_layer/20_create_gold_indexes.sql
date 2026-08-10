USE DataWarehouse;
GO

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'gold.dim_products') AND name = N'IX_dim_products_asof_lookup')
    CREATE INDEX IX_dim_products_asof_lookup
        ON gold.dim_products (product_number, effective_from, effective_to)
        INCLUDE (product_key);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'IX_fact_sales_order_date')
    CREATE INDEX IX_fact_sales_order_date
        ON gold.fact_sales (order_date_key)
        INCLUDE (customer_key, product_key, sales_amount, quantity);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'IX_fact_sales_customer_date')
    CREATE INDEX IX_fact_sales_customer_date
        ON gold.fact_sales (customer_key, order_date_key)
        INCLUDE (sales_amount, quantity);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'IX_fact_sales_product_date')
    CREATE INDEX IX_fact_sales_product_date
        ON gold.fact_sales (product_key, order_date_key)
        INCLUDE (sales_amount, quantity);
GO
