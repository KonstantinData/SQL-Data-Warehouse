USE DataWarehouse;
GO

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'performance.fact_sales_benchmark') AND name = N'IX_perf_order_date')
    CREATE INDEX IX_perf_order_date
        ON performance.fact_sales_benchmark (order_date_key)
        INCLUDE (customer_key, product_key, sales_amount, quantity);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'performance.fact_sales_benchmark') AND name = N'IX_perf_customer_date')
    CREATE INDEX IX_perf_customer_date
        ON performance.fact_sales_benchmark (customer_key, order_date_key)
        INCLUDE (sales_amount, quantity);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'performance.fact_sales_benchmark') AND name = N'IX_perf_product_date')
    CREATE INDEX IX_perf_product_date
        ON performance.fact_sales_benchmark (product_key, order_date_key)
        INCLUDE (sales_amount, quantity);
GO

UPDATE STATISTICS performance.fact_sales_benchmark WITH FULLSCAN;
GO
