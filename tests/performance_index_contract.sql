USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF
(
    SELECT COUNT(*) FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'performance.fact_sales_benchmark')
      AND name IN (N'IX_perf_order_date', N'IX_perf_customer_date', N'IX_perf_product_date')
      AND is_disabled = 0 AND has_filter = 0
) <> 3
    THROW 53400, 'All three unfiltered benchmark indexes must be enabled.', 1;

DECLARE @expected TABLE (index_name SYSNAME, key_columns NVARCHAR(200), include_columns NVARCHAR(200));
INSERT @expected VALUES
    (N'IX_perf_order_date', N'order_date_key', N'customer_key,product_key,sales_amount,quantity'),
    (N'IX_perf_customer_date', N'customer_key,order_date_key', N'sales_amount,quantity'),
    (N'IX_perf_product_date', N'product_key,order_date_key', N'sales_amount,quantity');

DECLARE @actual TABLE (index_name SYSNAME, key_columns NVARCHAR(200), include_columns NVARCHAR(200));
INSERT @actual
SELECT i.name, keys.key_columns, includes.include_columns
FROM sys.indexes AS i
OUTER APPLY
(
    SELECT STRING_AGG(c.name, N',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
) AS keys
OUTER APPLY
(
    SELECT STRING_AGG(c.name, N',') WITHIN GROUP (ORDER BY ic.index_column_id) AS include_columns
    FROM sys.index_columns AS ic
    INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
) AS includes
WHERE i.object_id = OBJECT_ID(N'performance.fact_sales_benchmark')
  AND i.name IN (N'IX_perf_order_date', N'IX_perf_customer_date', N'IX_perf_product_date');

IF EXISTS
(
    SELECT index_name, key_columns, include_columns FROM @expected
    EXCEPT
    SELECT index_name, key_columns, include_columns FROM @actual
)
    THROW 53401, 'Benchmark index key/include order differs from the contract.', 1;

SELECT N'PASS' AS performance_index_contract;
GO
