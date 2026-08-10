USE DataWarehouse;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'gold.dim_customers', N'U') IS NULL
   OR OBJECT_ID(N'gold.dim_products', N'U') IS NULL
   OR OBJECT_ID(N'gold.dim_date', N'U') IS NULL
   OR OBJECT_ID(N'gold.fact_sales', N'U') IS NULL
    THROW 53000, 'Gold model contract requires four physical tables.', 1;

IF COLUMNPROPERTY(OBJECT_ID(N'gold.dim_customers'), N'customer_key', 'IsIdentity') <> 1
   OR COLUMNPROPERTY(OBJECT_ID(N'gold.dim_products'), N'product_key', 'IsIdentity') <> 1
   OR COLUMNPROPERTY(OBJECT_ID(N'gold.fact_sales'), N'sales_key', 'IsIdentity') <> 1
    THROW 53001, 'Warehouse surrogate keys must be IDENTITY columns.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_customers') AND name = N'PK_dim_customers')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_products') AND name = N'PK_dim_products')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_date') AND name = N'PK_dim_date')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'PK_fact_sales')
    THROW 53002, 'A required primary key is missing.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_customers') AND name = N'UQ_dim_customers_customer_id')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_products') AND name = N'UQ_dim_products_product_id')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.dim_products') AND name = N'UQ_dim_products_product_number_effective_from')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'UQ_fact_sales_order_line')
   OR NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'gold.fact_sales') AND name = N'UQ_fact_sales_source_line')
    THROW 53003, 'A required alternate-key constraint is missing.', 1;

IF
(
    SELECT COUNT(*) FROM sys.foreign_keys
    WHERE parent_object_id = OBJECT_ID(N'gold.fact_sales')
      AND name IN
      (
          N'FK_fact_sales_dim_customers', N'FK_fact_sales_dim_products',
          N'FK_fact_sales_order_date', N'FK_fact_sales_ship_date', N'FK_fact_sales_due_date'
      )
      AND is_disabled = 0 AND is_not_trusted = 0
) <> 5
    THROW 53004, 'Gold fact foreign keys must exist, be enabled, and be trusted.', 1;

IF
(
    SELECT COUNT(*) FROM sys.check_constraints
    WHERE parent_object_id IN (OBJECT_ID(N'gold.dim_products'), OBJECT_ID(N'gold.fact_sales'))
      AND name IN
      (
          N'CK_dim_products_effective_range', N'CK_fact_sales_line_number',
          N'CK_fact_sales_measure_quality'
      )
) <> 3
    THROW 53005, 'A required Gold check constraint is missing.', 1;

IF EXISTS
(
    SELECT 1 FROM sys.check_constraints
    WHERE parent_object_id IN (OBJECT_ID(N'gold.dim_products'), OBJECT_ID(N'gold.fact_sales'))
      AND name IN
      (
          N'CK_dim_products_effective_range', N'CK_fact_sales_line_number',
          N'CK_fact_sales_measure_quality'
      )
      AND (is_disabled = 1 OR is_not_trusted = 1)
)
    THROW 53007, 'Gold check constraints must be enabled and trusted.', 1;

DECLARE @expected_indexes TABLE
(
    object_name SYSNAME,
    index_name SYSNAME,
    key_columns NVARCHAR(400),
    include_columns NVARCHAR(400)
);

INSERT @expected_indexes VALUES
    (N'gold.dim_products', N'IX_dim_products_asof_lookup', N'product_number,effective_from,effective_to', N'product_key'),
    (N'gold.fact_sales', N'IX_fact_sales_order_date', N'order_date_key', N'customer_key,product_key,sales_amount,quantity'),
    (N'gold.fact_sales', N'IX_fact_sales_customer_date', N'customer_key,order_date_key', N'sales_amount,quantity'),
    (N'gold.fact_sales', N'IX_fact_sales_product_date', N'product_key,order_date_key', N'sales_amount,quantity');

DECLARE @actual_indexes TABLE
(
    object_name SYSNAME,
    index_name SYSNAME,
    key_columns NVARCHAR(400),
    include_columns NVARCHAR(400)
);

INSERT @actual_indexes
SELECT
    CONCAT(OBJECT_SCHEMA_NAME(i.object_id), N'.', OBJECT_NAME(i.object_id)),
    i.name,
    keys.key_columns,
    includes.include_columns
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
WHERE i.name IN
(
    N'IX_dim_products_asof_lookup', N'IX_fact_sales_order_date',
    N'IX_fact_sales_customer_date', N'IX_fact_sales_product_date'
)
  AND i.is_disabled = 0;

IF EXISTS
(
    SELECT object_name, index_name, key_columns, include_columns FROM @expected_indexes
    EXCEPT
    SELECT object_name, index_name, key_columns, include_columns FROM @actual_indexes
)
    THROW 53006, 'Gold workload index structure differs from the model contract.', 1;

SELECT N'PASS' AS model_schema_contract;
GO
