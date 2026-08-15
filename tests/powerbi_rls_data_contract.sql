/* Deterministic Gold country baselines used by the Power BI RLS acceptance matrix. */
:on error exit

USE DataWarehouse;
GO

SET NOCOUNT ON;
DECLARE @error_count INT = 0;

;WITH actual AS
(
    SELECT
        CASE
            WHEN UPPER(TRIM(customers.country)) IN ('DE', 'GERMANY') THEN 'DE'
            WHEN UPPER(TRIM(customers.country)) IN ('US', 'USA', 'UNITED STATES') THEN 'US'
        END AS country_code,
        COUNT_BIG(*) AS sales_rows,
        CONVERT(DECIMAL(19,2), SUM(sales.sales_amount)) AS sales_total
    FROM gold.fact_sales AS sales
    INNER JOIN gold.dim_customers AS customers
        ON customers.customer_key = sales.customer_key
    WHERE UPPER(TRIM(customers.country)) IN ('DE', 'GERMANY', 'US', 'USA', 'UNITED STATES')
    GROUP BY
        CASE
            WHEN UPPER(TRIM(customers.country)) IN ('DE', 'GERMANY') THEN 'DE'
            WHEN UPPER(TRIM(customers.country)) IN ('US', 'USA', 'UNITED STATES') THEN 'US'
        END
), expected AS
(
    SELECT *
    FROM (VALUES
        ('DE', CONVERT(BIGINT, 5625), CONVERT(DECIMAL(19,2), 2894066.00)),
        ('US', CONVERT(BIGINT, 20466), CONVERT(DECIMAL(19,2), 9162225.00))
    ) valueset(country_code, sales_rows, sales_total)
)
SELECT @error_count += COUNT(*)
FROM
(
    SELECT country_code, sales_rows, sales_total FROM expected
    EXCEPT
    SELECT country_code, sales_rows, sales_total FROM actual

    UNION ALL

    SELECT country_code, sales_rows, sales_total FROM actual
    EXCEPT
    SELECT country_code, sales_rows, sales_total FROM expected
) differences;

;WITH actual AS
(
    SELECT
        locations.country_code,
        COUNT_BIG(*) AS inventory_rows,
        SUM(CONVERT(BIGINT, snapshots.available_qty)) AS available_qty,
        CONVERT(DECIMAL(19,2), SUM(snapshots.inventory_value)) AS inventory_value
    FROM gold.fact_inventory_snapshots AS snapshots
    INNER JOIN gold.dim_inventory_locations AS locations
        ON locations.warehouse_key = snapshots.warehouse_key
    GROUP BY locations.country_code
), expected AS
(
    SELECT *
    FROM (VALUES
        ('DE', CONVERT(BIGINT, 10), CONVERT(BIGINT, 335), CONVERT(DECIMAL(19,2), 4826.00)),
        ('US', CONVERT(BIGINT, 1), CONVERT(BIGINT, 20), CONVERT(DECIMAL(19,2), 312.50))
    ) valueset(country_code, inventory_rows, available_qty, inventory_value)
)
SELECT @error_count += COUNT(*)
FROM
(
    SELECT country_code, inventory_rows, available_qty, inventory_value FROM expected
    EXCEPT
    SELECT country_code, inventory_rows, available_qty, inventory_value FROM actual

    UNION ALL

    SELECT country_code, inventory_rows, available_qty, inventory_value FROM actual
    EXCEPT
    SELECT country_code, inventory_rows, available_qty, inventory_value FROM expected
) differences;

IF @error_count > 0
    THROW 51071, 'Power BI RLS country baselines differ from the deterministic acceptance contract.', 1;

PRINT 'Power BI RLS country baseline contract passed.';
GO
