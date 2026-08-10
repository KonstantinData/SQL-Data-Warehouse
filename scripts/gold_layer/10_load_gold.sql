USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE gold.usp_load_gold
    @date_start DATE = NULL,
    @date_end   DATE = NULL,
    @snapshot_as_of DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'silver.crm_cust_info', N'U') IS NULL
       OR OBJECT_ID(N'silver.crm_prd_info', N'U') IS NULL
       OR OBJECT_ID(N'silver.crm_sales_details', N'U') IS NULL
       OR OBJECT_ID(N'silver.erp_cust_az12', N'U') IS NULL
       OR OBJECT_ID(N'silver.erp_loc_a101', N'U') IS NULL
       OR OBJECT_ID(N'silver.erp_px_cat_g1v2', N'U') IS NULL
        THROW 51000, 'Gold load requires all CRM and ERP Silver tables.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @application_lock_result INT;
        EXEC @application_lock_result = sys.sp_getapplock
            @Resource = N'gold.usp_load_gold',
            @LockMode = N'Exclusive',
            @LockOwner = N'Transaction',
            @LockTimeout = 0;
        IF @application_lock_result < 0
            THROW 51007, 'Another Gold load is already in progress.', 1;

        SELECT
            c.cust_id AS customer_id,
            COALESCE(NULLIF(TRIM(c.cust_key), N''), CONCAT(N'CUSTOMER-', c.cust_id)) AS customer_number,
            NULLIF(TRIM(c.cust_firstname), N'') AS first_name,
            CONVERT(CHAR(64), HASHBYTES('SHA2_256', COALESCE(NULLIF(TRIM(c.cust_lastname), N''), N'[NULL]')), 2) AS last_name_hash,
            c.cust_marital_status AS marital_status,
            COALESCE(NULLIF(c.cust_gender, N'n/a'), demographics.gender, N'Unknown') AS gender,
            demographics.birth_date,
            location.country,
            c.cust_create_date AS customer_create_date,
            ISNULL(c.cust_is_future, 0) AS customer_is_future
        INTO #customer_source
        FROM silver.crm_cust_info AS c
        OUTER APPLY
        (
            SELECT TOP (1)
                e.bdate AS birth_date,
                CASE UPPER(TRIM(e.gen))
                    WHEN N'M' THEN N'Male'
                    WHEN N'F' THEN N'Female'
                    ELSE NULLIF(TRIM(e.gen), N'')
                END AS gender
            FROM silver.erp_cust_az12 AS e
            WHERE RIGHT(e.cid, 10) = c.cust_key
            ORDER BY e.dwh_create_date DESC
        ) AS demographics
        OUTER APPLY
        (
            SELECT TOP (1) NULLIF(TRIM(l.cntry), N'') AS country
            FROM silver.erp_loc_a101 AS l
            WHERE REPLACE(l.cid, N'-', N'') = c.cust_key
            ORDER BY l.dwh_create_date DESC
        ) AS location
        WHERE c.cust_id > 0;

        IF EXISTS (SELECT customer_id FROM #customer_source GROUP BY customer_id HAVING COUNT(*) > 1)
            THROW 51001, 'Customer source violates the one-row-per-customer Gold grain.', 1;

        UPDATE target
        SET
            customer_number = source.customer_number,
            first_name = source.first_name,
            last_name_hash = source.last_name_hash,
            marital_status = source.marital_status,
            gender = source.gender,
            birth_date = source.birth_date,
            country = source.country,
            customer_create_date = source.customer_create_date,
            customer_is_future = source.customer_is_future,
            dwh_updated_at = SYSUTCDATETIME()
        FROM gold.dim_customers AS target
        INNER JOIN #customer_source AS source
            ON source.customer_id = target.customer_id;

        INSERT gold.dim_customers
        (
            customer_id, customer_number, first_name, last_name_hash,
            marital_status, gender, birth_date, country, customer_create_date,
            customer_is_future
        )
        SELECT
            source.customer_id, source.customer_number, source.first_name,
            source.last_name_hash, source.marital_status, source.gender,
            source.birth_date, source.country, source.customer_create_date,
            source.customer_is_future
        FROM #customer_source AS source
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM gold.dim_customers AS target WITH (UPDLOCK, HOLDLOCK)
            WHERE target.customer_id = source.customer_id
        );

        ;WITH product_base AS
        (
            SELECT
                p.prd_id AS product_id,
                COALESCE(NULLIF(TRIM(SUBSTRING(p.prd_key, 7, LEN(p.prd_key))), N''), CONCAT(N'PRODUCT-', p.prd_id)) AS product_number,
                NULLIF(TRIM(p.prd_nm), N'') AS product_name,
                CONVERT(DECIMAL(18, 2), ISNULL(p.prd_cost, 0)) AS product_cost,
                NULLIF(TRIM(p.prd_line), N'') AS product_line,
                category.cat AS category,
                category.subcat AS subcategory,
                category.maintenance,
                COALESCE(CONVERT(DATE, p.prd_start_dt), CONVERT(DATE, '19000101', 112)) AS effective_from,
                CONVERT(DATE, p.prd_end_dt) AS source_end_date
            FROM silver.crm_prd_info AS p
            LEFT JOIN silver.erp_px_cat_g1v2 AS category
                ON category.id = REPLACE(SUBSTRING(p.prd_key, 1, 5), N'-', N'_')
            WHERE p.prd_id > 0
        ),
        product_with_next AS
        (
            SELECT
                product_id, product_number, product_name, product_cost,
                product_line, category, subcategory, maintenance, effective_from,
                LEAD(effective_from) OVER
                (
                    PARTITION BY product_number
                    ORDER BY effective_from, product_id
                ) AS next_effective_from,
                source_end_date
            FROM product_base
        ),
        product_versioned AS
        (
            SELECT
                product_id, product_number, product_name, product_cost,
                product_line, category, subcategory, maintenance, effective_from,
                COALESCE(
                    next_effective_from,
                    CASE
                        WHEN source_end_date IS NOT NULL AND source_end_date < CONVERT(DATE, '99991231', 112)
                            THEN DATEADD(DAY, 1, source_end_date)
                    END
                ) AS effective_to,
                source_end_date,
                CONVERT(BIT, CASE
                    WHEN next_effective_from IS NULL AND source_end_date IS NULL THEN 1
                    ELSE 0
                END) AS is_current
            FROM product_with_next
        )
        SELECT *
        INTO #product_source
        FROM product_versioned;

        IF EXISTS (SELECT product_id FROM #product_source GROUP BY product_id HAVING COUNT(*) > 1)
            THROW 51002, 'Product source violates the one-row-per-product-version Gold grain.', 1;

        IF EXISTS
        (
            SELECT product_number, effective_from
            FROM #product_source
            GROUP BY product_number, effective_from
            HAVING COUNT(*) > 1
        )
            THROW 51003, 'Product versions have duplicate effective start dates.', 1;

        UPDATE target
        SET
            product_number = source.product_number,
            product_name = source.product_name,
            product_cost = source.product_cost,
            product_line = source.product_line,
            category = source.category,
            subcategory = source.subcategory,
            maintenance = source.maintenance,
            effective_from = source.effective_from,
            effective_to = CASE
                WHEN target.is_current = 0 AND source.is_current = 1 THEN target.effective_to
                ELSE source.effective_to
            END,
            source_end_date = source.source_end_date,
            is_current = CASE
                WHEN target.is_current = 0 AND source.is_current = 1 THEN CONVERT(BIT, 0)
                ELSE source.is_current
            END,
            dwh_updated_at = SYSUTCDATETIME()
        FROM gold.dim_products AS target
        INNER JOIN #product_source AS source
            ON source.product_id = target.product_id;

        INSERT gold.dim_products
        (
            product_id, product_number, product_name, product_cost, product_line,
            category, subcategory, maintenance, effective_from, effective_to,
            source_end_date, is_current
        )
        SELECT
            source.product_id, source.product_number, source.product_name,
            source.product_cost, source.product_line, source.category,
            source.subcategory, source.maintenance, source.effective_from,
            source.effective_to, source.source_end_date, source.is_current
        FROM #product_source AS source
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM gold.dim_products AS target WITH (UPDLOCK, HOLDLOCK)
            WHERE target.product_id = source.product_id
        );

        /*
        Silver is a complete current snapshot. Product versions absent from the
        new snapshot must therefore no longer remain current in the retained
        SCD2 history. Retired products are allowed to have no current version.
        */
        UPDATE target
        SET effective_to = COALESCE(
                target.effective_to,
                CASE
                    WHEN target.source_end_date > target.effective_from THEN target.source_end_date
                    WHEN COALESCE(@snapshot_as_of, CONVERT(DATE, SYSUTCDATETIME())) > target.effective_from
                        THEN COALESCE(@snapshot_as_of, CONVERT(DATE, SYSUTCDATETIME()))
                    ELSE DATEADD(DAY, 1, target.effective_from)
                END
            ),
            is_current = 0,
            dwh_updated_at = SYSUTCDATETIME()
        FROM gold.dim_products AS target
        WHERE target.product_key <> 0
          AND target.is_current = 1
          AND NOT EXISTS (
              SELECT 1 FROM #product_source AS source
              WHERE source.product_id = target.product_id
          );

        IF EXISTS (
            SELECT product_number
            FROM gold.dim_products
            WHERE product_key <> 0 AND is_current = 1
            GROUP BY product_number
            HAVING COUNT_BIG(*) > 1
        )
            THROW 51008, 'Gold product history has more than one current version per product.', 1;

        /*
        Silver integration paths can multiply an otherwise identical CRM line
        when they join it to multiple product versions. Gold restores the
        validated source grain before temporal product resolution.
        */
        SELECT DISTINCT
            NULLIF(TRIM(f.sls_ord_num), N'') AS order_number,
            NULLIF(TRIM(f.sls_prd_key), N'') AS product_number,
            f.sls_cust_id AS customer_id,
            CASE WHEN f.sls_order_dt BETWEEN 10000101 AND 99991231
                THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), f.sls_order_dt), 112) END AS order_date,
            CASE WHEN f.sls_ship_dt BETWEEN 10000101 AND 99991231
                THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), f.sls_ship_dt), 112) END AS ship_date,
            CASE WHEN f.sls_due_dt BETWEEN 10000101 AND 99991231
                THEN TRY_CONVERT(DATE, CONVERT(CHAR(8), f.sls_due_dt), 112) END AS due_date,
            CONVERT(DECIMAL(18, 2), f.sls_sales) AS sales_amount,
            f.sls_quantity AS quantity,
            CONVERT(DECIMAL(18, 2), f.sls_price) AS price,
            CASE
                WHEN f.sls_sales IS NULL OR f.sls_quantity IS NULL OR f.sls_price IS NULL
                  OR f.sls_quantity <= 0 OR f.sls_price < 0 OR f.sls_sales < 0
                    THEN N'INVALID_SOURCE_MEASURE'
                WHEN f.sls_sales <> f.sls_quantity * f.sls_price
                    THEN N'SOURCE_ADJUSTMENT'
                ELSE N'VALID'
            END AS measure_quality_code
        INTO #fact_source_raw
        FROM silver.crm_sales_details AS f;

        IF EXISTS
        (
            SELECT 1 FROM #fact_source_raw
            WHERE order_number IS NULL OR product_number IS NULL
        )
            THROW 51004, 'Sales source contains a missing order or product grain key.', 1;

        IF EXISTS
        (
            SELECT order_number, product_number
            FROM #fact_source_raw
            GROUP BY order_number, product_number
            HAVING COUNT(*) > 1
        )
            THROW 51005, 'Sales source has conflicting payloads for the validated order/product line identity.', 1;

        SELECT @date_start = CASE
            WHEN @date_start IS NULL OR source_dates.minimum_date < @date_start THEN source_dates.minimum_date
            ELSE @date_start END,
            @date_end = CASE
            WHEN @date_end IS NULL OR source_dates.maximum_date > @date_end THEN source_dates.maximum_date
            ELSE @date_end END
        FROM
        (
            SELECT MIN(date_value) AS minimum_date, MAX(date_value) AS maximum_date
            FROM #fact_source_raw
            CROSS APPLY (VALUES (order_date), (ship_date), (due_date)) AS dates(date_value)
            WHERE date_value IS NOT NULL
        ) AS source_dates;

        SET @date_start = COALESCE(@date_start, CONVERT(DATE, '20000101', 112));
        SET @date_end = COALESCE(@date_end, CONVERT(DATE, '20301231', 112));

        IF @date_end < @date_start
            THROW 51006, 'Date dimension end date must not precede its start date.', 1;

        ;WITH calendar AS
        (
            SELECT @date_start AS calendar_date
            UNION ALL
            SELECT DATEADD(DAY, 1, calendar_date)
            FROM calendar
            WHERE calendar_date < @date_end
        )
        INSERT gold.dim_date
        (
            date_key, calendar_date, calendar_year, calendar_quarter,
            calendar_month, month_name, day_of_month, day_of_week_iso,
            day_name, iso_week, is_weekend, month_start_date
        )
        SELECT
            CONVERT(INT, CONVERT(CHAR(8), calendar.calendar_date, 112)),
            calendar.calendar_date,
            DATEPART(YEAR, calendar.calendar_date),
            DATEPART(QUARTER, calendar.calendar_date),
            DATEPART(MONTH, calendar.calendar_date),
            CASE DATEPART(MONTH, calendar.calendar_date)
                WHEN 1 THEN N'January' WHEN 2 THEN N'February'
                WHEN 3 THEN N'March' WHEN 4 THEN N'April'
                WHEN 5 THEN N'May' WHEN 6 THEN N'June'
                WHEN 7 THEN N'July' WHEN 8 THEN N'August'
                WHEN 9 THEN N'September' WHEN 10 THEN N'October'
                WHEN 11 THEN N'November' WHEN 12 THEN N'December'
            END,
            DATEPART(DAY, calendar.calendar_date),
            ((DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), calendar.calendar_date) % 7) + 1),
            CASE ((DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), calendar.calendar_date) % 7) + 1)
                WHEN 1 THEN N'Monday' WHEN 2 THEN N'Tuesday'
                WHEN 3 THEN N'Wednesday' WHEN 4 THEN N'Thursday'
                WHEN 5 THEN N'Friday' WHEN 6 THEN N'Saturday'
                WHEN 7 THEN N'Sunday'
            END,
            DATEPART(ISO_WEEK, calendar.calendar_date),
            CASE WHEN ((DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), calendar.calendar_date) % 7) + 1) IN (6, 7)
                THEN 1 ELSE 0 END,
            DATEFROMPARTS(YEAR(calendar.calendar_date), MONTH(calendar.calendar_date), 1)
        FROM calendar
        WHERE NOT EXISTS
        (
            SELECT 1 FROM gold.dim_date AS target
            WHERE target.calendar_date = calendar.calendar_date
        )
        OPTION (MAXRECURSION 0);

        ;WITH numbered AS
        (
            SELECT
                source.*,
                ROW_NUMBER() OVER
                (
                    PARTITION BY source.order_number
                    ORDER BY source.product_number, source.customer_id,
                             source.order_date, source.ship_date, source.due_date,
                             source.sales_amount, source.quantity, source.price
                ) AS sales_order_line_number
            FROM #fact_source_raw AS source
        )
        SELECT
            numbered.order_number,
            CONVERT(INT, numbered.sales_order_line_number) AS sales_order_line_number,
            numbered.product_number,
            COALESCE(customer.customer_key, 0) AS customer_key,
            CASE WHEN product_match.match_count = 1 THEN product_match.product_key ELSE 0 END AS product_key,
            COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.order_date, 112)), 0) AS order_date_key,
            COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.ship_date, 112)), 0) AS ship_date_key,
            COALESCE(CONVERT(INT, CONVERT(CHAR(8), numbered.due_date, 112)), 0) AS due_date_key,
            numbered.sales_amount,
            numbered.quantity,
            numbered.order_date,
            numbered.ship_date,
            numbered.due_date,
            numbered.price,
            numbered.measure_quality_code
        INTO #fact_source
        FROM numbered
        LEFT JOIN gold.dim_customers AS customer
            ON customer.customer_id = numbered.customer_id
        OUTER APPLY
        (
            SELECT COUNT(*) AS match_count, MIN(product.product_key) AS product_key
            FROM gold.dim_products AS product
            WHERE product.product_key <> 0
              AND product.product_number = numbered.product_number
              AND numbered.order_date >= product.effective_from
              AND (product.effective_to IS NULL OR numbered.order_date < product.effective_to)
        ) AS product_match;

        /* Move current ordinals out of the target range before a possible reorder. */
        UPDATE target
        SET sales_order_line_number = sales_order_line_number + 1000000
        FROM gold.fact_sales AS target
        WHERE EXISTS
        (
            SELECT 1 FROM #fact_source AS source
            WHERE source.order_number = target.order_number
              AND source.product_number = target.product_number
        );

        UPDATE target
        SET
            sales_order_line_number = source.sales_order_line_number,
            customer_key = source.customer_key,
            product_key = source.product_key,
            order_date_key = source.order_date_key,
            ship_date_key = source.ship_date_key,
            due_date_key = source.due_date_key,
            order_date = source.order_date,
            ship_date = source.ship_date,
            due_date = source.due_date,
            sales_amount = source.sales_amount,
            quantity = source.quantity,
            price = source.price,
            measure_quality_code = source.measure_quality_code,
            dwh_updated_at = SYSUTCDATETIME()
        FROM gold.fact_sales AS target
        INNER JOIN #fact_source AS source
            ON source.order_number = target.order_number
           AND source.product_number = target.product_number;

        INSERT gold.fact_sales
        (
            order_number, sales_order_line_number, product_number, customer_key,
            product_key, order_date_key, ship_date_key, due_date_key,
            order_date, ship_date, due_date, sales_amount, quantity, price,
            measure_quality_code
        )
        SELECT
            source.order_number, source.sales_order_line_number,
            source.product_number, source.customer_key, source.product_key,
            source.order_date_key, source.ship_date_key, source.due_date_key,
            source.order_date, source.ship_date, source.due_date,
            source.sales_amount, source.quantity, source.price,
            source.measure_quality_code
        FROM #fact_source AS source
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM gold.fact_sales AS target WITH (UPDLOCK, HOLDLOCK)
            WHERE target.order_number = source.order_number
              AND target.product_number = source.product_number
        );

        DELETE target
        FROM gold.fact_sales AS target
        WHERE NOT EXISTS
        (
            SELECT 1 FROM #fact_source AS source
            WHERE source.order_number = target.order_number
              AND source.product_number = target.product_number
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
