:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DECLARE @batch_id BIGINT;
SELECT TOP (1) @batch_id = batch_id
FROM control.pipeline_batch
WHERE pipeline_name = N'sql-data-warehouse-full-snapshot' AND status = 'SUCCEEDED'
ORDER BY batch_id DESC;

IF @batch_id IS NULL THROW 52300, 'No successful runtime batch is available for coverage checks.', 1;

IF 18484 <> (SELECT COUNT_BIG(*) FROM silver.crm_cust_info WHERE dwh_batch_id = @batch_id)
   OR 397 <> (SELECT COUNT_BIG(*) FROM silver.crm_prd_info WHERE dwh_batch_id = @batch_id)
   OR 60379 <> (SELECT COUNT_BIG(*) FROM silver.crm_sales_details WHERE dwh_batch_id = @batch_id)
   OR 18484 <> (SELECT COUNT_BIG(*) FROM silver.erp_cust_az12 WHERE dwh_batch_id = @batch_id)
   OR 18484 <> (SELECT COUNT_BIG(*) FROM silver.erp_loc_a101 WHERE dwh_batch_id = @batch_id)
   OR 37 <> (SELECT COUNT_BIG(*) FROM silver.erp_px_cat_g1v2 WHERE dwh_batch_id = @batch_id)
    THROW 52307, 'Synthetic Silver cardinalities do not match the verified reference snapshot.', 1;
IF EXISTS (
    SELECT required.table_name
    FROM (VALUES
        (N'silver.crm_cust_info'), (N'silver.crm_prd_info'), (N'silver.crm_sales_details'),
        (N'silver.erp_cust_az12'), (N'silver.erp_loc_a101'), (N'silver.erp_px_cat_g1v2')
    ) required(table_name)
    CROSS APPLY (
        SELECT CASE required.table_name
            WHEN N'silver.crm_cust_info' THEN (SELECT COUNT_BIG(*) FROM silver.crm_cust_info WHERE dwh_batch_id = @batch_id)
            WHEN N'silver.crm_prd_info' THEN (SELECT COUNT_BIG(*) FROM silver.crm_prd_info WHERE dwh_batch_id = @batch_id)
            WHEN N'silver.crm_sales_details' THEN (SELECT COUNT_BIG(*) FROM silver.crm_sales_details WHERE dwh_batch_id = @batch_id)
            WHEN N'silver.erp_cust_az12' THEN (SELECT COUNT_BIG(*) FROM silver.erp_cust_az12 WHERE dwh_batch_id = @batch_id)
            WHEN N'silver.erp_loc_a101' THEN (SELECT COUNT_BIG(*) FROM silver.erp_loc_a101 WHERE dwh_batch_id = @batch_id)
            WHEN N'silver.erp_px_cat_g1v2' THEN (SELECT COUNT_BIG(*) FROM silver.erp_px_cat_g1v2 WHERE dwh_batch_id = @batch_id)
        END AS row_count
    ) counts
    WHERE counts.row_count = 0
)
    THROW 52301, 'One or more Silver sources were not published.', 1;

IF 6 <> (SELECT COUNT(*) FROM control.load_watermark WHERE batch_id = @batch_id)
    THROW 52302, 'A successful batch does not own all six watermarks.', 1;
IF EXISTS (
    SELECT expected.source_name
    FROM (VALUES
        (N'crm_cust_info'), (N'crm_prd_info'), (N'crm_sales_details'),
        (N'erp_cust_az12'), (N'erp_loc_a101'), (N'erp_px_cat_g1v2')
    ) expected(source_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM control.load_watermark w
        INNER JOIN control.pipeline_batch b ON b.batch_id = w.batch_id
        WHERE w.batch_id = @batch_id AND w.source_name = expected.source_name
          AND w.pipeline_name = b.pipeline_name
          AND w.source_version = b.source_version
          AND w.watermark_value = b.source_watermark
    )
)
    THROW 52308, 'Watermarks do not match the exact six-source batch contract.', 1;
IF EXISTS (
    SELECT cust_id FROM silver.crm_cust_info WHERE dwh_batch_id = @batch_id GROUP BY cust_id HAVING COUNT(*) > 1
)
    THROW 52303, 'Operational customer keys are not unique.', 1;
IF EXISTS (
    SELECT prd_id FROM silver.crm_prd_info WHERE dwh_batch_id = @batch_id GROUP BY prd_id HAVING COUNT(*) > 1
)
    THROW 52304, 'Operational product-version IDs are not unique.', 1;
IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details s
    WHERE s.dwh_batch_id = @batch_id
      AND (s.order_date IS NULL OR s.sls_quantity IS NULL OR s.sls_price IS NULL OR s.sls_sales IS NULL
           OR s.sls_quantity <= 0 OR s.sls_price <= 0 OR s.sls_sales <> s.sls_quantity * s.sls_price
           OR (s.sls_ship_dt <> 0 AND s.ship_date IS NULL)
           OR (s.sls_due_dt <> 0 AND s.due_date IS NULL)
           OR (s.ship_date IS NOT NULL AND s.ship_date < s.order_date)
           OR (s.due_date IS NOT NULL AND s.due_date < s.order_date)
           OR (s.ship_date IS NOT NULL AND s.due_date IS NOT NULL AND s.due_date < s.ship_date))
)
    THROW 52305, 'Published Sales rows violate normalized date/value rules.', 1;
IF EXISTS (
    SELECT 1 FROM silver.crm_sales_details s
    WHERE s.dwh_batch_id = @batch_id
      AND (NOT EXISTS (SELECT 1 FROM silver.crm_cust_info c WHERE c.dwh_batch_id = @batch_id AND c.cust_id = s.sls_cust_id)
           OR NOT EXISTS (SELECT 1 FROM silver.crm_prd_info p WHERE p.dwh_batch_id = @batch_id AND SUBSTRING(p.prd_key, 7, LEN(p.prd_key)) = s.sls_prd_key))
)
    THROW 52306, 'Published Sales contains an orphan customer or product.', 1;

IF EXISTS (
    SELECT prd_key, prd_start_dt
    FROM silver.crm_prd_info WHERE dwh_batch_id = @batch_id
    GROUP BY prd_key, prd_start_dt HAVING COUNT(*) > 1
)
    THROW 52309, 'Published product versions have duplicate effective starts.', 1;

IF EXISTS (
    SELECT 1
    FROM (VALUES
        (N'crm_cust_info', N'MISSING_REQUIRED_KEY', 4),
        (N'crm_sales_details', N'INVALID_ORDER_DATE', 18),
        (N'crm_sales_details', N'INVALID_DATE_SEQUENCE', 1)
    ) expected(source_name, rule_code, expected_count)
    OUTER APPLY (
        SELECT COUNT(*) AS actual_count
        FROM control.load_reject r
        WHERE r.batch_id = @batch_id
          AND r.source_name = expected.source_name
          AND r.rule_code = expected.rule_code
    ) actual
    WHERE actual.actual_count <> expected.expected_count
)
    THROW 52310, 'Synthetic reject evidence does not match the verified 23-row rule distribution.', 1;
IF 23 <> (SELECT COUNT(*) FROM control.load_reject WHERE batch_id = @batch_id)
    THROW 52311, 'Synthetic batch must contain exactly 23 reject records.', 1;
IF EXISTS (
    SELECT 1 FROM control.load_reject
    WHERE batch_id = @batch_id
      AND (step_id IS NULL OR source_file IS NULL OR source_row_number IS NULL
           OR rule_code IS NULL OR raw_payload IS NULL OR error_message IS NULL)
)
    THROW 52312, 'Reject provenance is incomplete.', 1;
IF NOT EXISTS (
    SELECT 1 FROM control.pipeline_step
    WHERE batch_id=@batch_id AND step_name=N'bronze.full_snapshot' AND status='SUCCEEDED'
      AND completed_at_utc IS NOT NULL AND rows_read=116294 AND rows_rejected=4 AND rows_published=116290
)
   OR NOT EXISTS (
    SELECT 1 FROM control.pipeline_step
    WHERE batch_id=@batch_id AND step_name=N'silver.full_snapshot' AND status='SUCCEEDED'
      AND completed_at_utc IS NOT NULL AND rows_read=116290 AND rows_rejected=19
      AND rows_superseded=6 AND rows_published=116265
)
    THROW 52313, 'Batch step reconciliation does not match the synthetic snapshot.', 1;

IF OBJECT_ID(N'gold.dim_customers', N'U') IS NOT NULL
   AND (18485 <> (SELECT COUNT_BIG(*) FROM gold.dim_customers)
        OR 398 <> (SELECT COUNT_BIG(*) FROM gold.dim_products)
        OR 60379 <> (SELECT COUNT_BIG(*) FROM gold.fact_sales))
    THROW 52314, 'Installed physical Gold model is incompatible with the runtime snapshot.', 1;

PRINT 'Silver six-source coverage checks passed.';
GO
