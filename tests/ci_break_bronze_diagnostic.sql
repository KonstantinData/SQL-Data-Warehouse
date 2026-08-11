SET NOCOUNT ON;

IF EXISTS (SELECT 1 FROM bronze.crm_prd_info WHERE prd_key = N'__ci_bronze_diagnostic__')
    THROW 51000, 'Bronze diagnostic mutation already exists.', 1;

INSERT INTO bronze.crm_prd_info (
    prd_id, prd_key, prd_nm, prd_cost,
    prd_line, prd_start_dt, prd_end_dt
)
VALUES (
    -2147483648, N'__ci_bronze_diagnostic__', N'Synthetic diagnostic product', -1,
    N'CI', '2026-01-01', NULL
);
