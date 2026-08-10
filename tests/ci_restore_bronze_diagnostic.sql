SET NOCOUNT ON;

DELETE FROM bronze.crm_prd_info
WHERE prd_key = N'__ci_bronze_diagnostic__';

IF @@ROWCOUNT <> 1
    THROW 51000, 'Bronze diagnostic mutation cleanup did not remove exactly one product row.', 1;
