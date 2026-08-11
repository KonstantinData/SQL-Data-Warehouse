:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;

IF N'$(ConfirmRuntimeTests)' <> N'RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE'
    THROW 52505, 'Mutating downstream tests require the exact disposable-database confirmation token.', 1;

IF OBJECT_ID(N'gold.fact_sales', N'U') IS NULL
    THROW 52500, 'Downstream failure fixture requires gold.fact_sales.', 1;
IF OBJECT_ID(N'gold.CK_runtime_downstream_failure', N'C') IS NOT NULL
    ALTER TABLE gold.fact_sales DROP CONSTRAINT CK_runtime_downstream_failure;

ALTER TABLE gold.fact_sales WITH NOCHECK
ADD CONSTRAINT CK_runtime_downstream_failure CHECK (sales_amount < 0);
GO
