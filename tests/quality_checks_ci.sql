/* Combined SQLCMD compatibility entry point. The layer contracts remain
   independently executable so CI can prove their negative behavior. */

:On Error exit
:r /workspace/tests/ci_pipeline_contract.sql
GO
:r /workspace/tests/quality_checks_bronze.sql
GO
:r /workspace/tests/quality_checks_silver.sql
GO
:r /workspace/tests/quality_checks_gold.sql
GO
