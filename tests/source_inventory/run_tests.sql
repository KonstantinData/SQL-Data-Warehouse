:on error exit

/* Local SQLCMD idempotency and quality entrypoint. */
:r .\scripts\source_inventory\run_source_inventory.sql
:r .\tests\source_inventory\capture_run_state.sql
:r .\scripts\source_inventory\run_source_inventory.sql
:r .\tests\source_inventory\compare_run_state.sql
:r .\tests\source_inventory\quality_checks.sql
