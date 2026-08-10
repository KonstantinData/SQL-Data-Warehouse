:on error exit

/* Container-path SQLCMD idempotency and quality entrypoint. */
:r /workspace/scripts/source_inventory/run_source_inventory_ci.sql
:r /workspace/tests/source_inventory/capture_run_state.sql
:r /workspace/scripts/source_inventory/run_source_inventory_ci.sql
:r /workspace/tests/source_inventory/compare_run_state.sql
:r /workspace/tests/source_inventory/quality_checks.sql
