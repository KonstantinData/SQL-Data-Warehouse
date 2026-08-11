:ON ERROR EXIT
:setvar SourceVersion "ci-downstream-failure-v1"
:setvar SourceWatermark "2"
:setvar MaxRejectRows "23"
:setvar RestartOfBatchId "0"
:setvar SnapshotAsOf "2024-12-31"

:r scripts/operations/run_full_pipeline.sql
