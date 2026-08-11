:ON ERROR EXIT
USE DataWarehouse;
GO
SET NOCOUNT ON;

IF EXISTS (
    SELECT 1
    FROM control.pipeline_batch batch
    WHERE batch.pipeline_name = N'sql-data-warehouse-full-snapshot'
      AND batch.status = 'SUCCEEDED'
      AND EXISTS (
          SELECT required.step_name
          FROM (VALUES
              (N'bronze.full_snapshot'),
              (N'silver.full_snapshot'),
              (N'gold.star_schema'),
              (N'inventory.snapshot')
          ) required(step_name)
          WHERE NOT EXISTS (
              SELECT 1 FROM control.pipeline_step step
              WHERE step.batch_id = batch.batch_id
                AND step.step_name = required.step_name
                AND step.status = 'SUCCEEDED'
                AND step.completed_at_utc IS NOT NULL
          )
      )
)
    THROW 52520, 'A full-scope SUCCEEDED batch lacks complete end-to-end step evidence.', 1;

PRINT 'Full-scope success invariant checks passed.';
GO
