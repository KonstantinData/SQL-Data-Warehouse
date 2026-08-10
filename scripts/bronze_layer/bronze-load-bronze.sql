/*
Legacy compatibility notice
===========================
The operational loader requires an immutable source version and a monotonic
watermark. Run scripts/operations/run_operational_pipeline.sql instead.

This file intentionally fails closed so legacy runners cannot silently execute
an unversioned snapshot.
*/

USE DataWarehouse;
GO

THROW 51190, 'Unversioned Bronze execution is disabled. Use scripts/operations/run_operational_pipeline.sql.', 1;
GO
