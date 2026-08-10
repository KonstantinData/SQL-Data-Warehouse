/*
================================================================================
Compatibility entrypoint for the physical Gold model
================================================================================
Historically this file duplicated the complete Gold implementation. It now
delegates to the canonical idempotent modules so compatibility callers and the
operational pipeline execute exactly the same table, load, and index contracts.
================================================================================
*/

:ON ERROR EXIT

:r ./scripts/gold_layer/00_create_gold_tables.sql
:r ./scripts/gold_layer/10_load_gold.sql
:r ./scripts/gold_layer/20_create_gold_indexes.sql

USE DataWarehouse;
GO

EXEC gold.usp_load_gold;
GO
