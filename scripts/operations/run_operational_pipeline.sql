:ON ERROR EXIT

/*
Supported routine entry point after the repository modules have been installed.
It delegates to the canonical end-to-end execution contract so SUCCEEDED means
CRM/ERP Bronze, Silver, physical Gold, and Inventory all published successfully.
*/

:r .\scripts\operations\run_full_pipeline.sql
