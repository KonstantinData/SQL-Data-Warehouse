SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'crm_cst_info' 
AND table_schema = 'bronze';