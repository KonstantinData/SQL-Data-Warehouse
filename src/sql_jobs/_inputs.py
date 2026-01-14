"""CSV inputs for local DuckDB runs."""

INPUTS = {
    "raw.crm_cust_info": "datasets/source_crm/cst_info.csv",
    "raw.crm_prd_info": "datasets/source_crm/prd_info.csv",
    "raw.crm_sales_details": "datasets/source_crm/sales_details.csv",
    "raw.erp_cust_az12": "datasets/source_erp/CST_AZ12.csv",
    "raw.erp_loc_a101": "datasets/source_erp/LOC_A101.csv",
    "raw.erp_px_cat_g1v2": "datasets/source_erp/PX_CAT_G1V2.csv",
}

BRONZE_LOADS = {
    "bronze.crm_cust_info": "raw.crm_cust_info",
    "bronze.crm_prd_info": "raw.crm_prd_info",
    "bronze.crm_sales_details": "raw.crm_sales_details",
    "bronze.erp_cust_az12": "raw.erp_cust_az12",
    "bronze.erp_loc_a101": "raw.erp_loc_a101",
    "bronze.erp_px_cat_g1v2": "raw.erp_px_cat_g1v2",
}
