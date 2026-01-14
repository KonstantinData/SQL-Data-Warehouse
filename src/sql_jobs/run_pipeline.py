"""Runner for scripts/run_pipeline.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    run_job(SqlJob("init_database", Path("scripts/init.database.sql"), "init"))
    run_job(SqlJob("create_table_bronze_layer", Path("scripts/bronze_layer/create_table_bronze_layer.sql"), "ddl"))
    run_job(SqlJob("bronze_load_bronze", Path("scripts/bronze_layer/bronze-load-bronze.sql"), "bronze_load", "bronze"))
    run_job(
        SqlJob("create_silver_table_structure", Path("scripts/silver_layer/create_silver_table_structure.sql"), "ddl")
    )
    run_job(
        SqlJob("cleansing_crm_cust_info", Path("scripts/silver_layer/cleansing_crm_cust_info.sql"), "transform", "silver")
    )
    run_job(
        SqlJob("cleansing_crm_prd_info", Path("scripts/silver_layer/cleansing_crm_prd_info.sql"), "transform", "silver")
    )
    run_job(SqlJob("create_gold_views", Path("scripts/gold_layer/create_gold_views.sql"), "transform", "gold"))


if __name__ == "__main__":
    main()
