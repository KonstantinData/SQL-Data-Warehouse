"""Runner for scripts/bronze_layer/bulk_insert_crm_cust_info.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/bronze_layer/bulk_insert_crm_cust_info.sql")
    run_job(SqlJob(name="bulk_insert_crm_cust_info", sql_path=sql_path, job_type="ddl"))


if __name__ == "__main__":
    main()
