"""Runner for scripts/silver_layer/create_silver_table_structure.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/silver_layer/create_silver_table_structure.sql")
    run_job(SqlJob(name="create_silver_table_structure", sql_path=sql_path, job_type="ddl"))


if __name__ == "__main__":
    main()
