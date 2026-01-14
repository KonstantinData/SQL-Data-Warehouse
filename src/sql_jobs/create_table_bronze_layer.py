"""Runner for scripts/bronze_layer/create_table_bronze_layer.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/bronze_layer/create_table_bronze_layer.sql")
    run_job(SqlJob(name="create_table_bronze_layer", sql_path=sql_path, job_type="ddl"))


if __name__ == "__main__":
    main()
