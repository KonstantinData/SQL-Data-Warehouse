"""Runner for scripts/init.database.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/init.database.sql")
    run_job(SqlJob(name="init_database", sql_path=sql_path, job_type="init"))


if __name__ == "__main__":
    main()
