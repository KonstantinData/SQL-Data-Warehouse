"""Runner for tests/quality_checks_gold.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("tests/quality_checks_gold.sql")
    run_job(SqlJob(name="quality_checks_gold", sql_path=sql_path, job_type="quality"))


if __name__ == "__main__":
    main()
