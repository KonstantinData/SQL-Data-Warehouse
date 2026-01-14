"""Runner for scripts/ci/load_ci_silver.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/ci/load_ci_silver.sql")
    run_job(SqlJob(name="load_ci_silver", sql_path=sql_path, job_type="transform", output_layer="silver"))


if __name__ == "__main__":
    main()
