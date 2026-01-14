"""Runner for scripts/gold_layer/create_gold_views.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/gold_layer/create_gold_views.sql")
    run_job(SqlJob(name="create_gold_views", sql_path=sql_path, job_type="transform", output_layer="gold"))


if __name__ == "__main__":
    main()
