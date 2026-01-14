"""Runner for scripts/bronze_layer/bronze-load-bronze.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/bronze_layer/bronze-load-bronze.sql")
    run_job(
        SqlJob(
            name="bronze_load_bronze",
            sql_path=sql_path,
            job_type="bronze_load",
            output_layer="bronze",
        )
    )


if __name__ == "__main__":
    main()
