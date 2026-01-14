"""Runner for scripts/silver_layer/cleansing_crm_prd_info.sql."""

from pathlib import Path

from ._runtime import SqlJob, run_job


def main() -> None:
    sql_path = Path("scripts/silver_layer/cleansing_crm_prd_info.sql")
    run_job(
        SqlJob(
            name="cleansing_crm_prd_info",
            sql_path=sql_path,
            job_type="transform",
            output_layer="silver",
        )
    )


if __name__ == "__main__":
    main()
