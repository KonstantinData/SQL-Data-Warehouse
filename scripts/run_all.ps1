$ErrorActionPreference = "Stop"

python -m src.sql_jobs.run_pipeline
python -m src.sql_jobs.quality_checks_bronze
python -m src.sql_jobs.quality_checks_silver
python -m src.sql_jobs.quality_checks_gold
