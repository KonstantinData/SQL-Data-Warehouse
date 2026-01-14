#!/usr/bin/env bash
set -euo pipefail

python -m src.sql_jobs.run_ci_pipeline
python -m src.sql_jobs.quality_checks_ci
