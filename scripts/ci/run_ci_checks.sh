#!/usr/bin/env bash
set -euo pipefail

readonly MSSQL_IMAGE="mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04@sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89"
readonly SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

repo_root="$(pwd)"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    repo_root="$(pwd -W)"
    export MSYS_NO_PATHCONV=1
    ;;
esac

if [[ ! -f "scripts/ci/run_ci_pipeline.sql" || ! -d "datasets" ]]; then
  echo "Run this command from the repository root." >&2
  exit 1
fi

for required_command in docker openssl mktemp awk cat grep sed tail tee tr; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "Required command is not available: $required_command" >&2
    exit 1
  fi
done

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    if ! command -v cygpath >/dev/null 2>&1; then
      echo "Required command is not available: cygpath" >&2
      exit 1
    fi
    ;;
esac

if command -v python3 >/dev/null 2>&1 \
   && python3 -c "import sys; raise SystemExit(sys.version_info < (3, 9))" >/dev/null 2>&1; then
  python3 scripts/ci/check_ci_contract.py
elif command -v python >/dev/null 2>&1 \
     && python -c "import sys; raise SystemExit(sys.version_info < (3, 9))" >/dev/null 2>&1; then
  python scripts/ci/check_ci_contract.py
elif command -v py >/dev/null 2>&1 \
     && py -3 -c "import sys; raise SystemExit(sys.version_info < (3, 9))" >/dev/null 2>&1; then
  py -3 scripts/ci/check_ci_contract.py
else
  echo "Python 3 is required for the static CI wiring contract." >&2
  exit 1
fi

run_suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"
run_suffix="$(printf '%s' "$run_suffix" | tr -cd '[:alnum:]_.-')"
readonly MSSQL_CONTAINER_NAME="sql-dw-ci-${run_suffix}"

credentials_file="$(mktemp)"
readiness_log="$(mktemp)"
bronze_log="$(mktemp)"
negative_log="$(mktemp)"
docker_credentials_file="$credentials_file"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) docker_credentials_file="$(cygpath -w "$credentials_file")" ;;
esac
container_id=""

cleanup() {
  local exit_code=$?
  trap - EXIT
  if [[ -n "$container_id" ]]; then
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  fi
  rm -f "$credentials_file" "$readiness_log" "$bronze_log" "$negative_log"
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n "${MSSQL_SA_PASSWORD:-}" ]]; then
  generated_password="$MSSQL_SA_PASSWORD"
else
  generated_password="Aa1!$(openssl rand -hex 24)"
fi

if [[ "$generated_password" == *$'\n'* || "$generated_password" == *$'\r'* ]]; then
  echo "MSSQL_SA_PASSWORD must not contain newline characters." >&2
  exit 1
fi

if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  echo "::add-mask::$generated_password"
fi

chmod 600 "$credentials_file"
{
  printf 'ACCEPT_EULA=Y\n'
  printf 'MSSQL_PID=Express\n'
  printf 'MSSQL_SA_PASSWORD=%s\n' "$generated_password"
  printf 'SQLCMDPASSWORD=%s\n' "$generated_password"
} > "$credentials_file"
unset generated_password MSSQL_SA_PASSWORD

if docker container inspect "$MSSQL_CONTAINER_NAME" >/dev/null 2>&1; then
  echo "Refusing to reuse an existing container: $MSSQL_CONTAINER_NAME" >&2
  exit 1
fi

echo "Starting isolated SQL Server container."
container_id="$(docker create \
  --name "$MSSQL_CONTAINER_NAME" \
  --hostname "$MSSQL_CONTAINER_NAME" \
  --env-file "$docker_credentials_file" \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --mount "type=bind,src=${repo_root}/datasets,dst=/datasets,readonly" \
  --security-opt no-new-privileges \
  "$MSSQL_IMAGE")"
docker start "$container_id" >/dev/null

expected_bronze_crm_cust_info="$(awk 'END { print NR - 1 }' datasets/source_crm/cst_info.csv)"
expected_bronze_crm_prd_info="$(awk 'END { print NR - 1 }' datasets/source_crm/prd_info.csv)"
expected_bronze_crm_sales_details="$(awk 'END { print NR - 1 }' datasets/source_crm/sales_details.csv)"
expected_bronze_erp_cust_az12="$(awk 'END { print NR - 1 }' datasets/source_erp/CST_AZ12.csv)"
expected_bronze_erp_loc_a101="$(awk 'END { print NR - 1 }' datasets/source_erp/LOC_A101.csv)"
expected_bronze_erp_px_cat_g1v2="$(awk 'END { print NR - 1 }' datasets/source_erp/PX_CAT_G1V2.csv)"

sqlcmd_variables=(
  -v "BasePath=/datasets"
  -v "SourceVersion=ci-reviewed-synthetic-snapshot-v1"
  -v "SourceWatermark=1"
  -v "MaxRejectRows=23"
  -v "RestartOfBatchId=0"
  -v "SnapshotAsOf=2024-12-31"
  -v "EXPECTED_BRONZE_CRM_CUST_INFO=${expected_bronze_crm_cust_info}"
  -v "EXPECTED_BRONZE_CRM_PRD_INFO=${expected_bronze_crm_prd_info}"
  -v "EXPECTED_BRONZE_CRM_SALES_DETAILS=${expected_bronze_crm_sales_details}"
  -v "EXPECTED_BRONZE_ERP_CUST_AZ12=${expected_bronze_erp_cust_az12}"
  -v "EXPECTED_BRONZE_ERP_LOC_A101=${expected_bronze_erp_loc_a101}"
  -v "EXPECTED_BRONZE_ERP_PX_CAT_G1V2=${expected_bronze_erp_px_cat_g1v2}"
  -v "TestBasePath=/datasets"
  -v "ConfirmRuntimeTests=RUN_RUNTIME_TESTS_ON_DISPOSABLE_DATABASE"
)

parse_sql_file() {
  local sql_file=$1
  echo "Parsing ${sql_file}"
  {
    printf 'SET PARSEONLY ON;\n'
    cat "$sql_file"
  } | docker exec --interactive --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
    "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 60 \
    "${sqlcmd_variables[@]}" -d master
}

for attempt in {1..60}; do
  if docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
      "$SQLCMD" -S localhost -U sa -C -b -l 5 -t 5 \
      -Q "SET NOCOUNT ON; SELECT 1;" >"$readiness_log" 2>&1; then
    break
  fi
  if [[ "$attempt" -eq 60 ]]; then
    echo "SQL Server did not become ready within 120 seconds." >&2
    tail -n 20 "$readiness_log" >&2 || true
    exit 1
  fi
  sleep 2
done

run_sql() {
  local database=$1
  local sql_file=$2
  shift 2
  echo "Running ${sql_file}"
  docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
    "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 600 \
    "${sqlcmd_variables[@]}" \
    "$@" \
    -d "$database" -i "/workspace/${sql_file}"
}

expect_sql_failure() {
  local database=$1
  local sql_file=$2
  local label=$3
  local expected_marker=$4
  if docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
      "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 600 \
      "${sqlcmd_variables[@]}" \
      -d "$database" -i "/workspace/${sql_file}" >"$negative_log" 2>&1; then
    cat "$negative_log"
    echo "Negative contract unexpectedly passed: ${label}" >&2
    exit 1
  fi
  cat "$negative_log"
  if ! grep -Fq "$expected_marker" "$negative_log"; then
    echo "Negative contract failed for an unexpected reason: ${label}" >&2
    exit 1
  fi
  echo "Negative contract rejected as expected: ${label}"
}

warning_count() {
  local log_file=$1
  local marker=$2
  local value
  value="$(grep -F "$marker" "$log_file" | tail -n 1 | sed 's/.*count=//' || true)"
  if [[ -z "$value" ]]; then
    printf '0\n'
  elif [[ "$value" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$value"
  else
    echo "Diagnostic warning count is not numeric for marker: $marker" >&2
    exit 1
  fi
}

run_sql master scripts/ci/run_ci_pipeline.sql
for sql_file in tests/ci_*.sql tests/quality_checks_*.sql tests/runtime_*.sql tests/model_*.sql tests/powerbi_*.sql; do
  parse_sql_file "$sql_file"
done
run_sql DataWarehouse tests/ci_pipeline_contract.sql
run_sql DataWarehouse tests/runtime_contract.sql
run_sql DataWarehouse tests/runtime_silver_coverage.sql
run_sql DataWarehouse tests/runtime_fail_closed.sql
run_sql DataWarehouse tests/runtime_break_downstream_contract.sql
expect_sql_failure DataWarehouse tests/runtime_run_downstream_failure.sql \
  "Gold downstream publication" \
  "CK_runtime_downstream_failure"
run_sql DataWarehouse tests/runtime_restore_downstream_contract.sql
downstream_failed_batch_id="$(docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
  "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 60 -h -1 -W \
  -d DataWarehouse -Q "SET NOCOUNT ON; SELECT TOP (1) batch_id FROM control.pipeline_batch WHERE source_version=N'ci-downstream-failure-v1' AND status='FAILED' ORDER BY batch_id DESC;" \
  | tr -d '[:space:]')"
if [[ ! "$downstream_failed_batch_id" =~ ^[0-9]+$ ]]; then
  echo "Could not resolve the failed downstream batch for linked restart." >&2
  exit 1
fi
run_sql DataWarehouse tests/runtime_restart_downstream.sql -v "RestartOfBatchId=${downstream_failed_batch_id}"
run_sql DataWarehouse tests/runtime_verify_downstream_restart.sql
run_sql DataWarehouse tests/runtime_idempotency.sql
run_sql DataWarehouse tests/runtime_atomicity.sql
run_sql DataWarehouse tests/runtime_full_success_invariant.sql

docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
  "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 120 \
  "${sqlcmd_variables[@]}" \
  -d DataWarehouse -i /workspace/tests/quality_checks_bronze.sql | tee "$bronze_log"
baseline_product_cost_count="$(warning_count "$bronze_log" "WARNING (Bronze): null_or_negative_product_cost count=")"
run_sql DataWarehouse tests/quality_checks_silver.sql
run_sql DataWarehouse tests/quality_checks_gold.sql

run_sql DataWarehouse tests/ci_break_bronze_diagnostic.sql
docker exec --workdir /workspace --env-file "$docker_credentials_file" "$MSSQL_CONTAINER_NAME" \
  "$SQLCMD" -S localhost -U sa -C -b -l 15 -t 120 \
  "${sqlcmd_variables[@]}" \
  -d DataWarehouse -i /workspace/tests/quality_checks_bronze.sql | tee "$bronze_log"
mutated_product_cost_count="$(warning_count "$bronze_log" "WARNING (Bronze): null_or_negative_product_cost count=")"
if [[ "$mutated_product_cost_count" -ne $((baseline_product_cost_count + 1)) ]]; then
  echo "Bronze diagnostic self-test did not increase the targeted warning count by one." >&2
  exit 1
fi
run_sql DataWarehouse tests/ci_restore_bronze_diagnostic.sql

run_sql DataWarehouse tests/ci_break_silver_contract.sql
expect_sql_failure DataWarehouse tests/quality_checks_silver.sql \
  "Silver duplicate customer" \
  "ERROR (Silver): customer IDs must be non-null and unique."
run_sql DataWarehouse tests/ci_restore_silver_contract.sql
run_sql DataWarehouse tests/quality_checks_silver.sql

run_sql DataWarehouse tests/ci_break_gold_contract.sql
expect_sql_failure DataWarehouse tests/quality_checks_gold.sql \
  "Gold missing current product" \
  "ERROR (Gold): product current flags or effective intervals are inconsistent."
run_sql DataWarehouse tests/ci_restore_gold_contract.sql
run_sql DataWarehouse tests/quality_checks_gold.sql

run_sql DataWarehouse tests/model_schema_contract.sql
run_sql DataWarehouse tests/model_data_quality.sql
run_sql DataWarehouse tests/model_reproducibility.sql
run_sql DataWarehouse tests/model_sentinel_contract.sql
run_sql DataWarehouse tests/model_decimal_arithmetic.sql
run_sql DataWarehouse tests/model_scd2_reconciliation.sql
run_sql DataWarehouse tests/powerbi_rls_data_contract.sql
run_sql DataWarehouse tests/source_inventory/run_tests_ci.sql
run_sql DataWarehouse tests/quality_checks_ci.sql
echo "CI runtime, pipeline, model, Inventory, quality contracts, and negative self-tests passed."
