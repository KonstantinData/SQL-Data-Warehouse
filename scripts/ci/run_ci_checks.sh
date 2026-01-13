#!/usr/bin/env bash
set -euo pipefail

SA_PASSWORD=${SA_PASSWORD:-"YourStrong!Passw0rd1"}
MSSQL_IMAGE=${MSSQL_IMAGE:-"mcr.microsoft.com/mssql/server:2022-latest"}
MSSQL_TOOLS_IMAGE=${MSSQL_TOOLS_IMAGE:-"mcr.microsoft.com/mssql-tools:latest"}
MSSQL_CONTAINER_NAME=${MSSQL_CONTAINER_NAME:-"sql-dw-ci"}
START_CONTAINER=${START_CONTAINER:-"true"}
SQLCMD_SERVER=${SQLCMD_SERVER:-"localhost"}
SQLCMD_NETWORK=${SQLCMD_NETWORK:-"host"}

wait_for_sql_server() {
  for _ in {1..30}; do
    if docker run --rm --network "$SQLCMD_NETWORK" "$MSSQL_TOOLS_IMAGE" \
      /opt/mssql-tools/bin/sqlcmd -S "$SQLCMD_SERVER" -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1"; then
      return 0
    fi
    sleep 2
  done
  echo "SQL Server did not become available in time."
  return 1
}

if [[ "$START_CONTAINER" == "true" ]]; then
  if ! docker ps --format '{{.Names}}' | grep -qx "$MSSQL_CONTAINER_NAME"; then
    if docker ps -a --format '{{.Names}}' | grep -qx "$MSSQL_CONTAINER_NAME"; then
      docker rm -f "$MSSQL_CONTAINER_NAME"
    fi
    docker run -d --name "$MSSQL_CONTAINER_NAME" \
      -e ACCEPT_EULA=Y \
      -e MSSQL_PID=Express \
      -e SA_PASSWORD="$SA_PASSWORD" \
      -p 1433:1433 \
      "$MSSQL_IMAGE"
  fi
fi

wait_for_sql_server

MSSQL_CONTAINER=$(docker ps --filter "ancestor=$MSSQL_IMAGE" --format "{{.ID}}" | head -n 1)
if [[ -z "$MSSQL_CONTAINER" ]]; then
  echo "SQL Server container not found."
  exit 1
fi

docker cp datasets "$MSSQL_CONTAINER":/datasets
docker exec --user 0 "$MSSQL_CONTAINER" chmod -R a+rX /datasets

docker run --rm --network "$SQLCMD_NETWORK" -v "$PWD":/workspace "$MSSQL_TOOLS_IMAGE" \
  /opt/mssql-tools/bin/sqlcmd -S "$SQLCMD_SERVER" -U sa -P "$SA_PASSWORD" -C -b \
  -i /workspace/scripts/ci/run_ci_pipeline.sql

docker run --rm --network "$SQLCMD_NETWORK" -v "$PWD":/workspace "$MSSQL_TOOLS_IMAGE" \
  /opt/mssql-tools/bin/sqlcmd -S "$SQLCMD_SERVER" -U sa -P "$SA_PASSWORD" -C -b \
  -d DataWarehouse -i /workspace/tests/quality_checks_ci.sql
