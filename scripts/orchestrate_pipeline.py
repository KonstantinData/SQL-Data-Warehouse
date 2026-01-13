#!/usr/bin/env python3
"""Orchestrate SQL pipeline execution via sqlcmd."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SQL_FILES = [
    "scripts/init.database.sql",
    "scripts/bronze_layer/create_table_bronze_layer.SQL",
    "scripts/bronze_layer/bulk_insert_crm_cust_info.sql",
    "scripts/bronze_layer/bronze-load-bronze.sql",
    "scripts/silver_layer/create-silver-table-structure.sql",
    "scripts/silver_layer/cleansing_crm_cust_info.sql",
    "scripts/silver_layer/cleansing_crm_prd_info.sql",
]


def build_sqlcmd_args(
    sqlcmd_path: str,
    server: str,
    database: str,
    username: str | None,
    password: str | None,
    trusted_connection: bool,
) -> list[str]:
    args = [sqlcmd_path, "-S", server, "-d", database, "-b"]

    if trusted_connection:
        args.append("-E")
    else:
        if not username or not password:
            raise ValueError("Username and password are required when not using trusted connection.")
        args.extend(["-U", username, "-P", password])

    return args


def run_sql_file(sqlcmd_args: list[str], sql_file: Path) -> None:
    command = sqlcmd_args + ["-i", str(sql_file)]
    print(f"\n==> Running: {sql_file}")
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"sqlcmd failed for {sql_file} with exit code {result.returncode}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full SQL Data Warehouse pipeline using sqlcmd.",
    )
    parser.add_argument("--server", default="localhost", help="SQL Server host (default: localhost)")
    parser.add_argument("--database", default="master", help="Database context (default: master)")
    parser.add_argument("--sqlcmd-path", default="sqlcmd", help="Path to sqlcmd executable")
    parser.add_argument("--username", help="SQL login username")
    parser.add_argument("--password", help="SQL login password")
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Use Windows authentication (sqlcmd -E)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    sqlcmd_args = build_sqlcmd_args(
        sqlcmd_path=args.sqlcmd_path,
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        trusted_connection=args.trusted_connection,
    )

    for sql_file in SQL_FILES:
        run_sql_file(sqlcmd_args, repo_root / sql_file)

    print("\nPipeline execution completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
