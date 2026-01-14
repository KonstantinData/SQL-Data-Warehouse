"""DuckDB runtime helpers for executing SQL files locally."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Iterator

import duckdb

from ._inputs import BRONZE_LOADS, INPUTS


@dataclass(frozen=True)
class SqlJob:
    name: str
    sql_path: Path
    job_type: str
    output_layer: str | None = None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def normalize_sql(sql: str) -> str:
    sql = sql.replace("\ufeff", "")
    sql = re.sub(r"^\s*GO\s*$", "", sql, flags=re.IGNORECASE | re.MULTILINE)
    sql = re.sub(r"^\s*USE\s+\w+\s*;\s*$", "", sql, flags=re.IGNORECASE | re.MULTILINE)
    sql = re.sub(r"^\s*SET\s+NOCOUNT\s+ON\s*;\s*$", "", sql, flags=re.IGNORECASE | re.MULTILINE)
    sql = sql.replace("DataWarehouse.", "")

    sql = re.sub(r"\bNVARCHAR\b", "VARCHAR", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bDATETIME\b", "TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bBIT\b", "BOOLEAN", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bGETDATE\s*\(\s*\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bISNULL\s*\(", "COALESCE(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bLEN\s*\(", "LENGTH(", sql, flags=re.IGNORECASE)

    sql = re.sub(
        r"IF\s+OBJECT_ID\s*\(\s*'([^']+)'\s*,\s*'U'\s*\)\s*IS\s*NOT\s*NULL\s*\n?\s*DROP\s+TABLE\s+\1\s*;",
        r"DROP TABLE IF EXISTS \1;",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"IF\s+OBJECT_ID\s*\(\s*'([^']+)'\s*,\s*'V'\s*\)\s*IS\s*NOT\s*NULL\s*\n?\s*DROP\s+VIEW\s+\1\s*;",
        r"DROP VIEW IF EXISTS \1;",
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        r"IF\s+NOT\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+INFORMATION_SCHEMA\.COLUMNS\s+WHERE\s+TABLE_SCHEMA\s*=\s*'([^']+)'\s+AND\s+TABLE_NAME\s*=\s*'([^']+)'\s+AND\s+COLUMN_NAME\s*=\s*'([^']+)'\s*\)\s*BEGIN\s*ALTER\s+TABLE\s+([^\s]+)\s+ADD\s+([^;]+);\s*END",
        r"ALTER TABLE \4 ADD COLUMN IF NOT EXISTS \5;",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    sql = re.sub(
        r"CONVERT\(DATE,\s*CONVERT\(CHAR\(8\),\s*NULLIF\(([^,]+),\s*0\)\)\)",
        r"STRPTIME(CAST(NULLIF(\1, 0) AS VARCHAR), '%Y%m%d')::DATE",
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        r"CONVERT\(VARCHAR\(64\),\s*HASHBYTES\('SHA2_256',\s*([^\)]+)\),\s*2\)",
        r"lower(hex(sha256(\1)))",
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(r"\bCREATE\s+DATABASE\b.+?;", "", sql, flags=re.IGNORECASE | re.DOTALL)
    sql = re.sub(r"\bDROP\s+DATABASE\b.+?;", "", sql, flags=re.IGNORECASE | re.DOTALL)
    sql = re.sub(r"\bALTER\s+DATABASE\b.+?;", "", sql, flags=re.IGNORECASE | re.DOTALL)

    sql = re.sub(r"\bCREATE\s+OR\s+ALTER\s+PROCEDURE\b.+?\bEND\b", "", sql, flags=re.IGNORECASE | re.DOTALL)

    sql = re.sub(r"\bEXECUTE\s+bronze\.load_bronze\b.*?;", "", sql, flags=re.IGNORECASE)

    return sql


def split_statements(sql: str) -> list[str]:
    statements = []
    current = []
    in_string = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'":
            current.append(char)
            if in_string and i + 1 < len(sql) and sql[i + 1] == "'":
                current.append("'")
                i += 1
            else:
                in_string = not in_string
        elif char == ";" and not in_string:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        i += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def extract_expectations(sql: str) -> list[tuple[str, bool]]:
    statements = []
    buffer = []
    expect_no_rows = False
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") and "Expectation:" in stripped:
            expect_no_rows = "No Results" in stripped or "No Invalid" in stripped or "No NULL" in stripped
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer)
            cleaned = statement.strip()
            if cleaned:
                statements.append((cleaned.rstrip(";"), expect_no_rows))
            buffer = []
            expect_no_rows = False
    if buffer:
        cleaned = "\n".join(buffer).strip()
        if cleaned:
            statements.append((cleaned, expect_no_rows))
    return statements


def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path) if db_path else ":memory:")


def ensure_schemas(conn: duckdb.DuckDBPyConnection, schemas: Iterable[str]) -> None:
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def register_inputs(conn: duckdb.DuckDBPyConnection) -> None:
    ensure_schemas(conn, {name.split(".")[0] for name in INPUTS})
    root = repo_root()
    for view_name, rel_path in INPUTS.items():
        schema, table = view_name.split(".")
        path = root / rel_path
        conn.execute(
            "CREATE OR REPLACE VIEW {}.{} AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);".format(
                schema,
                table,
            ),
            [str(path)],
        )


def execute_statements(conn: duckdb.DuckDBPyConnection, statements: Iterable[str]) -> None:
    for statement in statements:
        cleaned = statement.strip()
        if cleaned:
            conn.execute(cleaned)


def run_sql_file(conn: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    normalized = normalize_sql(sql)
    statements = split_statements(normalized)
    execute_statements(conn, statements)


def run_quality_checks(conn: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    normalized = normalize_sql(sql)
    for statement, expect_no_rows in extract_expectations(normalized):
        result = conn.execute(statement).fetchall()
        if expect_no_rows and len(result) > 0:
            raise SystemExit(1)


def run_ci_quality_checks(
    conn: duckdb.DuckDBPyConnection,
    sql_path: Path,
    bronze_checks: Path,
    silver_checks: Path,
    gold_checks: Path,
) -> None:
    _ = sql_path.read_text(encoding="utf-8")
    for path in (bronze_checks, silver_checks, gold_checks):
        run_quality_checks(conn, path)


def load_bronze_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for target_table, source_view in BRONZE_LOADS.items():
        conn.execute(f"DELETE FROM {target_table};")
        conn.execute(f"INSERT INTO {target_table} SELECT * FROM {source_view};")


def export_schema(conn: duckdb.DuckDBPyConnection, schema: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    entries = conn.execute(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ?
        ORDER BY table_name
        """,
        [schema],
    ).fetchall()
    for table_name, table_type in entries:
        output_path = output_dir / f"{table_name}.parquet"
        conn.execute(
            "COPY (SELECT * FROM {}.{}) TO ? (FORMAT 'parquet');".format(schema, table_name),
            [str(output_path)],
        )


def run_job(job: SqlJob) -> None:
    conn = connect()
    ensure_schemas(conn, ["bronze", "silver", "gold", "raw"])
    register_inputs(conn)

    if job.job_type == "init":
        _ = job.sql_path.read_text(encoding="utf-8")
        ensure_schemas(conn, ["bronze", "silver", "gold"])
        return

    if job.job_type == "bronze_load":
        _ = job.sql_path.read_text(encoding="utf-8")
        load_bronze_tables(conn)
        if job.output_layer:
            export_schema(conn, job.output_layer, repo_root() / "artifacts" / job.output_layer)
        return

    if job.job_type == "quality":
        run_quality_checks(conn, job.sql_path)
        return

    run_sql_file(conn, job.sql_path)
    if job.job_type == "transform" and job.output_layer:
        export_schema(conn, job.output_layer, repo_root() / "artifacts" / job.output_layer)
