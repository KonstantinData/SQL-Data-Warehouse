"""Runner for tests/quality_checks_ci.sql."""

from pathlib import Path

from ._runtime import connect, ensure_schemas, register_inputs, run_ci_quality_checks


def main() -> None:
    conn = connect()
    ensure_schemas(conn, ["bronze", "silver", "gold", "raw"])
    register_inputs(conn)
    run_ci_quality_checks(
        conn,
        Path("tests/quality_checks_ci.sql"),
        Path("tests/quality_checks_bronze.sql"),
        Path("tests/quality_checks_silver.sql"),
        Path("tests/quality_checks_gold.sql"),
    )


if __name__ == "__main__":
    main()
