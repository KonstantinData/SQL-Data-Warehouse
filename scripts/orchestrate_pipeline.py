#!/usr/bin/env python3
"""Run the canonical SQLCMD warehouse pipeline without exposing credentials."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def build_sqlcmd_args(args: argparse.Namespace, pipeline_file: Path) -> list[str]:
    command = [
        args.sqlcmd_path,
        "-S",
        args.server,
        "-d",
        "master",
        "-b",
        "-i",
        str(pipeline_file),
    ]

    if args.trusted_connection:
        command.append("-E")
    else:
        if not args.username:
            raise ValueError("--username is required unless --trusted-connection is used.")
        if "SQLCMDPASSWORD" not in os.environ:
            raise ValueError(
                "Set SQLCMDPASSWORD in the process environment; passwords are never accepted "
                "as command-line arguments."
            )
        command.extend(["-U", args.username])

    if args.trust_server_certificate:
        command.append("-C")

    command.extend(
        [
            "-v",
            f"BasePath={args.base_path}",
            f"SourceVersion={args.source_version}",
            f"SourceWatermark={args.source_watermark}",
            f"MaxRejectRows={args.max_reject_rows}",
            f"RestartOfBatchId={args.restart_of_batch_id}",
        ]
    )
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the audited CRM/ERP, Gold, and Inventory reference pipeline.",
    )
    parser.add_argument("--server", default="localhost", help="SQL Server name or address")
    parser.add_argument("--sqlcmd-path", default="sqlcmd", help="Path to sqlcmd")
    parser.add_argument("--username", help="SQL login; password must be in SQLCMDPASSWORD")
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Use Windows integrated authentication",
    )
    parser.add_argument(
        "--trust-server-certificate",
        action="store_true",
        help="Pass -C to sqlcmd for a trusted development endpoint",
    )
    parser.add_argument(
        "--base-path",
        required=True,
        help="Absolute dataset path visible to the SQL Server service",
    )
    parser.add_argument(
        "--source-version",
        required=True,
        help="Immutable identifier for the six-file CRM/ERP snapshot",
    )
    parser.add_argument(
        "--source-watermark",
        required=True,
        type=int,
        help="Positive monotonically increasing delivery sequence",
    )
    parser.add_argument("--max-reject-rows", type=int, default=23)
    parser.add_argument("--restart-of-batch-id", type=int, default=0)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.trusted_connection and args.username:
        raise ValueError("Choose either --trusted-connection or --username, not both.")
    if args.source_watermark < 1:
        raise ValueError("--source-watermark must be positive.")
    if args.max_reject_rows < 0:
        raise ValueError("--max-reject-rows must be zero or greater.")
    if args.restart_of_batch_id < 0:
        raise ValueError("--restart-of-batch-id must be zero or greater.")


def main() -> int:
    args = parse_args()
    try:
        validate_args(args)
        repo_root = Path(__file__).resolve().parents[1]
        pipeline_file = repo_root / "scripts" / "run_pipeline.sql"
        command = build_sqlcmd_args(args, pipeline_file)
        print("Running the fail-closed SQL Data Warehouse pipeline.")
        subprocess.run(command, cwd=repo_root, check=True)
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1

    print("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
