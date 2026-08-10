"""Unit contracts for the credential-safe canonical pipeline wrapper."""

from __future__ import annotations

import argparse
import importlib.util
import os
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "orchestrate_pipeline.py"
SPEC = importlib.util.spec_from_file_location("orchestrate_pipeline", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
orchestrator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(orchestrator)


def arguments(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "sqlcmd_path": "sqlcmd",
        "server": "localhost",
        "trusted_connection": False,
        "username": "warehouse_runner",
        "trust_server_certificate": False,
        "base_path": "/datasets",
        "source_version": "fixture-v1",
        "source_watermark": 1,
        "max_reject_rows": 23,
        "restart_of_batch_id": 0,
        "snapshot_as_of": "2024-12-31",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class PipelineArgumentTests(unittest.TestCase):
    def test_sql_auth_uses_environment_password_without_command_argument(self) -> None:
        with patch.dict(os.environ, {"SQLCMDPASSWORD": "secret"}, clear=True):
            command = orchestrator.build_sqlcmd_args(arguments(), Path("pipeline.sql"))

        self.assertIn("-U", command)
        self.assertNotIn("-P", command)
        self.assertNotIn("secret", command)
        self.assertIn("SourceVersion=fixture-v1", command)
        self.assertIn("SnapshotAsOf=2024-12-31", command)

    def test_trusted_connection_uses_integrated_authentication(self) -> None:
        args = arguments(trusted_connection=True, username=None)
        with patch.dict(os.environ, {}, clear=True):
            command = orchestrator.build_sqlcmd_args(args, Path("pipeline.sql"))

        self.assertIn("-E", command)
        self.assertNotIn("-U", command)
        self.assertNotIn("-P", command)

    def test_sql_auth_requires_environment_password(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SQLCMDPASSWORD"):
                orchestrator.build_sqlcmd_args(arguments(), Path("pipeline.sql"))

    def test_mutually_exclusive_authentication_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "either"):
            orchestrator.validate_args(arguments(trusted_connection=True))

    def test_invalid_operational_thresholds_are_rejected(self) -> None:
        for overrides in (
            {"source_watermark": 0},
            {"max_reject_rows": -1},
            {"restart_of_batch_id": -1},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    orchestrator.validate_args(arguments(**overrides))

    def test_snapshot_date_and_sqlcmd_literal_inputs_fail_closed(self) -> None:
        for overrides in (
            {"snapshot_as_of": "31.12.2024"},
            {"base_path": "/datasets'; DROP DATABASE DataWarehouse;--"},
            {"source_version": "fixture$(ESCAPE_SQUOTE(v1))"},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    orchestrator.validate_args(arguments(**overrides))


if __name__ == "__main__":
    unittest.main()
