"""Failure-path tests for the Snowflake loader.

These cover the checks ingest_trades.py makes *before* it talks to Snowflake, so
they need no credentials and no warehouse. The behaviour that does require
Snowflake - COPY INTO error handling, load history, and the type gate in BRONZE -
is verified by the Phase 6 end-to-end run and recorded in README section 20.

The Snowflake connector is stubbed at import time for the same reason.
"""

import argparse
import json
import sys
import types

import pytest


def _stub_connector():
    """Let ingest_trades import without snowflake-connector-python present."""
    if "snowflake.connector" in sys.modules:
        return
    snowflake = types.ModuleType("snowflake")
    connector = types.ModuleType("snowflake.connector")
    connector.Error = type("Error", (Exception,), {})
    connector.connect = lambda **kwargs: None
    snowflake.connector = connector
    sys.modules.setdefault("snowflake", snowflake)
    sys.modules.setdefault("snowflake.connector", connector)


_stub_connector()

import ingest_trades  # noqa: E402  (conftest puts data_generator on sys.path)


# --- input validation ------------------------------------------------------


def test_missing_batch_file_is_rejected_before_any_snowflake_call():
    missing = ingest_trades.DATA_DIR / "trades_batch_999999.json"
    with pytest.raises(ingest_trades.IngestionError, match="Batch file not found"):
        ingest_trades.ingest_file(cursor=None, local_file=missing, force=False)


def test_batch_id_is_derived_from_the_file_name():
    from pathlib import Path

    assert ingest_trades.batch_id_for(Path("trades_batch_001.json")) == "BATCH_001"
    assert ingest_trades.batch_id_for(Path("trades_batch_042.json")) == "BATCH_042"


def test_a_file_name_without_a_batch_number_is_rejected():
    from pathlib import Path

    with pytest.raises(ingest_trades.IngestionError, match="Cannot derive a batch id"):
        ingest_trades.batch_id_for(Path("trades.json"))


def test_resolve_files_rejects_an_empty_data_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest_trades, "DATA_DIR", tmp_path)
    with pytest.raises(ingest_trades.IngestionError, match="No batch files found"):
        ingest_trades.resolve_files(argparse.Namespace(file=None, batch=None))


# --- credentials -----------------------------------------------------------


def test_missing_credentials_are_reported_by_name(monkeypatch):
    for name in ingest_trades.REQUIRED_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ingest_trades.IngestionError) as excinfo:
        ingest_trades.connect()

    message = str(excinfo.value)
    assert "Missing required environment variables" in message
    # every missing variable is named, so the operator knows what to set
    for name in ingest_trades.REQUIRED_ENV_VARS:
        assert name in message


def test_real_environment_variables_win_over_the_env_file(tmp_path, monkeypatch):
    """load_env_file uses setdefault, so an exported value is never overwritten.

    This is what lets docker-compose inject credentials into the Airflow
    containers while the same .env sits on the host.
    """
    env_file = tmp_path / ".env"
    env_file.write_text("SNOWFLAKE_ROLE=FROM_FILE\n", encoding="utf-8")

    monkeypatch.setenv("SNOWFLAKE_ROLE", "FROM_ENVIRONMENT")
    ingest_trades.load_env_file(env_file)

    import os

    assert os.environ["SNOWFLAKE_ROLE"] == "FROM_ENVIRONMENT"


# --- COPY INTO result handling --------------------------------------------


class _FakeCursor:
    """Minimal stand-in exposing the two attributes rows_as_dicts uses."""

    def __init__(self, columns, rows):
        self.description = [(c,) for c in columns]
        self._rows = rows

    def fetchall(self):
        return self._rows

    def execute(self, sql):  # pragma: no cover - never asserted on
        self.last_sql = sql


def test_copy_into_errors_are_raised_not_swallowed(monkeypatch):
    """errors_seen > 0 must fail the task rather than report a partial load."""
    cursor = _FakeCursor(
        ["file", "status", "rows_loaded", "errors_seen", "first_error"],
        [("trades_batch_001.json.gz", "LOAD_FAILED", 0, 3, "bad value")],
    )
    monkeypatch.setattr(ingest_trades, "rows_as_dicts",
                        lambda c: [dict(zip([d[0].upper() for d in c.description], r))
                                   for r in c.fetchall()])

    with pytest.raises(ingest_trades.IngestionError, match="3 error"):
        ingest_trades.copy_into(cursor, "trades_batch_001.json.gz", "BATCH_001", force=False)


def test_a_load_history_skip_reports_zero_rows_rather_than_failing(monkeypatch):
    """Snowflake returns an informational row with no ROWS_LOADED when it skips."""
    cursor = _FakeCursor(["status"], [("Copy executed with 0 files processed.",)])
    monkeypatch.setattr(ingest_trades, "rows_as_dicts",
                        lambda c: [dict(zip([d[0].upper() for d in c.description], r))
                                   for r in c.fetchall()])

    assert ingest_trades.copy_into(cursor, "x.json.gz", "BATCH_001", force=False) == 0


# --- the malformed fixture -------------------------------------------------


def test_the_malformed_fixture_is_genuinely_invalid_json():
    """Guards the fixture used by the documented bad-input scenario.

    Snowflake's JSON parser is lenient enough to recover records from this file,
    which is the point: RAW is schema-on-read and accepts it. The pipeline fails
    later, at the BRONZE cast. See README section 20.
    """
    from pathlib import Path

    fixture = Path(__file__).parent / "fixtures" / "trades_batch_999.json"
    assert fixture.is_file(), "malformed fixture is missing"
    with pytest.raises(json.JSONDecodeError):
        json.loads(fixture.read_text(encoding="utf-8"))
