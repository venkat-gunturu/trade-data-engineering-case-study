"""Tests for the Airflow failure-notification script.

No Snowflake account is needed: the connector is stubbed at import time and
send_email is monkeypatched, the same approach as tests/test_ingestion_errors.py.

What matters about this script is not that it can send mail - it is that it never
lets a notification problem make a failed pipeline look successful. Every test
here asserts some part of that contract.
"""

import sys
import types
from pathlib import Path

import pytest


def _stub_connector():
    """Let the script import without snowflake-connector-python present."""
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "monitoring"))

import send_failure_alert  # noqa: E402


ARGV = ["--dag-id", "trade_pipeline", "--run-id", "manual__2026-08-30", "--logical-date", "2026-08-30"]


@pytest.fixture
def recipient_set(monkeypatch):
    monkeypatch.setenv(send_failure_alert.RECIPIENT_ENV_VAR, "someone@example.com")


@pytest.fixture(autouse=True)
def never_read_the_real_env_file(monkeypatch):
    """.env is gitignored and machine-specific, so keep it out of the tests."""
    monkeypatch.setattr(send_failure_alert, "load_env_file", lambda *a, **k: None)


# --- the exit-code contract ------------------------------------------------


def test_a_successful_notification_still_exits_non_zero(monkeypatch, recipient_set):
    """The leaf task must stay red, or Airflow reports the failed run as a success."""
    monkeypatch.setattr(send_failure_alert, "send_email", lambda *args: None)

    assert send_failure_alert.main(ARGV) == send_failure_alert.EXIT_NOTIFIED
    assert send_failure_alert.EXIT_NOTIFIED != 0


def test_a_missing_recipient_is_reported_not_swallowed(monkeypatch, capsys):
    monkeypatch.delenv(send_failure_alert.RECIPIENT_ENV_VAR, raising=False)

    assert send_failure_alert.main(ARGV) == send_failure_alert.EXIT_NOT_CONFIGURED

    stderr = capsys.readouterr().err
    assert send_failure_alert.RECIPIENT_ENV_VAR in stderr
    # the alert content is still printed, so the failure is never lost
    assert "trade_pipeline" in stderr


def test_a_send_failure_does_not_crash_the_task(monkeypatch, recipient_set, capsys):
    """A broken integration must produce a logged warning, not a traceback."""
    def explode(*args):
        raise RuntimeError("integration TRADE_ALERT_EMAIL does not exist")

    monkeypatch.setattr(send_failure_alert, "send_email", explode)

    assert send_failure_alert.main(ARGV) == send_failure_alert.EXIT_SEND_FAILED
    assert "does not exist" in capsys.readouterr().err


def test_every_exit_path_is_non_zero():
    """There is no code path on which this script can report success."""
    codes = (
        send_failure_alert.EXIT_NOTIFIED,
        send_failure_alert.EXIT_NOT_CONFIGURED,
        send_failure_alert.EXIT_SEND_FAILED,
    )
    assert all(code != 0 for code in codes)
    assert len(set(codes)) == len(codes), "each reason needs a distinguishable code"


# --- message content -------------------------------------------------------


def test_the_message_identifies_the_run():
    args = send_failure_alert.parse_args(ARGV)
    subject, body = send_failure_alert.build_message(args)

    assert "trade_pipeline" in subject
    assert "manual__2026-08-30" in body
    assert "V_PIPELINE_HEALTH" in body, "the operator needs somewhere to look next"


def test_the_notification_uses_the_snowflake_integration(monkeypatch, recipient_set):
    """Snowflake sends the mail - there is no second credential or SMTP path."""
    captured = {}
    monkeypatch.setattr(
        send_failure_alert,
        "send_email",
        lambda recipient, subject, body: captured.update(recipient=recipient),
    )

    send_failure_alert.main(ARGV)

    assert captured["recipient"] == "someone@example.com"
    assert send_failure_alert.INTEGRATION == "TRADE_ALERT_EMAIL"
