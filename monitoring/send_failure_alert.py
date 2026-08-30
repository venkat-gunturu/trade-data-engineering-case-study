"""Send an email when a task in the trade pipeline fails.

Called by the `notify_failure` task in airflow/dags/trade_pipeline.py, which runs
with trigger_rule="one_failed" and therefore only when an upstream task has
already failed.

The email is sent by Snowflake through the TRADE_ALERT_EMAIL notification
integration created in monitoring/sql/monitoring.sql. Snowflake is the mail
sender, so the project needs no SMTP server and no external notification service.

Credentials are not handled here. load_env_file() and connect() are imported from
the loader, so the pipeline has exactly one credential mechanism.

WHY THIS SCRIPT ALWAYS EXITS NON-ZERO
-------------------------------------
Airflow decides a DAG run's state from its leaf tasks. `notify_failure` is the
leaf, so if it succeeded the run would be reported as successful even though an
upstream task had failed. Exiting non-zero keeps the run red, which is the truth.
The exit code says nothing about whether the email was sent - that is in the log.
This is the same reasoning behind the "watcher" pattern in Airflow's own system
test DAGs.

Usage:
    python send_failure_alert.py --dag-id trade_pipeline --run-id manual__... \
                                 --logical-date 2026-08-30
"""

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

# One credential mechanism for the whole project: reuse the loader's.
sys.path.insert(0, str(PROJECT_ROOT / "data_generator"))

from ingest_trades import connect, load_env_file  # noqa: E402

INTEGRATION = "TRADE_ALERT_EMAIL"
RECIPIENT_ENV_VAR = "ALERT_EMAIL_RECIPIENT"

# The pipeline had already failed before this script ran, so every path exits
# non-zero. The codes only distinguish the reason in `echo $?`.
EXIT_NOTIFIED = 1
EXIT_NOT_CONFIGURED = 2
EXIT_SEND_FAILED = 3


def build_message(args):
    """Return the (subject, body) describing the failed run."""
    subject = f"Trade pipeline FAILED: {args.dag_id}"
    body = (
        f"A task in the {args.dag_id} DAG failed.\n\n"
        f"  DAG            {args.dag_id}\n"
        f"  Run id         {args.run_id}\n"
        f"  Logical date   {args.logical_date}\n\n"
        "The Airflow UI shows which task failed and its log. For the Snowflake "
        "side of the same run:\n\n"
        "  SELECT * FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH ORDER BY day DESC;\n\n"
        "README section 15 lists the administrative queries that identify the "
        "failing file or dbt node."
    )
    return subject, body


def send_email(recipient, subject, body):
    """Ask Snowflake to send the notification. Raises on failure."""
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "CALL SYSTEM$SEND_EMAIL(%s, %s, %s, %s)",
                (INTEGRATION, recipient, subject, body),
            )
    finally:
        connection.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-id", default="trade_pipeline", help="DAG that failed.")
    parser.add_argument("--run-id", default="unknown", help="Airflow run id.")
    parser.add_argument("--logical-date", default="unknown", help="Run's logical date.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    load_env_file()

    subject, body = build_message(args)
    recipient = os.environ.get(RECIPIENT_ENV_VAR, "").strip()

    # A missing recipient is a configuration problem, and it is reported as one.
    # It is never allowed to look like a successful notification.
    if not recipient:
        print(
            f"WARNING: {RECIPIENT_ENV_VAR} is not set, so no failure email was sent.\n"
            f"         Set it in .env to a verified Snowflake account email and "
            f"apply monitoring/sql/monitoring.sql.\n"
            f"         The upstream task failure stands regardless - see the "
            f"Airflow UI.\n\n{subject}\n{body}",
            file=sys.stderr,
        )
        return EXIT_NOT_CONFIGURED

    try:
        send_email(recipient, subject, body)
    except Exception as error:  # noqa: BLE001 - the reason must reach the log verbatim
        print(
            f"WARNING: failure email to {recipient} could not be sent: {error}\n"
            f"         Check that TRADE_ALERT_EMAIL exists and that the address is "
            f"verified in Snowsight.\n"
            f"         The upstream task failure stands regardless.\n\n"
            f"{subject}\n{body}",
            file=sys.stderr,
        )
        return EXIT_SEND_FAILED

    print(f"Failure notification sent to {recipient} via {INTEGRATION}.")
    print(
        "Exiting non-zero on purpose: this task is the DAG's leaf, and a "
        "successful leaf would report the failed run as successful."
    )
    return EXIT_NOTIFIED


if __name__ == "__main__":
    sys.exit(main())
