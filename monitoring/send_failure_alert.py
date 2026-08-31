"""Send a pipeline-failure email through Snowflake, by hand.

A standalone CLI. The DAG does NOT call it: trade_pipeline reports failures with
Airflow's built-in email_on_failure over SMTP. This script is
the second, independent route - useful when SMTP is unavailable or unconfigured,
or to prove the Snowflake notification integration works without waiting for a
pipeline failure.

The email is sent by Snowflake through the TRADE_ALERT_EMAIL notification
integration created in monitoring/sql/monitoring.sql - the same integration the
ALERT_PIPELINE_FAILURE data alert uses. The recipient must therefore be a
VERIFIED email address of a user in the Snowflake account.

Credentials are not handled here. load_env_file() and connect() are imported from
the loader, so the pipeline has exactly one credential mechanism.

WHY EVERY EXIT CODE IS NON-ZERO
-------------------------------
This script only ever runs because something failed, so "success" would be a
misleading exit status: a caller that checks $? should see a failure, not a
report that the failure was successfully described. The codes below distinguish
only the reason - whether the email went out is in the output, not the code.

Usage:
    python send_failure_alert.py --dag-id trade_pipeline --run-id manual__... \
                                 --logical-date 2026-08-30 \
                                 --failed-tasks ingest_trades
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


def format_failed_tasks(raw):
    """Normalise the --failed-tasks value into a readable list.

    The DAG passes a comma-separated list read back from the run's own task
    instances. It can legitimately arrive empty, so an empty value is reported
    as unknown rather than as "no task failed" - this script only ever runs
    because something did fail.
    """
    tasks = [task.strip() for task in (raw or "").split(",") if task.strip()]
    return ", ".join(tasks) if tasks else "unknown (see the Airflow UI)"


def build_message(args):
    """Return the (subject, body) describing the failed run."""
    failed = format_failed_tasks(getattr(args, "failed_tasks", ""))

    subject = f"Trade pipeline FAILED: {args.dag_id} - {failed}"
    body = (
        f"A task in the {args.dag_id} DAG failed.\n\n"
        f"  DAG            {args.dag_id}\n"
        f"  Failed task    {failed}\n"
        f"  Run id         {args.run_id}\n"
        f"  Logical date   {args.logical_date}\n\n"
        "The Airflow UI shows the failed task's log. For the Snowflake side of "
        "the same run:\n\n"
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
    parser.add_argument(
        "--failed-tasks",
        default="",
        help="Comma-separated ids of the tasks that failed. The DAG fills this in.",
    )
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
        "Exiting non-zero on purpose: this script only runs because something "
        "failed, so a zero exit status would misreport that failure."
    )
    return EXIT_NOTIFIED


if __name__ == "__main__":
    sys.exit(main())
