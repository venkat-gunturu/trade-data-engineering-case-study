import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.email import send_email


PROJECT_DIR = "/opt/airflow/project"
VENV_PYTHON = "/opt/pipeline_venv/bin/python"
VENV_DBT = "/opt/pipeline_venv/bin/dbt"

ALERT_EMAIL = os.environ.get("ALERT_EMAIL_RECIPIENT", "").strip()
ALERT_EMAILS = [ALERT_EMAIL] if ALERT_EMAIL else []


sys.path.insert(0, f"{PROJECT_DIR}/data_generator")
from file_lifecycle import discover_files


def find_files_to_process(**_):

    files = [str(path) for path in discover_files()]

    if not files:
        print("No files available for processing.")
        return []

    print(f"{len(files)} file(s) available for processing:")
    for path in files:
        print(f"  {path}")
    return files


def send_dag_success_email(context):
    """Sends an email notification when the entire DAG succeeds."""
    ingest = context["dag_run"].get_task_instance("ingest_trades")
    if not ALERT_EMAILS or ingest.state == "skipped":
        return

    dag_id = context["dag"].dag_id
    subject = f"Airflow Alert: DAG [{dag_id}] Succeeded!"
    body = f"""
    <h3>Trade pipeline run succeeded</h3>
    <p><b>DAG ID:</b> {dag_id}</p>
    <p><b>Run:</b> {context['run_id']}</p>
    <p>Files were ingested into Snowflake, dbt rebuilt BRONZE -> SILVER -> GOLD,
       and the processed files were archived.</p>
    """
    send_email(to=ALERT_EMAILS, subject=subject, html_content=body)

default_args = {
        "owner": "airflow",
        "start_date": datetime(2026, 1, 1),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email": ALERT_EMAILS,
        "email_on_failure": True,
        "email_on_retry": False,
    }

with DAG(
    dag_id="trade_pipeline",
    description="Ingest the trade files waiting in data/, run dbt, then archive them.",
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    on_success_callback=send_dag_success_email,
    params={
        "force_reload": Param(
            False,
            type="boolean",
            description=(
                "Pass --force to the loader, reloading a file Snowflake has already "
                "loaded. This DOES create duplicate rows in RAW - use only for a "
                "deliberate demonstration."
            ),
        ),
    },
    tags=["trade", "snowflake", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    find_files = ShortCircuitOperator(
        task_id="find_files",
        python_callable=find_files_to_process,
        doc_md=(
            "List the batch files directly under data/. Scanning is not recursive, "
            "so data/archive/ is never rediscovered. An empty result skips the rest "
            "of the run and the DAG finishes successfully."
        ),
    )

    discovered = "{{ ti.xcom_pull(task_ids='find_files') | join(' ') }}"

    ingest_trades = BashOperator(
        task_id="ingest_trades",
        doc_md="PUT each discovered file to the named stage, then COPY INTO RAW.RAW_TRADES.",
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/data_generator/ingest_trades.py "
            f"--file {discovered}"
            "{{ ' --force' if params.force_reload else '' }}"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        doc_md=(
            "Build BRONZE -> SILVER -> GOLD and run the dbt tests. `dbt build` "
            "interleaves each model with its own tests and fails on the first "
            "failure, so no separate test task is needed."
        ),
        bash_command=(
            f"cd {PROJECT_DIR}/dbt && {VENV_DBT} build --profiles-dir ."
        ),
    )

    archive_files = BashOperator(
        task_id="archive_files",
        doc_md=(
            "Move the processed files to data/archive/. Runs last, so a file is "
            "archived only once its rows are in Snowflake and dbt has rebuilt "
            "successfully."
        ),
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/data_generator/file_lifecycle.py "
            f"--file {discovered}"
        ),
    )

    start >> find_files >> ingest_trades >> dbt_build >> archive_files >> end
