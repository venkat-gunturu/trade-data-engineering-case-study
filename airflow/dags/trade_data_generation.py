from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "/opt/airflow/project"
VENV_PYTHON = "/opt/pipeline_venv/bin/python"


with DAG(
    dag_id="trade_data_generation",
    description="Generate synthetic trade batch files into data/.",
    start_date=datetime(2026, 1, 1),
    # Manual by default so cloning this repository never writes files unprompted.
    # Set a cron here to schedule it, e.g. "0 */2 * * *". A scheduled DAG can
    # still be triggered by hand at any time.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "seed": Param(
            42,
            type="integer",
            description="Generator seed. A fixed seed keeps the batch reproducible.",
        ),
        "records_per_batch": Param(
            200,
            type="integer",
            description="Trades per batch file.",
        ),
    },
    tags=["trade", "generation"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    generate_trades = BashOperator(
        task_id="generate_trades",
        doc_md="Write the next deterministic JSON batch file into data/.",
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/data_generator/generate_trades.py "
            "--seed {{ params.seed }} "
            "--records-per-batch {{ params.records_per_batch }} "
            "--as-of-date {{ ds }}"
        ),
    )

start >> generate_trades >> end