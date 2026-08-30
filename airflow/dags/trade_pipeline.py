"""Orchestrate the trade pipeline: generate a controlled batch, load it into
Snowflake, then run the dbt transformation and tests.

    generate_trades  >>  ingest_trades  >>  dbt_build  >>  notify_failure

Airflow only orchestrates. The generator, the PUT/COPY INTO loader, and the
business rules stay exactly where they already live:

    data_generator/generate_trades.py   generates the JSON batches
    data_generator/ingest_trades.py     PUT + COPY INTO into RAW.RAW_TRADES
    dbt/                                BRONZE -> SILVER -> GOLD
    monitoring/send_failure_alert.py    emails through Snowflake on failure

FAILURE NOTIFICATION
--------------------
notify_failure runs with trigger_rule="one_failed", so it fires only when one of
the three pipeline tasks has failed. It asks Snowflake to send the email, so no
SMTP server or external notification service is involved.

It then exits non-zero on purpose. Airflow derives a DAG run's state from its
leaf tasks, and notify_failure is the leaf: if it succeeded, a run whose dbt
build had failed would be reported as successful. Failing it keeps the run red,
which is the truth. Its exit code says nothing about whether the email was sent -
that is in the task log.

Both Python scripts are CLIs with argparse and non-zero exit codes, so
BashOperator is the natural fit. It also lets each task run against the isolated
virtual environment built in airflow/Dockerfile, keeping dbt and the Snowflake
connector out of Airflow's own Python environment.

RE-RUN BEHAVIOUR
----------------
Generation is pinned to the run's logical date ({{ ds }}) and a fixed seed, so
re-running a given DAG run reproduces byte-identical files. The loader uses
COPY INTO with FORCE = FALSE, so Snowflake's load history skips a file it has
already loaded.

  - Retrying a failed ingest task     safe: a committed load is skipped, a
                                      failed one committed nothing.
  - Re-running the same DAG run       safe: identical bytes, load history skips.
  - Running on a different day        {{ ds }} changes, so the file content
                                      changes and Snowflake reloads it. RAW
                                      grows by one batch carrying the same
                                      trade_ids at the same versions; dbt treats
                                      those as same-version re-sends (rule 2),
                                      so TRADE_STORE stays stable while RAW and
                                      SILVER grow. Known limitation: the staged
                                      file name is always trades_batch_NNN.json.gz.
  - force_reload = true               deliberately reloads, which DOES duplicate
                                      rows in RAW. Off by default.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


# Paths inside the container. See the volume mounts in docker-compose.yml.
PROJECT_DIR = "/opt/airflow/project"
VENV_PYTHON = "/opt/pipeline_venv/bin/python"
VENV_DBT = "/opt/pipeline_venv/bin/dbt"


with DAG(
    dag_id="trade_pipeline",
    description="Generate a controlled trade batch, load it into Snowflake, run dbt.",
    start_date=datetime(2026, 1, 1),
    # Manual trigger: cloning this repository must never write to someone's
    # Snowflake account unprompted. Change to "@daily" for a scheduled batch run.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "batch": Param(
            "001",
            type="string",
            description="Which generated batch to ingest, e.g. 001.",
        ),
        "seed": Param(
            42,
            type="integer",
            description="Generator seed. A fixed seed keeps the batch reproducible.",
        ),
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

    generate_trades = BashOperator(
        task_id="generate_trades",
        doc_md="Write the four deterministic JSON batches to data/.",
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/data_generator/generate_trades.py "
            "--seed {{ params.seed }} "
            "--as-of-date {{ ds }}"
        ),
    )

    ingest_trades = BashOperator(
        task_id="ingest_trades",
        doc_md="PUT the selected batch to the named stage, then COPY INTO RAW.RAW_TRADES.",
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/data_generator/ingest_trades.py "
            "--batch {{ params.batch }}"
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

    notify_failure = BashOperator(
        task_id="notify_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
        # A notification must not be retried into a flood of duplicate emails.
        retries=0,
        doc_md=(
            "Runs only when a pipeline task has failed. Asks Snowflake to email "
            "the failure through the TRADE_ALERT_EMAIL integration, then exits "
            "non-zero so the DAG run is not reported as successful."
        ),
        bash_command=(
            f"{VENV_PYTHON} {PROJECT_DIR}/monitoring/send_failure_alert.py "
            "--dag-id {{ dag.dag_id }} "
            "--run-id {{ run_id }} "
            "--logical-date {{ ds }}"
        ),
    )

    generate_trades >> ingest_trades >> dbt_build

    # Wired to all three rather than chained after dbt_build, so ONE_FAILED sees
    # the task that actually failed instead of a downstream upstream_failed one.
    # On a successful run notify_failure is skipped, and a skipped leaf leaves the
    # run green.
    [generate_trades, ingest_trades, dbt_build] >> notify_failure
