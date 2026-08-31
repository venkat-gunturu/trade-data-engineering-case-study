# Trade Data Engineering Case Study

An end-to-end batch data pipeline that ingests synthetic trade messages into Snowflake,
applies trade-lifecycle business rules with dbt, and maintains a current-state **trade store**
plus a rejection audit trail — orchestrated by Airflow, with infrastructure provisioned by
Terraform.

---

## 1. Project Overview

Trading systems re-send the same trade many times: an amendment bumps the version, a
retransmission repeats a version, and a stale message can arrive after a newer one. A
downstream store must therefore decide, for every incoming message, whether it represents the
trade's current state — and keep evidence of what it rejected.

This project builds that store end to end:

| Stage | What happens |
| --- | --- |
| **Generate** | A Python generator writes one JSON batch of ~200 synthetic trades into `data/`. Batches include higher-version amendments, same-version resends, stale lower versions, and already-matured trades to exercise the validation rules. |
| **Detect** | Airflow scans `data/` on a schedule. Nothing waiting → the run short-circuits and still finishes green. |
| **Land** | Each file is `PUT` to a Snowflake internal stage and `COPY INTO` `RAW.RAW_TRADES` as an unflattened `VARIANT` payload. A `LOAD_CONTROL` row records the file. |
| **Transform** | dbt builds `BRONZE → SILVER → GOLD`: typed projection, arrival sequencing, rule validation, then the trade store and rejection audit. |
| **Archive** | Only after ingestion *and* dbt succeed does the file move to `data/archive/`. |
| **Observe** | Airflow emails success/failure over SMTP; Snowflake exposes `V_PIPELINE_HEALTH` and an alert that mails through `SYSTEM$SEND_EMAIL`. |

The final deliverables are `GOLD.TRADE_STORE` — exactly one current row per `trade_id` — and
`GOLD.REJECTED_TRADES`, the compliance trail.

---

## 2. Architecture

```
                       ┌──────────────────────────────────────────┐
   Terraform  ────────▶│  Snowflake TRADE_DB (containers only)    │
   (one-off, local)    │  RAW · BRONZE · SILVER · GOLD ·          │
                       │  MONITORING · TRADE_WH · stage ·         │
                       │  file format · RAW_TRADES · LOAD_CONTROL │
                       └──────────────────────────────────────────┘

   DAG trade_data_generation (manual)
        generate_trades.py
              │  writes one batch
              ▼
        data/trades_batch_NNN.json
              │
   DAG trade_pipeline (*/30 * * * *)
              │
        find_files ──── empty ──▶ downstream skipped, run SUCCESS
              │ files found
              ▼
        ingest_trades ──▶  PUT    ──▶ RAW.TRADE_STAGE  (staged copy retained)
              │            COPY   ──▶ RAW.RAW_TRADES
              │            INSERT ──▶ MONITORING.LOAD_CONTROL  (status LOADED)
              ▼
        dbt_build  ──▶  BRONZE.STG_TRADES           typed projection
                        SILVER.INT_TRADE_SEQUENCE   arrival order
                        SILVER.INT_TRADE_VALIDATION ACCEPTED / REJECTED
                        GOLD.TRADE_STORE            incremental MERGE
                        GOLD.REJECTED_TRADES        incremental APPEND
              │
              ▼
        archive_files ──▶ data/archive/
              │
              ▼
   Monitoring / Notifications
        Airflow SMTP: success callback + email_on_failure (names the failed task)
        Snowflake:    MONITORING.V_PIPELINE_HEALTH  →  ALERT_PIPELINE_FAILURE
                      →  SYSTEM$SEND_EMAIL via TRADE_ALERT_EMAIL
```

A PlantUML source of the same flow lives at [docs/architecture.puml](docs/architecture.puml).

### Ownership boundaries

| Layer | Owns | Explicitly does **not** own |
| --- | --- | --- |
| **Terraform** | `TRADE_DB`; schemas `RAW`, `BRONZE`, `SILVER`, `GOLD`, `MONITORING`; warehouse `TRADE_WH`; `JSON_FILE_FORMAT`; internal stage `TRADE_STAGE`; tables `RAW.RAW_TRADES` and `MONITORING.LOAD_CONTROL` | Any dbt model; the monitoring view, alert or notification integration; any data |
| **Python** | Synthetic batch generation, `PUT`/`COPY INTO` ingestion, `LOAD_CONTROL` bookkeeping, the `data/ → data/archive/` file lifecycle | Orchestration, scheduling, transformation SQL |
| **Airflow** | Scheduling, file detection, task order, retries, success/failure email over SMTP | Ingestion logic (shells out to Python), transformation logic (shells out to dbt) |
| **Snowflake** | Storage and compute, staging, the `V_PIPELINE_HEALTH` view, `ALERT_PIPELINE_FAILURE`, `TRADE_ALERT_EMAIL` | Orchestration — no Streams, no Tasks |
| **dbt Core** | `STG_TRADES`, `INT_TRADE_SEQUENCE`, `INT_TRADE_VALIDATION`, `TRADE_STORE`, `REJECTED_TRADES` and all transformation tests | Anything Terraform creates |

---

## 3. Tech Stack

| Technology | Role in this project |
| --- | --- |
| **Terraform** (`snowflakedb/snowflake` v2.20) | Provisions every Snowflake *container* from an empty account — no manual clicking, no import step. Credentials come from the environment, never from a committed file. |
| **Apache Airflow** 2.11 (Docker, LocalExecutor) | Runs two DAGs: one to generate batches, one to detect → ingest → transform → archive. Also the notification layer: `email_on_failure` in `default_args` plus an `on_success_callback`. |
| **Snowflake** | The warehouse. `VARIANT` keeps `RAW` schema-on-read, the internal stage keeps the exact loaded bytes for inspection, and `ACCOUNT_USAGE` supplies monitoring with no extra instrumentation. |
| **dbt Core** 1.12 | Owns all transformation and all business-rule logic, layered `BRONZE → SILVER → GOLD`. `dbt build` interleaves each model with its own tests, so a bad model never leaves a half-built GOLD. |
| **Python** 3.12 | Generates deterministic, scenario-guaranteed trade data and performs ingestion. Kept out of the DAG file so orchestration and ingestion stay separable and unit-testable. |

---

## 4. Repository Structure

```
trade-data-engineering-case-study/
├── terraform/                  Snowflake infrastructure as code
│   ├── main.tf                 database, schemas, warehouse, file format, stage, tables
│   ├── variables.tf            non-secret naming/sizing only
│   ├── outputs.tf              fully-qualified names + the ownership boundary
│   └── providers.tf            provider config; credentials read from the environment
│
├── data_generator/             Python — generation, ingestion, file lifecycle
│   ├── trade_scenarios.py      scenario quotas + cross-batch trade state
│   ├── generate_trades.py      CLI: writes ONE batch per invocation
│   ├── ingest_trades.py        CLI: PUT → COPY INTO → LOAD_CONTROL
│   └── file_lifecycle.py       discover_files() / archive_files(); also a CLI
│
├── airflow/
│   ├── dags/trade_data_generation.py   manual DAG: write the next batch
│   ├── dags/trade_pipeline.py          scheduled DAG: find → ingest → dbt → archive
│   ├── Dockerfile              Airflow image + isolated /opt/pipeline_venv for dbt
│   └── requirements.txt        pinned deps for that venv
│
├── dbt/
│   ├── dbt_project.yml         folder → schema mapping (BRONZE/SILVER/GOLD)
│   ├── macros/                 generate_schema_name override (literal schema names)
│   ├── models/staging/         stg_trades + sources.yml
│   ├── models/intermediate/    int_trade_sequence, int_trade_validation, unit tests
│   ├── models/marts/           trade_store, rejected_trades, unit tests
│   └── tests/                  two singular cross-layer integrity tests
│
├── monitoring/
│   ├── sql/monitoring.sql      TRADE_ALERT_EMAIL, V_PIPELINE_HEALTH, ALERT_PIPELINE_FAILURE
│   └── send_failure_alert.py   standalone CLI: SYSTEM$SEND_EMAIL failure notice
│
├── tests/                      pytest — 65 tests over generator, loader, lifecycle, alerts
│   └── fixtures/               committed sample batch used by the loader tests
│
├── snowflake/sql/              historical hand-written DDL from the first phase.
│                               Superseded by Terraform; kept for reference only.
├── docs/architecture.puml      pipeline diagram (PlantUML source)
├── docs/case-study/            the original case-study brief
├── .github/workflows/          ci.yml (validate + isolated dbt build), deploy.yml
├── docker-compose.yml          local Airflow: postgres, init, scheduler, webserver
├── data/                       runtime landing area (gitignored) + data/archive/
├── .env.example                every environment variable, documented
└── requirements.txt            local dev environment
```

---

## 5. End-to-End Execution Flow

1. **Generate.** `generate_trades.py` writes **one** batch file — `data/trades_batch_NNN.json` —
   per invocation. `data/.generation_progress` tracks position in a four-batch cycle; after the
   fourth batch, `data/archive/` is cleared and the cycle restarts.
2. **Detect.** `find_files` (a `ShortCircuitOperator`) globs `data/trades_batch_*.json`
   *non-recursively*, so `data/archive/` is never rediscovered. It publishes the list as XCom;
   both `ingest_trades` and `archive_files` act on that exact snapshot. An empty list skips
   everything downstream and the run still ends **SUCCESS**.
3. **Stage.** `ingest_trades.py` runs `PUT ... OVERWRITE = TRUE, AUTO_COMPRESS = TRUE` to
   `RAW.TRADE_STAGE`. `OVERWRITE` matters because batch file names repeat every cycle — without
   it the second cycle's upload would be `SKIPPED` and silently load stale bytes.
4. **Load.** `COPY INTO RAW.RAW_TRADES` with `STRIP_OUTER_ARRAY`, so each array element becomes
   one row: `payload` (VARIANT), `batch_id`, `source_file_name`, `ingested_at`, `surrogate_key`.
   `ON_ERROR = ABORT_STATEMENT` — a bad file loads nothing.
5. **Record.** One row per file is inserted into `MONITORING.LOAD_CONTROL` with
   `status = 'LOADED'`. This is the handshake that tells dbt which files are new.
6. **Staged file retained.** The `.gz` copy stays on the stage after the load, so the exact bytes
   Snowflake read remain available for reconciliation. The next cycle overwrites it.
7. **Transform.** `dbt build` runs the five models and all tests. `stg_trades` reads only rows
   whose `source_file_name` is `LOADED`, then its post-hook flips those `LOAD_CONTROL` rows to
   `COMPLETED` — so the next run naturally picks up only the next batch.
8. **Trade store.** `GOLD.TRADE_STORE` is `MERGE`d on `trade_id`; `GOLD.REJECTED_TRADES` is
   appended to.
9. **Archive.** `archive_files` moves the processed files to `data/archive/`. It is the last task,
   so a file that failed ingestion or dbt stays in `data/` and the next run retries it. Nothing is
   ever deleted.
10. **Monitor.** `MONITORING.V_PIPELINE_HEALTH` derives one row per active day from
    `ACCOUNT_USAGE.COPY_HISTORY`, `ACCOUNT_USAGE.QUERY_HISTORY` and the live `TRADE_STORE`.
11. **Notify.** Airflow mails success (`on_success_callback`) and failure (`email_on_failure`,
    which names the failing task) over SMTP. `ALERT_PIPELINE_FAILURE` independently mails through
    Snowflake when the health view reports `DEGRADED`.

---

## 6. Step-by-Step Execution Guide

### Prerequisites

- A Snowflake account with a role that can create databases (`ACCOUNTADMIN` is used, because
  `ACCOUNT_USAGE` and the notification integration require it)
- Python 3.12
- Terraform ≥ 1.5
- Docker Desktop (for the Airflow runtime)
- SnowSQL or Snowflake Snowsight for executing/validating Snowflake SQL

### One-time setup

```bash
python -m venv .venv
.venv/Scripts/activate            # Windows;  source .venv/bin/activate on Linux/macOS
pip install -r requirements.txt

cp .env.example .env               # fill in real Snowflake + SMTP values
cp dbt/profiles.yml.example dbt/profiles.yml
```

`.env`, `dbt/profiles.yml` and `terraform.tfvars` are all gitignored. No credential is ever
committed.

### Environment configuration

Create a `.env` file in the project root based on `.env.example`:

```bash
cp .env.example .env

Add below values into the variables
------------------------------------------------------------------------------
| Variable              | Description                                        |
| --------------------- | -------------------------------------------------- |
| `SNOWFLAKE_ACCOUNT`   | Snowflake account identifier                       |
| `SNOWFLAKE_USER`      | Snowflake username                                 |
| `SNOWFLAKE_PASSWORD`  | Snowflake password                                 |
| `SNOWFLAKE_ROLE`      | Snowflake role used by the project                 |
| `SNOWFLAKE_DATABASE`  | Snowflake database name (`TRADE_DB`)               |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name (`TRADE_WH`)              |
| `SNOWFLAKE_SCHEMA`    | Snowflake schema used by the ingestion script      |
| `ALERT_EMAIL`         | Email address that receives pipeline notifications |
| `SMTP_HOST`           | SMTP server hostname                               |
| `SMTP_PORT`           | SMTP server port                                   |
| `SMTP_USER`           | SMTP username                                      |
| `SMTP_PASSWORD`       | SMTP password or app password                      |
------------------------------------------------------------------------------
> **Security:** Never commit `.env` or real credentials to Git. Use `.env.example` as the configuration template.
```

**SMTP_PASSWORD**: For gmail, 
https://myaccount.google.com/security → enable 2-Step Verification on gmail id.
https://myaccount.google.com/apppasswords → generate an App Password, name it e.g. airflow.
Google displays it as abcd efgh ijkl mnop. Paste it into .env with the spaces stripped — abcdefghijklmnop. 
Docker Compose does not strip them, and a value with spaces will fail auth.
.env is already gitignored and untracked (.gitignore:2), so the secret stays local.

#### Setup steps (do these once, before the first email will arrive)

1. **Enable 2-Step Verification** on the Gmail account:
   *myaccount.google.com → Security → 2-Step Verification*. App Passwords do
   not exist as an option until this is on.

2. **Generate a 16-character App Password**: *myaccount.google.com → Security →
   App passwords*. Google displays it grouped as `abcd efgh ijkl mnop`.
   **Remove the spaces** when pasting into `.env` — Compose passes the value
   through verbatim, and a password containing spaces fails authentication.
   Your normal Gmail password will NOT work; it fails with SMTP error `535`.

3. **Set `SMTP_MAIL_FROM` equal to `SMTP_USER`.** Gmail refuses to send with a
   `From` address the authenticated account does not own or have as a verified
   alias. An invented sender such as `airflow-alerts@gmail.com` produces
   `SMTPSenderRefused` even when the credentials are perfectly valid.


### Infrastructure — Terraform

#### Terraform environment configuration:

The Snowflake provider configuration in `terraform/providers.tf` is intentionally defined without hardcoded credentials or connection values. The required values are sourced from the project `.env` file through environment variables.

Before running **any Terraform command**, load the `.env` values into the current PowerShell session:

```powershell
cd terraform

Get-Content .env | Where-Object { $_ -and $_ -notmatch '^#' } | ForEach-Object { $name, $value = $_.Split('=', 2); Set-Content "env:$name" ($value -replace '^["'']|["'']$') }

To verify that a variable has been loaded, run:
$env:YOUR_VARIABLE_NAME
Example: $env:SNOWFLAKE_ACCOUNT

terraform init
terraform plan        # ~11 resources to add on a fresh account
terraform apply
cd ..
```

Terraform reads `SNOWFLAKE_ORGANIZATION_NAME`, `SNOWFLAKE_ACCOUNT_NAME`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PASSWORD` and `SNOWFLAKE_ROLE` from the environment (provider v2 replaced the single
account identifier with the org/account pair). `warehouse = ""` in `providers.tf` is deliberate:
Terraform issues DDL only, and inheriting `SNOWFLAKE_WAREHOUSE` would make it try to connect to
the very warehouse it is about to create.

### Data generation

```bash
python data_generator/generate_trades.py
python data_generator/generate_trades.py --seed 7 --records-per-batch 500
python data_generator/generate_trades.py --as-of-date 2026-06-01
```

One invocation writes one batch. The four-batch cycle is stateful: `BATCH_003` can contain a
stale version-1 message only because `BATCH_002` already pushed that trade to version 2. All
dates derive from the anchor date, so generated data never goes stale, and a fixed seed plus a
fixed anchor reproduces a run exactly.

### Airflow

```bash
docker compose build
docker compose up -d
docker compose ps          # postgres, airflow-scheduler, airflow-webserver healthy
# http://localhost:8080    (airflow / airflow by default)
```

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `trade_data_generation` | `None` (manual) | `start → generate_trades → end` |
| `trade_pipeline` | `*/30 * * * *` | `start → find_files → ingest_trades → dbt_build → archive_files → end` |

The two DAGs share only the `data/` directory — there is no sensor or trigger between them.
`trade_pipeline` runs with `retries=2` and `max_active_runs=1`, and exposes one parameter,
`force_reload`, which passes `--force` to the loader (this *does* create duplicate RAW rows — use
it only for a deliberate demonstration).

Stop with `docker compose down` (add `-v` to drop the Airflow metadata database).

### Monitoring objects — once, after apply

```bash
# Replace the single you@example.com placeholder, then run as ACCOUNTADMIN:
snowsql -f monitoring/sql/monitoring.sql
```

This creates `TRADE_ALERT_EMAIL`, `V_PIPELINE_HEALTH` and `ALERT_PIPELINE_FAILURE`. It must run
**after** `terraform apply` (Terraform owns the `MONITORING` schema) and **after**
`GOLD.TRADE_STORE` exists, because the view reads `TRADE_STORE` for its freshness column — run it
too early and the `CREATE VIEW` fails on a missing table. And `GOLD.TRADE_STORE` is created by dbt, so after a succesfull
airflow run execute this monitoring.sql

> **Manual prerequisite:** the recipient must be a *verified* email of a user in the Snowflake
> account. `SYSTEM$SEND_EMAIL` refuses unverified addresses.

### dbt

dbt does **not** read `.env`. Export the variables into your shell first, then:

```bash
cd dbt
dbt build --profiles-dir .        # 5 models + 45 data tests + 13 unit tests = 63 nodes
```

`profiles.yml` resolves every value through `env_var()`. `generate_schema_name.sql` is overridden
so `+schema: SILVER` lands models in `SILVER`, not dbt's default `BRONZE_SILVER`.

### Running the pipeline by hand, without Airflow

```bash
python data_generator/generate_trades.py
python data_generator/ingest_trades.py                        # everything waiting in data/
cd dbt && dbt build --profiles-dir . && cd ..
python data_generator/file_lifecycle.py --file data/trades_batch_001.json
```

### Validation

| What to check | How |
| --- | --- |
| File generation | `ls data/` — one new `trades_batch_NNN.json`; `cat data/.generation_progress` |
| Stage contents | `LIST @TRADE_DB.RAW.TRADE_STAGE;` — the `.gz` copy is retained on purpose |
| RAW | `SELECT batch_id, COUNT(*) FROM TRADE_DB.RAW.RAW_TRADES GROUP BY 1;` |
| Load bookkeeping | `SELECT * FROM TRADE_DB.MONITORING.LOAD_CONTROL ORDER BY ingested_at DESC;` |
| dbt models | `dbt build --profiles-dir .` — 69 nodes pass |
| Trade store | `SELECT COUNT(*), COUNT(DISTINCT trade_id) FROM TRADE_DB.GOLD.TRADE_STORE;` — the two must match |
| Rejections | `SELECT rejection_reason, COUNT(*) FROM TRADE_DB.GOLD.REJECTED_TRADES GROUP BY 1;` |
| Monitoring | `SELECT * FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH ORDER BY day DESC;` |
| Alert, on demand | `EXECUTE ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE;` then query `TABLE(TRADE_DB.INFORMATION_SCHEMA.ALERT_HISTORY())` |
| Notifications | Fail a run deliberately and check the inbox; or run `python monitoring/send_failure_alert.py --failed-tasks ingest_trades` |
| Archive | `ls data/archive/` after a green run; `data/` should be empty |
| Python tests | `pytest tests/ -q` — 65 passed |

---

## 7. Validation & Business Logic

Every message is classified and **nothing is discarded** — the audit trail stays complete.

### Where each rule is enforced

| Rule | Enforced in | What the code does |
| --- | --- | --- |
| **Past maturity** | `int_trade_validation` | `maturity_date < ingested_at::date` → `REJECTED` / `PAST_MATURITY`. Evaluated *first* in the `CASE`, so an intrinsic property of the record beats any comparison against stored state. |
| **Superseded within a batch** | `int_trade_sequence` + `int_trade_validation` | A window function over `(event_timestamp, surrogate_key)` per `trade_id` flags any record that a later arrival in the same batch supersedes → `REJECTED` / `LOWER_VERSION`. This also guarantees the GOLD `MERGE` sees at most one row per `trade_id`. |
| **Lower version across batches** | `trade_store` | The merge keeps a row only where the trade is new or `s.trade_version >= e.trade_version`. A stale message is simply not merged — the store keeps the newer version. |
| **Same version, resent** | `trade_store` | `>=` rather than `>`, so a re-sent version *replaces* the stored row. The generator deliberately mutates price and notional on a resend, so "replaced" is observable rather than indistinguishable from "ignored". |
| **Expiration** | `trade_store` | `status` is derived as expired once `maturity_date < current_date`, otherwise `VALID`. The row is never removed. |
| **Rejection audit** | `rejected_trades` | Appends every arrival whose version is below the version now stored, with `rejection_reason`, `batch_id`, `source_file_name`, `ingested_at` and `rejected_at`. |

### Worked example

One trade seen across a generation cycle:

| Batch | Arrives as | Outcome |
| --- | --- | --- |
| `BATCH_001` | v1, matures 2027 | New trade → inserted, `status = VALID` |
| `BATCH_002` | v2, amended price | `2 >= 1` → merged; the store now holds v2 |
| `BATCH_002` | v2 again, new price | `2 >= 2` → merged; the resent economics replace the stored row |
| `BATCH_003` | v1, stale retransmission | `1 >= 2` is false → not merged; the store still shows v2, and the arrival is appended to `REJECTED_TRADES` as `LOWER_VERSION` |
| later | maturity date passes | `status` flips to expired; the row stays |

A separate trade arriving with `maturity_date` already in the past is rejected on arrival as
`PAST_MATURITY` and never reaches the store.

### Model responsibilities

| Model | Schema | Materialization | Responsibility |
| --- | --- | --- | --- |
| `stg_trades` | `BRONZE` | table | Cast 13 fields out of the `VARIANT` payload. No business logic. Reads only files flagged `LOADED`; post-hook marks them `COMPLETED`. |
| `int_trade_sequence` | `SILVER` | table | Rebuild arrival order per `trade_id` with window functions; derive `prior_version` and `previously_established`. No rules applied. |
| `int_trade_validation` | `SILVER` | table | Assign `validation_status` (`ACCEPTED` / `REJECTED`) and `rejection_reason`. Filters nothing, so the audit trail is complete. |
| `trade_store` | `GOLD` | incremental (`merge`) | One current row per `trade_id`, with `status` and `updated_timestamp`. |
| `rejected_trades` | `GOLD` | incremental (`append`) | Immutable audit of superseded arrivals. |

---

## 8. Incremental Processing

Two mechanisms keep each run proportional to the new data, not the accumulated history.

**1. `LOAD_CONTROL` gates BRONZE.** `stg_trades` selects only RAW rows whose `source_file_name`
appears in `LOAD_CONTROL` with `status = 'LOADED'`; its post-hook then sets those rows to
`COMPLETED`. So even though `stg_trades` is a full table rebuild, it only ever holds the batch
that just landed. RAW keeps growing; the transformation cost does not.

**2. `GOLD.TRADE_STORE` is an incremental merge.**

```sql
materialized         = 'incremental'
incremental_strategy = 'merge'
unique_key           = 'trade_id'
```

- **First run** — `is_incremental()` is false, so the `existing_trades` CTE resolves to a typed
  `where false` block. Every incoming trade is treated as new, and the table is created with the
  correct column types.
- **Subsequent runs** — the model reads `{{ this }}` (the live store), left-joins the incoming
  batch to it on `trade_id`, and keeps a row only when the trade is new (`e.trade_id is null`) or
  the arriving version is not older (`s.trade_version >= e.trade_version`). dbt then `MERGE`s that
  result on `trade_id`: matched trades are updated in place, unmatched are inserted.

The pipeline therefore never re-reads the full RAW history to rebuild the store, and a re-run with
no new files is effectively a no-op. `REJECTED_TRADES` follows the same principle with an `append`
strategy plus a `not exists` guard on `(trade_id, trade_version)`, so a re-run cannot duplicate an
audit row.

---

## 9. Monitoring & Notifications

Two independent layers answer two different questions.

### "Did a task fail?" — Airflow, over SMTP

- `email_on_failure: True` sits in `default_args`, so every task inherits it and a task added
  later cannot be forgotten. Airflow's failure mail names the task that failed.
- With `retries=2`, the failure mail fires once after the **final** attempt, not per retry — a task
  with retries left goes to `up_for_retry`, not `failed`.
- `on_success_callback=send_dag_success_email` sends the success mail, but it first inspects the
  `ingest_trades` task instance and stays silent if that task was skipped. Without that check, a
  30-minute schedule with nothing to do would mail 48 times a day to say nothing happened.
- SMTP settings map from `.env` onto `AIRFLOW__SMTP__*` in `docker-compose.yml`.

### "Is the data wrong?" — Snowflake

Created by `monitoring/sql/monitoring.sql`, **not** by Terraform:

| Object | What it is |
| --- | --- |
| `TRADE_ALERT_EMAIL` | Email notification integration. `ALLOWED_RECIPIENTS` is deliberately omitted, so Snowflake permits any verified user email in the account and no personal address is committed. |
| `MONITORING.V_PIPELINE_HEALTH` | One row per day the pipeline was active: `files_loaded`, `rows_loaded`, `load_errors`, `dbt_nodes`, `dbt_failures`, `last_gold_refresh`, `status`. Built from `ACCOUNT_USAGE.COPY_HISTORY`, `ACCOUNT_USAGE.QUERY_HISTORY` (dbt-snowflake stamps every query with `"app": "dbt"` and a `node_id`, so no `query_tag` setup is needed) and the live `GOLD.TRADE_STORE`. `status = 'DEGRADED'` only when something ran **and failed**. |
| `MONITORING.ALERT_PIPELINE_FAILURE` | Fires when the last 24 hours contain a `DEGRADED` day, and calls `SYSTEM$SEND_EMAIL`. Created **suspended** on purpose — a resumed alert consumes credits at every evaluation. Demonstrate it with `EXECUTE ALERT`. |

`monitoring/send_failure_alert.py` is a standalone CLI on the Snowflake route
(`SYSTEM$SEND_EMAIL`), useful when SMTP is unavailable or to prove the integration works.
**The DAG never calls it.** Every one of its exit paths returns non-zero, because the script only
ever runs after something has already failed — a zero exit would misreport that failure.

**Why the monitoring objects are not Terraform-managed:** the view reads `ACCOUNT_USAGE`, which
makes it operational rather than structural, and it depends on `GOLD.TRADE_STORE`, which dbt owns.
Keeping it as plain SQL means a change to a monitoring query can never fail `terraform apply` or
block a deployment. Terraform owns the `MONITORING` *schema*; the view, the alert and the
integration inside it are not in Terraform state.

---

## 10. Testing Strategy

Tests target the logic that can actually be wrong, not every function in every file.

| Suite | Size | Focus |
| --- | --- | --- |
| `pytest tests/` | **65 tests** | Python behaviour, entirely offline — the Snowflake connector is stubbed at import and everything else runs in `tmp_path`, so CI needs no credentials. |
| `dbt build` | **69 nodes** (5 models, 46 data tests, 18 unit tests) | Business-rule correctness in SQL. |

**pytest breakdown**

| File | Tests | Proves |
| --- | --- | --- |
| `test_trade_generator.py` | 27 | Every rule scenario is present regardless of seed; a seed reproduces a run exactly; one batch per invocation; the cycle rolls over and clears `archive/`. |
| `test_file_lifecycle.py` | 19 | Discovery and archiving, plus design guards: `archive/` is never rediscovered, `PUT` overwrites, the staged file is retained, a load is recorded in the control table, the DAG hardcodes no batch number. |
| `test_ingestion_errors.py` | 9 | Loader failure paths: missing file, unparseable batch id, missing credentials named explicitly, `COPY` errors raised rather than swallowed. |
| `test_send_failure_alert.py` | 10 | A failed run can never look successful: non-zero exit on every path, missing recipient reported, a broken integration logged not raised, the failing task named. |

**dbt unit tests** carry the business logic: they feed hand-written rows into a model and assert
the output, so they are deterministic, need no warehouse data, and can construct states that
loading cannot reach — notably expiry, which requires elapsed time.

Three **singular tests** in `dbt/tests/` assert cross-layer integrity: that every staged trade is
classified exactly once, and that a rejection reason is present if and only if the record is
rejected.

---

## 11. CI/CD

| Workflow | Trigger | Does |
| --- | --- | --- |
| `ci.yml` → `validate` | every PR, push to `main` | `pytest`; `dbt parse` with dummy credentials (offline — it structurally cannot reach the warehouse); `terraform fmt -check`; `terraform init -backend=false && terraform validate`. |
| `ci.yml` → `dbt-integration` | same, non-fork | `CREATE DATABASE TRADE_DB_CI_<run_id> CLONE TRADE_DB` (zero-copy, instant) → `dbt build` against the clone → `DROP DATABASE` with `if: always()`. `TRADE_DB` is never read or written. |
| `deploy.yml` | `workflow_dispatch` only | `dbt build` against the real `TRADE_DB`, gated by the `production` GitHub Environment. |

Isolation needed no code change: `sources.yml` and `profiles.yml` both resolve the database from
`SNOWFLAKE_DATABASE`, so pointing that one variable at the clone moves the source *and* every
model target. **No workflow runs `terraform apply`** — a pull request structurally cannot change
infrastructure.

---

## 12. Key Design Decisions

| Decision | Rationale |
| --- | --- |
| Terraform owns containers, never content | An empty Snowflake account plus credentials is enough to stand the project up. Terraform never references a dbt model, so `dbt build` and `terraform apply` cannot fight over the same object. |
| Monitoring SQL kept out of Terraform | It reads `ACCOUNT_USAGE` and depends on a dbt-owned table. A monitoring query change must not be able to break `terraform apply`. |
| Airflow orchestrates; it does not implement | Every task shells out to a Python script or to dbt, so ingestion and transformation are testable and runnable without Airflow. |
| Streams + Tasks deliberately not used | Orchestration is Airflow's job. Splitting it across two schedulers would make failure handling and retries ambiguous. |
| `LOAD_CONTROL` drives incremental transformation | Gives dbt an explicit, queryable "what is new" signal instead of relying on timestamps or Snowflake load history. |
| The staged file is retained after `COPY INTO` | Storage is negligible, and it leaves the exact loaded bytes available for reconciliation. `PUT ... OVERWRITE` handles the repeating file names. |
| `archive_files` runs last | A file is archived only once its rows are in Snowflake *and* dbt has succeeded. A failure leaves the file in `data/` for the next run — nothing is ever deleted. |
| File-driven, not batch-number-driven | The DAG processes whatever is waiting; no task names a batch, so the pipeline needs no edit as batches change. |
| Notification is configuration, not a task | `email_on_failure` plus a success callback keeps the DAG graph to the four tasks that do work, and nothing has to exit non-zero to keep a failed run red. |
| `SYSTEM$SEND_EMAIL` kept as a second route | Independent of SMTP, and the mechanism the Snowflake-side alert uses anyway. |
| dbt in an isolated venv inside the Airflow image | Airflow and dbt-core pin overlapping-but-different Jinja2/click ranges; installing them together produces an unresolvable image. |

---

## 13. Assumptions & Limitations

- **Terraform state is local** (`terraform/terraform.tfstate`, gitignored), so `plan` and `apply`
  work only from the machine that holds it. That is why CI validates the configuration but never
  applies it. A real deployment would use an encrypted remote backend.
- **No `lifecycle.prevent_destroy`** is set, so `terraform destroy` works out of the box for a case
  study. `RAW.RAW_TRADES` is the object that would need protecting in production.
- **`ACCOUNT_USAGE` lags** — documented at up to 45 minutes for `QUERY_HISTORY` and up to 2 hours
  for `COPY_HISTORY`. `last_gold_refresh` is read from the live GOLD table instead, so that one
  column is always current.
- **No "no data today" alert.** The case study defines no arrival SLA, so a day with no run
  produces no row and carries no meaning.
- **The alert ships suspended**, and its recipient is a `you@example.com` placeholder that must be
  edited once. Both email senders require an address verified in the Snowflake account.
- **Expiry needs elapsed time.** A freshly loaded trade is either rejected as `PAST_MATURITY`
  (maturity before the ingestion date) or not yet matured, so expired rows cannot be produced by
  loading alone — which is why that rule is proven by a dbt unit test rather than by loaded data.
- **`force_reload` creates duplicate RAW rows** by design. It exists to demonstrate the
  load-history behaviour, not for routine use.
- **`snowflake/sql/`** is superseded by Terraform and kept only as a record of the first phase. It
  is not part of setup.
- **Password authentication**, not key-pair. Sufficient for a case study; a production deployment
  would use key-pair or OAuth.
