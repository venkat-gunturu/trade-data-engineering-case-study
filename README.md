# Trade Data Engineering Case Study

## 1. Project Overview

This repository implements the Deutsche Bank Trade Data Engineering case study using:

- **Python** — simulate an API-like trade source and generate JSON batches
- **Snowflake** — ingestion and cloud data warehouse
- **dbt Core + dbt-snowflake** — transformation, validation, and business rules
- **Apache Airflow** — orchestration and monitoring
- **Docker** — containerized Airflow runtime
- **Terraform** — Infrastructure as Code
- **GitHub Actions** — CI/CD

The case study asks for a robust ETL pipeline that ingests, processes, validates, and stores trade data in Snowflake. Mandatory rules include lower-version rejection, same-version replacement, maturity validation, expiration, rejected-trade auditing, orchestration/monitoring, and error handling. Optional items include custom trade rules, dashboards, architecture diagrams, Airflow/Composer orchestration, and IaC.

## 2. Business Understanding

Thousands of trades are generated and transmitted to a central store daily.

For this implementation, the source is modeled as an **API-like service**. Python generates deterministic trade payloads and writes them as JSON batch files.

```text
Mock API / Python Generator
          |
          v
      JSON batches
          |
          v
   Snowflake Stage
          |
          v
      RAW layer
          |
          v
       dbt
          |
          v
 Bronze -> Silver -> Gold
          |
          +------------------+
          |                  |
          v                  v
     TRADE_STORE       REJECTED_TRADES
```

## 3. Agreed Architecture

We use a **Medallion-style architecture**.

```text
                         +----------------------+
                         |  Python Mock Source  |
                         |     API Simulator    |
                         +----------+-----------+
                                    |
                                    | JSON
                                    v
                         +----------------------+
                         |   Snowflake Stage    |
                         |     TRADE_STAGE      |
                         +----------+-----------+
                                    |
                                    | COPY INTO
                                    v
                    +--------------------------------+
                    |             RAW                  |
                    |          RAW_TRADES              |
                    |                                  |
                    | payload VARIANT                  |
                    | batch_id                         |
                    | source_file_name                 |
                    | surrogate_key                    |
                    | ingested_at                      |
                    +---------------+------------------+
                                    |
                                    | dbt
                                    v
                    +--------------------------------+
                    |          BRONZE / STAGING        |
                    |            STG_TRADES             |
                    |                                  |
                    | Relational trade structure       |
                    +---------------+------------------+
                                    |
                                    | dbt
                                    v
                    +--------------------------------+
                    |         SILVER / INTERMEDIATE    |
                    |        INT_TRADE_VALIDATION      |
                    |                                  |
                    | Version + maturity + DQ rules    |
                    +---------------+------------------+
                                    |
                                    | dbt
                                    v
                    +--------------------------------+
                    |            GOLD / FINAL          |
                    |                                  |
                    | TRADE_STORE                     |
                    | REJECTED_TRADES                  |
                    +--------------------------------+

                    Airflow orchestrates the flow
```

### Snowflake schemas

```text
TRADE_DB
|
+-- RAW
|   +-- JSON_FILE_FORMAT
|   +-- TRADE_STAGE
|   +-- RAW_TRADES
|
+-- BRONZE
|   +-- STG_TRADES              <-- created by dbt
|
+-- SILVER
|   +-- INT_TRADE_VALIDATION    <-- created by dbt
|
+-- GOLD
    +-- TRADE_STORE             <-- created by dbt
    +-- REJECTED_TRADES         <-- created by dbt
```

### Ownership decision

Phase 1 creates the initial Snowflake infrastructure and RAW ingestion table.

dbt owns the transformation-layer objects:

- Bronze
- Silver
- Gold

Therefore, Bronze/Silver/Gold tables are **not manually created** during Phase 1.

Terraform now owns that infrastructure plus `RAW.RAW_TRADES`, and creates it
from scratch against an empty Snowflake account. See section 16.

## 4. RAW Layer

`RAW.RAW_TRADES` is **append-only**.

Structure:

```text
surrogate_key
payload
batch_id
source_file_name
ingested_at
```

Types:

```text
surrogate_key       NUMBER AUTOINCREMENT
payload             VARIANT
batch_id            VARCHAR
source_file_name    VARCHAR
ingested_at         TIMESTAMP_NTZ
```

The source payload is preserved without flattening during ingestion.

Example payload:

```json
{
  "trade_id": "T1001",
  "trade_version": 1,
  "trade_type": "BUY",
  "instrument_type": "FX",
  "counterparty": "DB_COUNTERPARTY",
  "trade_date": "2026-08-28",
  "event_timestamp": "2026-08-28T09:00:00",
  "maturity_date": "2026-09-30",
  "notional_amount": 1000000.00,
  "currency": "USD",
  "price": 1.084500,
  "quantity": 1000000,
  "trade_status": "NEW"
}
```

The entire object is stored in `payload`.

Ingestion metadata is maintained separately:

```text
batch_id
source_file_name
ingested_at
surrogate_key
```

This gives us source-payload preservation, auditability, replayability, and a clean boundary between source data and ingestion metadata.

## 5. Why JSON?

The source is modeled as an API-like service.

Therefore Python generates JSON payloads rather than relational CSV files.

```text
API response
     |
     v
JSON
     |
     v
VARIANT
     |
     v
dbt relational transformation
```

The JSON contains trade/business attributes. Ingestion metadata belongs to the ingestion layer.

## 6. Data Loading Strategy

The agreed ingestion pattern is **batch file ingestion**, not streaming.

```text
Python
   |
   | Generate JSON
   v
JSON file
   |
   | PUT
   v
Snowflake named stage
   |
   | COPY INTO
   v
RAW.RAW_TRADES
```

### PUT

`PUT` uploads the local JSON file to the Snowflake internal named stage.

### COPY INTO

`COPY INTO` loads the JSON data from the stage into `RAW.RAW_TRADES`.

The Python ingestion script executes both operations using the Snowflake Python connector.

Airflow will call the Python ingestion script.

Therefore:

> **Python owns the ingestion implementation. Airflow owns orchestration.**

We will not put the entire ingestion implementation directly inside the Airflow DAG.

## 7. Why We Are Not Using Streams + Tasks

We explicitly decided **not to use Snowflake Streams + Tasks** for this implementation.

The pipeline already has:

```text
Airflow
   |
   +-- generate source data
   |
   +-- execute ingestion
   |
   +-- invoke dbt
   |
   +-- run dbt tests
   |
   +-- handle failures
```

Streams + Tasks would introduce another orchestration mechanism into the same pipeline.

They are not required by the case study.

This does not mean Streams + Tasks are inappropriate in production. A different Snowflake-native/event-driven architecture could use them. For this case study, the responsibility split is:

```text
Airflow   = orchestration
dbt       = transformation/business logic
Snowflake = storage + processing
```

RAW is append-only, Airflow controls the batch workflow, and there is no requirement for Snowflake-native CDC.

## 8. BRONZE / STAGING

dbt transforms the RAW `VARIANT` payload into the structured trade model.

Model:

```text
BRONZE.STG_TRADES
```

Columns:

```text
trade_id
trade_version
trade_type
instrument_type
counterparty
trade_date
event_timestamp
maturity_date
notional_amount
currency
price
quantity
trade_status

surrogate_key
batch_id
source_file_name
ingested_at
```

Example extraction:

```sql
payload:trade_id::VARCHAR
payload:trade_version::NUMBER
payload:maturity_date::DATE
payload:notional_amount::NUMBER(18,2)
payload:event_timestamp::TIMESTAMP_NTZ
```

This layer handles parsing, casting, extraction, and basic normalization.

## 9. SILVER / INTERMEDIATE

Model:

```text
SILVER.INT_TRADE_VALIDATION
```

This is the business-rule processing layer.

It determines whether an incoming trade is:

```text
ACCEPTED
REJECTED
```

and records the reason where applicable.

Example rejection reasons:

```text
LOWER_VERSION
PAST_MATURITY
```

The layer also prepares the data needed to maintain the current trade state and expiration status.

## 10. GOLD / FINAL

### GOLD.TRADE_STORE

Contains the current trade state.

```text
trade_id
trade_version
trade_type
instrument_type
counterparty
trade_date
event_timestamp
maturity_date
notional_amount
currency
price
quantity
status
updated_timestamp
```

Status:

```text
VALID
EXPIRED
```

Logical current-state key:

```text
trade_id
```

There should be one current record per trade.

### GOLD.REJECTED_TRADES

Stores rejected trades for audit/compliance.

```text
trade_id
trade_version
trade_type
instrument_type
counterparty
trade_date
event_timestamp
maturity_date
notional_amount
currency
price
quantity

rejection_reason
rejected_at
source_file_name
ingested_at
batch_id
```

## 11. Required Business Rules

The first implementation contains **only the rules explicitly requested by the case study**.

### Rule 1 — Lower version

```text
Existing:  T1001 V2
Incoming:  T1001 V1
Result:    REJECT
Reason:    LOWER_VERSION
```

The current V2 remains in `TRADE_STORE`.

### Rule 2 — Same version

```text
Existing:  T1001 V2
Incoming:  T1001 V2
Result:    REPLACE
```

The incoming record becomes the current record.

### Rule 3 — Past maturity

If:

```text
maturity_date < current_date
```

the incoming trade is rejected.

```text
Reason = PAST_MATURITY
```

### Rule 4 — Expiration

A valid trade whose maturity date has passed is marked:

```text
status = EXPIRED
```

### Rule 5 — Rejection audit

Every rejected trade is written to:

```text
GOLD.REJECTED_TRADES
```

with:

- rejection reason
- rejected timestamp
- source file
- ingestion timestamp
- batch ID

## 12. Custom Trade Rules — Agreed Strategy

The case study says additional trade-processing rules are optional.

We will **not add custom rules before the mandatory pipeline works**.

### First

Complete the entire required end-to-end flow:

```text
Python
  ↓
JSON
  ↓
Stage
  ↓
RAW
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Airflow
  ↓
dbt tests
```

### Then

After successful end-to-end implementation and validation, add a small number of meaningful domain enhancements.

Potential enhancements:

```text
1. Notional amount must be > 0
2. Currency must be valid/supported
3. Quantity must be > 0
4. Price validation
5. Required counterparty validation
```

These will be clearly documented as **optional enhancements**, not mixed with the core requirements.

## 13. Batch Test Scenarios

The generator produces four deterministic batches of 200 trades each (800 total,
720 distinct `trade_id`s). Attributes are synthetic via Faker and a seeded RNG;
the business-rule scenarios are **not** random - each batch fills a fixed quota
and is then padded with new trades.

```text
              quota  padding   new ids  reused ids
BATCH_001       0      200       200        0     baseline: all new, future maturity
BATCH_002      35      165       170       30     higher 15, same 15, past_maturity 5
BATCH_003      35      165       175       25     lower  15, same 10, past_maturity 10
BATCH_004      30      170       175       25     higher 20, same 5,  past_maturity 5
```

Only `past_maturity` records and padding allocate **new** `trade_id`s. Higher,
same, and lower version records **reuse** existing ids - which is why 800 records
carry only 720 distinct ids.

The cross-batch dependency is deliberate: a lower-version trade in BATCH_003 can
only exist because BATCH_002 raised that trade to version 2. This is why the
scenarios cannot be generated randomly.

Reproducibility: `--seed` fixes the RNG and `--as-of-date` anchors every date, so
a given seed and anchor always produce byte-identical files.


## 14. Airflow Architecture

Airflow orchestrates the components built in Phases 2 and 3. It does not
reimplement any of them.

```text
generate_trades        data_generator/generate_trades.py
        |
        v
ingest_trades          data_generator/ingest_trades.py  (PUT + COPY INTO)
        |
        v
dbt_build              dbt build  (models + tests)
```

`dbt build` interleaves each model with its own tests and fails on the first
failure, so there is no separate test task.

The DAG lives in `airflow/dags/trade_pipeline.py` and uses `BashOperator`,
because both Python components are CLIs with argparse and non-zero exit codes.

### Docker runtime

```text
docker-compose.yml
|
+-- postgres            Airflow metadata database
+-- airflow-init        one-shot: db migrate + create UI user
+-- airflow-scheduler   LocalExecutor
+-- airflow-webserver   http://localhost:8080
```

Snowflake is **not** containerised; it remains the external cloud warehouse.

`airflow/Dockerfile` builds on `apache/airflow:2.11.2-python3.12` and installs
dbt-core, dbt-snowflake, the Snowflake connector, and Faker into an **isolated
virtual environment at `/opt/pipeline_venv`** — deliberately not into Airflow's
own Python environment, which shares pinned dependencies such as Jinja2 and
click with dbt-core. The DAG invokes that venv's interpreters directly.

### Prerequisites

- Docker Desktop (or Docker Engine) with Compose v2+
- A Snowflake account with Phase 1 objects created
- `cp .env.example .env` and fill in real Snowflake values
- `cp dbt/profiles.yml.example dbt/profiles.yml`

Both `.env` and `dbt/profiles.yml` are gitignored. The repository contains only
the example files.

### Start the environment

```bash
docker compose build
docker compose up -d
docker compose ps          # postgres, scheduler, webserver healthy
```

Open http://localhost:8080 and log in with the values of `AIRFLOW_WWW_USER` /
`AIRFLOW_WWW_PASSWORD` from `.env` (defaults `airflow` / `airflow`, local only).

### Trigger the DAG

Unpause `trade_pipeline`, then **Trigger DAG w/ config**. Parameters:

| Param | Default | Meaning |
| --- | --- | --- |
| `batch` | `001` | Which generated batch to ingest |
| `seed` | `42` | Generator seed, keeps the batch reproducible |
| `force_reload` | `false` | Pass `--force` to the loader; **duplicates RAW rows** |

Expected: three green tasks in order, `dbt_build` reporting 5 models and 45
tests passing.

### Re-run behaviour

Generation is pinned to the run's logical date (`{{ ds }}`) and a fixed seed,
and the loader uses `COPY INTO ... FORCE = FALSE`.

| Situation | Result |
| --- | --- |
| Retry a failed `ingest_trades` | Safe. A failed `COPY INTO` commits nothing, so the retry starts clean. |
| Re-run the same DAG run | RAW gains a second copy of the batch. `TRADE_STORE` and `REJECTED_TRADES` do not change — see below. |
| Run on a different day | `{{ ds }}` changes, so the generated content changes and the batch loads as new data. Same `trade_id`s at the same versions, which Rule 2 treats as same-version re-sends. |
| `force_reload = true` | Deliberately reloads. Same effect as a re-run, made explicit. |

**The pipeline is business-state idempotent, but RAW is append-only and grows on a
re-run.** With the current design that happens because `PUT ... OVERWRITE = TRUE`
replaces the staged file, so its metadata changes and `COPY INTO ... FORCE = FALSE`
treats it as a file it has not seen. Load history only protects a staged file that is
*not* re-uploaded.

Measured: re-running the full pipeline through Airflow took RAW from 800 to 1,000 rows,
while `TRADE_STORE` stayed at 700 and `REJECTED_TRADES` at 35 — the duplicate records
arrive as same-version re-sends and Rule 2 collapses them. RAW behaving this way is
consistent with what RAW is for: an append-only audit record of every message received,
including one received twice.

Making the staged path run-scoped in `ingest_trades.py` would make RAW append-idempotent
as well. That is a deliberate future enhancement rather than a Phase 7 change, and it is
listed in section 20.

One further consequence worth knowing: **generating on the Windows host and in the Linux
container produces different bytes for identical data.** `generate_trades.py` opens files
in text mode, so Windows writes CRLF (84,432 bytes for batch 001) and the container writes
LF (81,431 bytes). The 200 trades are identical and parse the same; only the line endings
differ. Pinning `newline="\n"` in the generator would remove the difference.

### Stop the environment

```bash
docker compose down           # keep the metadata database
docker compose down -v        # also drop it, for a clean rebuild
```

## 15. Error Handling and Monitoring

Two different questions, answered by two different layers:

| Question | Layer | Where the answer is |
| --- | --- | --- |
| **Operational monitoring** — did the pipeline run, and did anything fail? | Airflow + Snowflake administrative views | Airflow UI, `MONITORING.V_PIPELINE_HEALTH` |
| **Business / data-quality validation** — is the *data* correct? | dbt | 45 data tests, 13 unit tests, 2 singular tests, and `GOLD.REJECTED_TRADES` |

Keeping them apart matters. A rejected trade is **not** a pipeline failure: rejecting a
lower-version amendment is the pipeline working correctly, and 35 rejected trades out
of 800 is a healthy business outcome, not an incident. Operational monitoring asks
whether the machinery ran; dbt asks whether the answer is right.

### What is monitored

```text
ingestion   COPY into RAW_TRADES        files, rows, load errors
transform   dbt models and tests        nodes executed, nodes failed
freshness   GOLD.TRADE_STORE            when the store was last rebuilt
orchestration  Airflow task states      which task failed, retries, duration
```

### How Airflow reports task failures

Nothing new was needed here — the DAG already fails honestly:

- Both Python scripts are CLIs that exit non-zero, so a Snowflake problem surfaces as a
  failed task rather than a silent success.
- `retries = 2` with a one-minute delay absorbs a transient connection problem.
- `dbt build` interleaves each model with its own tests and stops at the first failure,
  so downstream models are skipped rather than built from bad input.
- The Airflow UI shows task state, retry count, duration and full logs per run.

Phase 7 adds one task, `notify_failure`, with `trigger_rule = "one_failed"`. It runs only
when a pipeline task has failed, and asks Snowflake to email the failure through the
`TRADE_ALERT_EMAIL` notification integration — so **no SMTP server or external
notification service is part of this project**.

It then exits non-zero on purpose. Airflow decides a DAG run's state from its leaf tasks,
and `notify_failure` is the leaf; if it succeeded, a run whose dbt build had failed would
be reported as successful. Failing it keeps the run red, which is the truth. On a
successful run the task is skipped, and a skipped leaf leaves the run green.

If `ALERT_EMAIL_RECIPIENT` is not configured the task says so loudly in its log and still
fails. A missing notification never turns a failed run into a successful-looking one.

### `MONITORING.V_PIPELINE_HEALTH`

One row for every day the pipeline was active:

```sql
SELECT * FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH ORDER BY day DESC;
```

```text
DAY         FILES_LOADED  ROWS_LOADED  LOAD_ERRORS  DBT_NODES  DBT_FAILURES  LAST_GOLD_REFRESH  STATUS
2026-08-30             0            0            0        117             0  2026-08-29 18:56   HEALTHY
2026-08-29             5         1000            0        964             1  2026-08-29 18:56   DEGRADED
2026-08-28             1          200            0       2103             3  2026-08-29 18:56   DEGRADED
```

It is built from `ACCOUNT_USAGE.COPY_HISTORY` (ingestion), `ACCOUNT_USAGE.QUERY_HISTORY`
(dbt), and the live `GOLD.TRADE_STORE` (freshness). `status` is `DEGRADED` when a load did
not complete cleanly or a dbt node failed, and `HEALTHY` otherwise.

The two `DEGRADED` days come from the deliberate bad-input test in Phase 6, whose
`Failed to cast variant value not-a-number` error is still in query history. That is the
view doing its job: RAW accepted the malformed payload, so `LOAD_ERRORS` is 0 and the
failure appears as `DBT_FAILURES` — exactly where the design puts the type gate.

Two things this example shows about reading the view honestly:

- `ROWS_LOADED` counts every row Snowflake has ever loaded, so it is larger than the 800
  rows in `RAW_TRADES`. Load history is a record of loads, not of current table contents.
- The top row shows `FILES_LOADED = 0` even though a load had just run, because
  `COPY_HISTORY` lags. `DBT_NODES` was already visible, because `QUERY_HISTORY` lags less.
  Use the Airflow UI for "what happened in the last few minutes"; use this view for
  "what has been happening".

**Identifying dbt without configuring dbt.** dbt-snowflake prefixes every query it issues
with a JSON comment containing `"app": "dbt"` and the node's `node_id`, so dbt work is
already distinguishable in `QUERY_HISTORY`. No `query_tag` setting was added.

**Scoping.** The Snowflake account contains other warehouses and unrelated query activity,
so every query below is scoped to `TRADE_DB` / `TRADE_WH` / dbt queries. Unscoped, the
numbers are noise rather than signal.

**Latency.** `ACCOUNT_USAGE` is documented as lagging by up to 45 minutes for
`QUERY_HISTORY` and up to 2 hours for `COPY_HISTORY` (observed here: a few minutes).
`LAST_GOLD_REFRESH` is read from the live GOLD table, so freshness is never stale.

### `ALERT_PIPELINE_FAILURE`

```sql
-- what it evaluates, hourly, once resumed
SELECT 1 FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH
WHERE status = 'DEGRADED' AND day >= DATEADD(day, -1, CURRENT_DATE());
```

When that returns a row, the alert calls `SYSTEM$SEND_EMAIL`.

It complements rather than duplicates `notify_failure`: Airflow reports that a **task**
failed, the alert reports that the **outcome in Snowflake** is bad — including work Airflow
never orchestrated, such as a manual `dbt build` or a direct load.

It is deliberately left **suspended**. A resumed alert consumes credits at every evaluation,
which is not appropriate for a development account. Run it on demand instead:

```sql
EXECUTE ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE;

SELECT name, state, scheduled_time, completed_time
FROM TABLE(TRADE_DB.INFORMATION_SCHEMA.ALERT_HISTORY(
    scheduled_time_range_start => DATEADD(hour, -2, CURRENT_TIMESTAMP())))
ORDER BY scheduled_time DESC;
-- ALERT_PIPELINE_FAILURE  TRIGGERED  ...

ALTER ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE RESUME;   -- to schedule it
```

### Why there is no "no file today" alert

The case study defines no source arrival frequency and no SLA, and the DAG is triggered
manually (`schedule = None`). A day without a file is therefore not a late file, and an
alert on absence would fire every quiet day while telling us nothing. `V_PIPELINE_HEALTH`
follows the same principle: a day with no activity produces no row, and there is no
`NO_RUN` status. The alert fires only on **evidence that something ran and failed**.

**If a source SLA existed**, the design would extend rather than change: move the DAG to a
schedule matching the contract, add a sensor or timeout on the arrival window so a missing
file fails the run, and add a second alert on "no successful load within the agreed
window". None of that is built now, because the requirement to build against does not exist.

### How data-quality failures are handled

The layered design already fails closed — this is what Phase 6 demonstrated with a
deliberately malformed file:

```text
RAW      VARIANT, schema-on-read     accepts whatever the source sent, by design:
                                     RAW is the audit record of the actual message
BRONZE   typed CAST                  the type gate. Bad values fail here
SILVER   business rules              lower version / past maturity -> REJECTED
GOLD     trade_store + rejected      only clean, current records reach the store
```

```text
COPY INTO   2 rows loaded, exit 0     <- RAW accepted the malformed payload
dbt build   ERROR in stg_trades: 100071 Failed to cast variant value not-a-number
            PASS=6  ERROR=1  SKIP=56  <- everything downstream skipped
```

GOLD is never corrupted, because a failed model skips its dependents rather than building
them from bad input. Records that are well-formed but *business*-invalid are not errors at
all: they are written to `GOLD.REJECTED_TRADES` with a reason, batch id, source file and
timestamps, which is requirement 5 (log rejected trades for audit).

### How dbt failures are detected

Three independent ways, in increasing distance from the run:

1. `dbt build` exits non-zero, so the Airflow task fails and `notify_failure` fires.
2. `V_PIPELINE_HEALTH.DBT_FAILURES` becomes non-zero for the day.
3. `ALERT_PIPELINE_FAILURE` matches and emails.

### Administrative queries

The one view above is the only monitoring object created. Everything else is a query, kept
here rather than materialised as a view that would add no reuse.

```sql
-- Load history: which files arrived, when, how many rows, any errors
SELECT file_name, status, row_count, COALESCE(error_count, 0) AS errors,
       first_error_message,
       CONVERT_TIMEZONE('UTC', last_load_time)::TIMESTAMP_NTZ AS loaded_utc
FROM snowflake.account_usage.copy_history
WHERE table_catalog_name = 'TRADE_DB' AND table_name = 'RAW_TRADES'
ORDER BY last_load_time DESC;
```

```sql
-- dbt runs and failures, per node
SELECT REGEXP_SUBSTR(query_text, '"node_id": "([^"]+)"', 1, 1, 'e') AS node_id,
       execution_status, total_elapsed_time AS ms, start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND query_text ILIKE '%"app": "dbt"%'
ORDER BY start_time DESC;
```

```sql
-- Recent failures with the actual Snowflake error
SELECT start_time, error_code, error_message, LEFT(query_text, 120) AS query
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'FAIL'
  AND (database_name = 'TRADE_DB' OR query_text ILIKE '%"app": "dbt"%')
ORDER BY start_time DESC;
```

```sql
-- Warehouse cost and queuing
SELECT DATE_TRUNC('day', start_time)::DATE AS day, SUM(credits_used) AS credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE warehouse_name = 'TRADE_WH' AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1 DESC;

SELECT AVG(avg_running) AS running, AVG(avg_queued_load) AS queued
FROM snowflake.account_usage.warehouse_load_history
WHERE warehouse_name = 'TRADE_WH' AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP());
```

```sql
-- Query performance
SELECT COUNT(*) AS queries,
       ROUND(AVG(total_elapsed_time) / 1000, 3) AS avg_seconds,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_elapsed_time) / 1000, 3) AS p95_seconds
FROM snowflake.account_usage.query_history
WHERE warehouse_name = 'TRADE_WH' AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP());
```

```sql
-- Table sizes per layer. INFORMATION_SCHEMA is used rather than
-- ACCOUNT_USAGE.TABLE_STORAGE_METRICS because it reflects the live database.
SELECT table_schema, table_name, row_count, bytes
FROM TRADE_DB.information_schema.tables
WHERE table_type = 'BASE TABLE' ORDER BY 1, 2;
```

Measured on this account: `TRADE_WH` p95 query time 0.585 s over 1,965 queries in 7 days,
0.63 credits on the busiest day, zero queuing, and six loads of 200 rows with no errors.

### Setting it up

The `MONITORING` schema is created by Terraform. The view, the alert and the notification
integration are created by one SQL file:

```bash
terraform -chdir=terraform apply          # creates TRADE_DB.MONITORING
# edit the recipient in monitoring/sql/monitoring.sql, then run it in Snowsight
# as ACCOUNTADMIN, or: snowsql -f monitoring/sql/monitoring.sql
```

The file is idempotent — every statement is `CREATE OR REPLACE`, and re-running it leaves
the alert suspended.

Monitoring SQL is deliberately kept out of both Terraform state and the dbt DAG. It reads
`ACCOUNT_USAGE`, so it is operational rather than structural, and keeping it separate means
a change to a monitoring query can never fail `terraform apply` or block `dbt build`.

### MANUAL PREREQUISITE — email verification

Snowflake sends the alert mail, and it will only deliver to a **verified** email address of
a user in the account.

1. In Snowsight, open your profile and verify your email address.
2. Put that address in `monitoring/sql/monitoring.sql` (it appears once, as
   `you@example.com`) before running the file.
3. Add `ALERT_EMAIL_RECIPIENT=<that address>` to `.env`, so the Airflow `notify_failure`
   task can use it. `.env` is gitignored; only `.env.example` is committed.

Test the channel on its own:

```sql
CALL SYSTEM$SEND_EMAIL('TRADE_ALERT_EMAIL', 'you@example.com',
                       'Trade pipeline test', 'Notification integration works.');
```

Without step 1 the send fails and the alert cannot deliver. The pipeline still runs and
still fails visibly — only the email is lost.

## 16. Terraform

Terraform is the Infrastructure as Code layer. It **creates** the Snowflake
infrastructure from scratch: an empty Snowflake account plus credentials is all
that is needed. There is no import step and no dependency on pre-existing objects.

### Prerequisites

| Tool | Notes |
| --- | --- |
| Git | to clone the repository |
| Python 3.12 | `py -3.12 -m venv .venv` |
| Docker Desktop | Compose v2+, for the Airflow runtime |
| Terraform | standalone CLI, see below |
| Snowflake account | any edition; a brand-new empty account is fine |

### Installing Terraform

Terraform is a standalone Go binary. It is **not** a Python package and must not
be installed with `pip`.

```powershell
winget install --id Hashicorp.Terraform -e     # Windows
```

```bash
brew install terraform                          # macOS
```

Restart the shell, then verify:

```bash
terraform --version
```

Terraform 1.5 or newer is required.

### Snowflake credentials

Copy `.env.example` to `.env` and fill it in. `.env` is gitignored; nothing
secret is ever committed.

```bash
SNOWFLAKE_ORGANIZATION_NAME=<org>       # Terraform provider v2
SNOWFLAKE_ACCOUNT_NAME=<account>        # Terraform provider v2
SNOWFLAKE_ACCOUNT=<org>-<account>       # Python loader and dbt
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=TRADE_WH
SNOWFLAKE_DATABASE=TRADE_DB
SNOWFLAKE_SCHEMA=RAW
```

Provider v2 replaced the single `SNOWFLAKE_ACCOUNT` identifier with an
organization/account pair, so both forms are present: split your account
identifier on the hyphen. The Terraform provider is pinned to `warehouse = ""`
because it only issues DDL, which Snowflake runs without compute. Without that
pin the provider would inherit `SNOWFLAKE_WAREHOUSE` and try to connect to the
very warehouse it is about to create, which fails on an empty account.

### Creating the infrastructure

```bash
cd terraform
terraform init
terraform validate
terraform plan          # expect: 10 to add, 0 to change, 0 to destroy
terraform apply
```

Running `terraform plan` again afterwards must report **"No changes"**.

To tear the environment down again:

```bash
terraform destroy
```

### Ownership boundary

```text
Terraform  ->  TRADE_DB
               RAW / BRONZE / SILVER / GOLD / MONITORING schemas
               TRADE_WH warehouse
               RAW.JSON_FILE_FORMAT
               RAW.TRADE_STAGE
               RAW.RAW_TRADES          (append-only landing table)

dbt        ->  BRONZE.STG_TRADES
               SILVER.INT_TRADE_SEQUENCE
               SILVER.INT_TRADE_VALIDATION
               GOLD.TRADE_STORE
               GOLD.REJECTED_TRADES

SQL        ->  MONITORING.V_PIPELINE_HEALTH
               ALERT_PIPELINE_FAILURE      (monitoring/sql/monitoring.sql)
               TRADE_ALERT_EMAIL

Python     ->  the data itself (PUT / COPY INTO)
Airflow    ->  orchestration
```

Terraform creates the containers and the landing table; dbt creates the models
inside BRONZE, SILVER, and GOLD. After `terraform apply` those three schemas are
empty by design - dbt populates them on its first run.

MONITORING is the same pattern one level further out: Terraform owns the schema,
and the view and alert inside it are plain SQL. They read `ACCOUNT_USAGE`, so they
are operational rather than structural, and keeping them out of Terraform state and
out of the dbt DAG means a monitoring change can never fail a deployment or a build.
See section 15.

Roles and grants are deliberately not managed. The project runs as
`ACCOUNTADMIN`, which already owns everything.

### Loading data after apply

Terraform creates empty infrastructure. To populate it:

```bash
# directly
cd data_generator
python generate_trades.py --seed 42
python ingest_trades.py --batch 001
cd ../dbt && dbt build --profiles-dir .

# or through Airflow, which runs all three steps
docker compose up -d
# trigger the trade_pipeline DAG at http://localhost:8080
```

Baseline after BATCH_001: 200 rows in RAW, BRONZE, both SILVER models, and
TRADE_STORE (all `VALID`), with REJECTED_TRADES empty.

### Lifecycle protection

No `lifecycle.prevent_destroy` is set, so `apply` and `destroy` both work out of
the box for a developer working in their own sandbox. For a production
deployment, add it to the data-bearing resources:

```hcl
resource "snowflake_table" "raw_trades" {
  # ...
  lifecycle {
    prevent_destroy = true
  }
}
```

Terraform then refuses to destroy or replace the resource, hard-failing instead.
Note that `prevent_destroy` cannot be driven by a variable - Terraform requires a
literal - so it is an edit rather than a toggle.

### State

Local `terraform/terraform.tfstate`, gitignored via `*.tfstate`. Terraform state
stores values in plaintext and must never be committed. `.terraform.lock.hcl`
**is** committed, so everyone resolves the same provider version. A production
deployment would use an encrypted remote backend with state locking.

### A note on `snowflake/sql/`

Those scripts are the historical Phase 1 record of how the objects were first
created by hand. They are **not** part of setup, and running them is unnecessary:
Terraform is the source of truth for all infrastructure.

## 17. CI/CD

GitHub Actions will validate:

```text
Python
dbt
Terraform
```

Validation will include appropriate checks such as:

```text
Python tests
dbt compile
dbt tests / validation
Terraform fmt
Terraform validate
Terraform plan where appropriate
```

Deployment will use secrets supplied through the GitHub environment rather than committed credentials.

## 18. Security / Credentials

Never commit:

```text
Snowflake password
private keys
API credentials
.env
```

The repository contains:

```text
.env.example
```

with placeholders only.

Local development uses environment variables.

Airflow uses an Airflow connection/secret mechanism.

GitHub Actions uses GitHub Secrets.

## 19. Project Folder Structure

```text
trade-data-engineering-case-study/
│
├── README.md
├── .gitignore
├── .env.example
│
├── data/
│   └── .gitkeep
│
├── data_generator/
│   ├── generate_trades.py         batch generation CLI
│   ├── trade_scenarios.py         scenario quotas and record builders
│   └── ingest_trades.py           PUT + COPY INTO loader
│
├── snowflake/
│   └── sql/                       historical record of Phase 1 only.
│       ├── 01_database.sql        Terraform provisions the account now;
│       ├── 02_schemas.sql         these files are NOT part of setup.
│       ├── 03_warehouse.sql
│       ├── 04_stages.sql
│       └── 05_tables.sql
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_trades.sql
│   │   │   ├── schema.yml
│   │   │   └── sources.yml
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_trade_sequence.sql
│   │   │   ├── int_trade_validation.sql
│   │   │   ├── schema.yml
│   │   │   └── unit_tests.yml
│   │   │
│   │   └── marts/
│   │       ├── trade_store.sql
│   │       ├── rejected_trades.sql
│   │       ├── schema.yml
│   │       └── unit_tests.yml
│   │
│   ├── tests/                     2 singular tests
│   └── macros/
│       └── generate_schema_name.sql
│
├── airflow/
│   ├── dags/
│   │   └── trade_pipeline.py      generate -> ingest -> dbt (+ notify_failure)
│   ├── Dockerfile
│   └── requirements.txt
│
├── terraform/
│   ├── main.tf                    10 resources, created from scratch
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
│
├── monitoring/
│   ├── sql/
│   │   └── monitoring.sql         V_PIPELINE_HEALTH + ALERT_PIPELINE_FAILURE
│   └── send_failure_alert.py      called by the notify_failure task
│
├── tests/
│   ├── test_trade_generator.py
│   ├── test_ingestion_errors.py
│   ├── test_send_failure_alert.py
│   └── fixtures/
│       └── trades_batch_999.json  deliberately malformed, for the bad-input test
│
├── docs/
│   └── case-study/                the original brief
│
├── requirements.txt
├── docker-compose.yml
│
└── .github/                       Phase 8
    └── workflows/
```

## 20. Phase-by-Phase Implementation Plan

### Phase 1 — Snowflake Foundation

**Status: partially completed**

Completed/being completed:

```text
Database
Schemas
Warehouse
JSON File Format
Named Stage
RAW.RAW_TRADES
```

Do not manually create Bronze/Silver/Gold tables.

Those are dbt-owned.

---

### Phase 2 — Source Simulation + Ingestion

Build:

```text
generate_trades.py
trade_scenarios.py
ingest_trades.py
```

Flow:

```text
Python
 ↓
JSON batch
 ↓
PUT
 ↓
TRADE_STAGE
 ↓
COPY INTO
 ↓
RAW_TRADES
```

Validate:

- JSON generation
- multiple batches
- append-only RAW
- source filename
- batch ID
- ingestion timestamp
- load failures

---

### Phase 3 — dbt

Initialize:

```text
dbt Core
dbt Snowflake adapter
dbt project
profiles configuration
```

Implement:

```text
RAW
 ↓
STG_TRADES
 ↓
INT_TRADE_VALIDATION
 ↓
TRADE_STORE
REJECTED_TRADES
```

Implement all mandatory business rules.

Add dbt tests.

---

### Phase 4 — Airflow + Docker

Create:

```text
Dockerfile
docker-compose.yml
DAG
```

Orchestrate:

```text
Generate
   ↓
Ingest
   ↓
dbt build
   ↓
dbt test
```

Implement retries and failure handling.

---

### Phase 5 — Terraform

Codify Snowflake infrastructure.

Target:

```text
terraform init
terraform plan
terraform apply
```

should reproduce the required infrastructure.

---

### Phase 6 — Testing + Data Quality

**Complete.** All five mandatory rules verified in Snowflake, plus the negative paths.

#### Verified end state (all four batches loaded)

```text
RAW.RAW_TRADES               800    200 per batch
BRONZE.STG_TRADES            800
SILVER.INT_TRADE_SEQUENCE    800
SILVER.INT_TRADE_VALIDATION  800    765 ACCEPTED / 35 REJECTED
GOLD.TRADE_STORE             700    all VALID, trade_id unique
GOLD.REJECTED_TRADES          35    PAST_MATURITY 20, LOWER_VERSION 15
```

`TRADE_STORE` is 700, not 720, because the 20 past-maturity records each carry a
fresh `trade_id` and are rejected on arrival, so those ids never enter the store.
The 15 lower-version rejections do **not** reduce the count - each reuses an id
whose higher version was accepted, so all 15 remain at the higher version.

#### Rule evidence

| Rule | Evidence |
| --- | --- |
| 1 Lower version | 15 rejections; all 15 `trade_id`s still present in the store at the higher version |
| 2 Same version replaces | `T100296` v1 arrived twice; the later record (price 290.344256) is the one in the store |
| 3 Past maturity | 20 rejections; zero of those `trade_id`s appear in the store |
| 4 Expiration | Proven by dbt unit test - see below |
| 5 Rejection audit | 35 rows in `REJECTED_TRADES` with reason, batch, source file and timestamps |

#### Why zero EXPIRED rows is correct, not a gap

Rule 3 rejects when `maturity_date < ingested_at::date`; rule 4 marks EXPIRED when
`maturity_date < current_date()`. On a freshly loaded batch `ingested_at` is today,
so a trade is either rejected by rule 3 (matured) or cannot be expired (not
matured). **No loadable data can produce an EXPIRED row.** The only path is a trade
established earlier whose maturity has since passed - which requires elapsed time.

Rule 4 is therefore proven by a dbt unit test that constructs exactly that state.
The earliest maturity date in the store is 2026-09-29, so EXPIRED rows will start
appearing naturally from that date.

#### Test suite

```text
dbt build      63 nodes    5 models + 45 data tests + 13 unit tests
pytest         28 tests    19 generator + 9 loader failure paths
```

The 13 unit tests (`dbt/models/*/unit_tests.yml`) assert rule logic against fixture
rows rather than warehouse contents. That keeps them deterministic, lets them cover
states data cannot reach, and means `dbt build` stays green on a fresh clone that
has only BATCH_001 loaded.

#### Duplicate file behaviour

With the current design, re-ingesting an already-loaded batch loads it again:

```text
re-ingest BATCH_002 -> 200 rows loaded again, exit code 0
COPY_HISTORY shows trades_batch_002.json.gz with status=Loaded twice
```

That happens because `PUT ... OVERWRITE = TRUE` replaces the staged file, so its
metadata changes and `COPY INTO ... FORCE = FALSE` treats it as a file it has not
seen. Load history only protects a staged file that is *not* re-uploaded.

Business impact is contained: the duplicate records arrive as same-version re-sends,
which rule 2 collapses. Re-running the whole pipeline through Airflow took RAW from
800 to 1,000 rows while `TRADE_STORE` stayed at 700 and `REJECTED_TRADES` at 35. The
pipeline is **business-state idempotent**; RAW is append-only by design and grows.

Removing duplicate rows is a targeted `DELETE` on RAW keyed by `batch_id` and the
load's `ingested_at`. Making the staged path run-scoped in `ingest_trades.py` would
make RAW append-idempotent too — a deliberate future enhancement, listed below.

#### Bad input behaviour

A deliberately malformed fixture (`tests/fixtures/trades_batch_999.json`: unclosed
JSON array, `trade_version` of `"not-a-number"`, an invalid date, an oversized
currency code) was loaded with `--file`:

```text
COPY INTO   2 rows loaded, exit code 0      <- RAW accepted it
dbt build   ERROR in stg_trades:
            100071 (22000): Failed to cast variant value not-a-number to FIXED(10,0)
            PASS=6  ERROR=1  SKIP=56
```

RAW is schema-on-read and stores the payload as VARIANT, so malformed content lands
successfully - that is by design, since RAW is the audit record of what the source
actually sent. **BRONZE is the type gate.** The build fails there and all 56
downstream nodes are skipped, so GOLD is never corrupted. The pipeline fails closed.


### Phase 7 — Monitoring + Alerting

**Complete.** Deliberately small: one view, one alert, one Airflow task. See section 15
for the full write-up.

| Delivered | Where |
| --- | --- |
| `TRADE_DB.MONITORING` schema | `terraform/main.tf` — 10th resource |
| `V_PIPELINE_HEALTH` — daily health from COPY_HISTORY, QUERY_HISTORY and live GOLD | `monitoring/sql/monitoring.sql` |
| `ALERT_PIPELINE_FAILURE` — emails on a failed load or dbt node, left suspended | `monitoring/sql/monitoring.sql` |
| `TRADE_ALERT_EMAIL` notification integration — Snowflake sends the mail, no SMTP | `monitoring/sql/monitoring.sql` |
| `notify_failure` task, `trigger_rule="one_failed"` | `airflow/dags/trade_pipeline.py` |
| Administrative queries for loads, dbt runs, failures, cost, performance, table sizes | README section 15 |

#### What was deliberately not built

- **No "no file today" alert.** The case study defines no arrival frequency or SLA, so
  the absence of a run carries no meaning. Section 15 explains what would be added if a
  source contract existed.
- **No wrapper view per administrative query.** Five candidate views were reduced to
  documented SQL; only `V_PIPELINE_HEALTH` earned an object, because the alert condition
  and the operator query must not drift apart.
- **No monitoring CLI, no dashboard, no external notification service.**
- **No `query_tag` configuration in dbt** — dbt already stamps every query with its
  `node_id`, so dbt activity is identifiable in `QUERY_HISTORY` without changing dbt.

Verified: Terraform plan clean at 10 resources; the view returns a correct `DEGRADED` day
for the real Phase 6 dbt failure; the alert exists, is suspended, and `EXECUTE ALERT`
recorded `TRIGGERED` in `ALERT_HISTORY`; `SYSTEM$SEND_EMAIL` delivered; a controlled DAG
failure ran `notify_failure` and left all six table counts unchanged.

---

### Phase 8 — CI/CD

GitHub Actions:

```text
Python validation
      +
dbt validation
      +
Terraform validation
```

Then add deployment workflow where appropriate.

---

### Phase 9 — Documentation + Final Submission

Deliver:

```text
README
Architecture diagram
Setup guide
Execution guide
Business rules
Technology choices
Monitoring approach
Failure handling
Scalability approach
```

All code and documentation will be stored in the public GitHub repository.

## 21. Final End-to-End Target

```text
                         +----------------+
                         | Python Source  |
                         | API Simulator  |
                         +-------+--------+
                                 |
                                 | JSON
                                 v
                         +---------------+
                         | Snowflake     |
                         | Stage         |
                         +-------+-------+
                                 |
                              COPY INTO
                                 |
                                 v
                         +---------------+
                         | RAW_TRADES    |
                         | VARIANT       |
                         | Append Only   |
                         +-------+-------+
                                 |
                                dbt
                                 |
                                 v
                         +---------------+
                         | BRONZE        |
                         | STG_TRADES    |
                         +-------+-------+
                                 |
                                dbt
                                 |
                                 v
                         +---------------+
                         | SILVER        |
                         | Validation    |
                         +-------+-------+
                                 |
                                dbt
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
             +-------------+          +----------------+
             | TRADE_STORE |          | REJECTED_      |
             | VALID/      |          | TRADES         |
             | EXPIRED     |          | + reason       |
             +-------------+          +----------------+

                         ^
                         |
                    AIRFLOW
                  ORCHESTRATION

                         ^
                         |
                    DOCKER

                         +

                    TERRAFORM
               INFRASTRUCTURE AS CODE

                         +

                  GITHUB ACTIONS
                       CI/CD
```

## 22. Core Implementation Principle

**Do not over-engineer the first iteration.**

The agreed sequence is:

```text
MANDATORY REQUIREMENTS
        ↓
END-TO-END SUCCESS
        ↓
TEST EVERYTHING
        ↓
OPTIONAL CUSTOM TRADE RULES
        ↓
MONITORING / SCALABILITY HARDENING
        ↓
CI/CD + IaC
        ↓
FINAL DOCUMENTATION
```

The priority is a working, demonstrable pipeline first, followed by enhancements.

## 23. Current Status

```text
Phase 1   ██████████  Snowflake foundation
Phase 2   ██████████  Generator + PUT/COPY INTO ingestion
Phase 3   ██████████  dbt: Bronze -> Silver -> Gold, 45 tests
Phase 4   ██████████  Airflow + Docker orchestration
Phase 5   ██████████  Terraform IaC (10 resources created from scratch)
Phase 6   ██████████  Testing + Data Quality (rules verified in Snowflake)
Phase 7   ██████████  Monitoring + Alerting (1 view, 1 alert, 1 Airflow task)
Phase 8   ░░░░░░░░░░  Next: CI/CD
Phase 9   ░░░░░░░░░░
```

**Next implementation step:** Phase 8 — CI/CD with GitHub Actions to validate and deploy dbt and Terraform.

Deferred by decision, each to its own pass: the optional custom trade rules (section 12); run-scoped stage paths to make RAW append-idempotent; an incremental `TRADE_STORE` and append-only `REJECTED_TRADES`; splitting `dbt build` into `dbt run` and `dbt test`; late-arriving dimensions; and the scalability design for a 10,000x increase in volume.
