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

### One batch per invocation

`generate_trades.py` writes **one** file per run, not all four. A local
checkpoint, `data/.generation_progress`, lists the file names already written in
the current cycle, one per line:

```text
invocation 1   clear data/archive/, write trades_batch_001.json
invocation 2                        write trades_batch_002.json
invocation 3                        write trades_batch_003.json
invocation 4                        write trades_batch_004.json
invocation 5   reset checkpoint, clear data/archive/, write trades_batch_001.json
```

So a **scheduled** generation DAG drip-feeds one file at a time into `data/`,
which is what the pipeline is built to react to, instead of dumping the whole
data set at once.

Two things the generator never touches: files sitting directly in `data/` — they
are waiting for the pipeline and would be lost — and `data/archive/` in the
middle of a cycle. Archive is cleared only at a cycle boundary, where everything
in it has already been processed.

Because the scenarios depend on each other across batches, each invocation still
builds the whole four-batch sequence in memory from the same seed and anchor date
and writes only the batch it owns. The content is therefore identical to what an
all-at-once run produced.

A checkpoint file is enough here — the generator and Airflow share the same
mounted `data/` directory, so there is nothing to coordinate across hosts and no
reason for a table. It is safe if missing, empty, partially filled, or complete.
The file is git-ignored: it is local run state, not source.


## 14. Airflow Architecture

Airflow orchestrates the components built in Phases 2 and 3. It does not
reimplement any of them.

**Two independent DAGs**, connected only by a directory. Generation produces
files; the pipeline reacts to whatever files are there when it runs. There is no
Airflow dependency between them, so either can be triggered, rescheduled or
re-run on its own.

```text
trade_data_generation                         trade_pipeline
─────────────────────                         ──────────────
generate_trades                               find_files      what is waiting in data/?
      |                                             |
      v                                             v
   data/trades_batch_NNN.json  ------------>  ingest_trades   PUT + COPY INTO
                                                    |
                                                    v
                                              dbt_build       models + tests
                                                    |
                                                    v
                                              archive_files   data/ -> data/archive/
```

### Directories

```text
data/                        waiting to be processed
  trades_batch_001.json
  trades_batch_002.json
  .generation_progress       generator checkpoint, git-ignored
  archive/                   successfully processed
    trades_batch_003.json
```

The generator writes only to `data/`. The pipeline reads only from `data/`, and
moves a file to `data/archive/` once it is done with it. Discovery is **not
recursive**, which is what keeps `data/archive/` from being picked up again, and
it matches `trades_batch_*.json`, so the `.generation_progress` checkpoint is
never mistaken for input.

The files are plain `.json`. Compression happens in flight — `PUT ... AUTO_COMPRESS`
gzips on upload, so the stage holds `.json.gz` while the local file stays `.json`.

### File-driven, not batch-driven

The pipeline has no `batch` parameter and no hardcoded `BATCH_001`. Every run
processes whatever is present, whether that is one file or several.

| On a run | What happens |
| --- | --- |
| Files waiting | All of them are ingested, dbt rebuilds, then all of them are archived |
| `data/` empty | `find_files` short-circuits. Ingestion and dbt are **skipped**, the run is **successful**, and the log says `No files available for processing.` No warehouse cost |
| Ingestion fails | DAG fails. Files stay in `data/`. Nothing is archived |
| dbt fails | DAG fails. Files stay in `data/`. Nothing is archived |

Archiving is the last task, downstream of both ingestion and dbt, so a file is
only ever archived after its rows are in Snowflake *and* the models rebuilt.
Failed files are never deleted — the next run retries them.

`find_files` is a `ShortCircuitOperator`: it returns the list of files it found,
which serves as both the run/skip decision and the exact list handed to
`ingest_trades` and `archive_files` through XCom. A file generated midway through
a run is therefore left for the next run rather than ingested but not archived.

### Scheduling

```text
trade_data_generation   schedule=None      manual; set a cron to schedule it
trade_pipeline          */30 * * * *       every 30 minutes
```

DAGs are paused at creation, so nothing runs until you unpause it in the UI. A
scheduled DAG can still be triggered by hand at any time.

```text
10:00  generation runs      -> data/trades_batch_001.json
10:30  pipeline runs        -> ingests, dbt, moves it to data/archive/
11:00  pipeline runs        -> data/ empty, "No files available", SUCCESS
11:30  generation runs      -> data/trades_batch_002.json  (next in the cycle)
12:00  pipeline runs        -> ingests, dbt, archives
```

Each generation run advances the cycle by one batch, so BATCH_001 through
BATCH_004 arrive over four runs and the fifth starts the cycle again.

### Idempotency, stated honestly

The operational guard is the directory move: **once a file is archived it is no
longer in `data/`, so a normal run cannot process it twice.** That is the
mechanism the design relies on.

Snowflake's load history is a second line of defence, not the primary one. `PUT`
uses `OVERWRITE = TRUE` because batch file names repeat every generation cycle —
without it the second cycle's upload would be `SKIPPED` and the load would take
the previous cycle's bytes. The staged file is then **deliberately retained**
after `COPY INTO`: it costs almost nothing and leaves the exact bytes Snowflake
loaded available for inspection. If `COPY INTO` fails the file stays staged and
the retry re-uploads it.

Deleting files from `data/` or `data/archive/` and regenerating is safe and
supported; nothing blocks a re-run. Re-ingesting content Snowflake has already
loaded needs the `force_reload` parameter, which **does** add duplicate rows to
RAW — Rule 2 then collapses them, so `TRADE_STORE` stays correct.

### Notification email — MANUAL PREREQUISITE

The DAG reports its own outcome by email, with no notification task in the graph:

| Outcome | How | Fires |
| --- | --- | --- |
| Failure | `email_on_failure` in `default_args` — Airflow's **built-in**, no code | Once, when a task reaches `failed` |
| Success | `send_dag_success_email`, one function in `trade_pipeline.py` | Once per successful run |

Mail is sent by Airflow's own SMTP mailer, configured with the
`AIRFLOW__SMTP__*` variables in `docker-compose.yml`, which read from `.env`:

```text
SMTP_HOST / SMTP_PORT / SMTP_STARTTLS    the server (Gmail: smtp.gmail.com, 587, True)
SMTP_USER / SMTP_PASSWORD                the credential
SMTP_MAIL_FROM                           the sender address
ALERT_EMAIL_RECIPIENT                    who receives the mail
```

**Gmail needs an App Password**, not your account password: enable 2-Step
Verification, then generate a 16-character App Password at *myaccount.google.com
→ Security → App passwords*. Your normal password fails with SMTP error `535`.
`.env` is gitignored; only `.env.example` is committed, and CI never sends mail
(section 17).

`ALERT_EMAIL_RECIPIENT` is shared with the Snowflake sender described in section
15, which requires a **verified** email address of a user in your Snowflake
account (Snowsight → profile → verify email). Use an address that satisfies both.

Two behaviours worth knowing:

- **The failure email fires once, not three times.** `retries = 2` means a task
  with retries left goes to `up_for_retry`, not `failed`, and only `failed`
  sends the mail. You get one email per failed task, after its final attempt.
- **A short-circuited run sends nothing.** When `data/` is empty `find_files`
  short-circuits, everything downstream is skipped, and the run is correctly
  *successful*. The DAG runs every 30 minutes, so mailing on that would send 48
  "success" notifications a day reporting that nothing happened. The success
  function checks whether `ingest_trades` was skipped and returns early.

If `ALERT_EMAIL_RECIPIENT` is unset the recipient list is empty and no mail is
attempted — the pipeline still runs and a failure is still red in the UI.

This is a simple operational alert: it says which task failed, nothing more. It
is separate from the Snowflake monitoring *alert* in section 15, which watches
the data rather than the orchestrator.

`monitoring/send_failure_alert.py` remains as a **standalone CLI** — a second,
independent route that asks Snowflake to send the mail via `SYSTEM$SEND_EMAIL`
on the `TRADE_ALERT_EMAIL` integration. The DAG does not call it. It is there for
when SMTP is unavailable, and to prove the Snowflake integration works without
waiting for a real failure.

Both DAGs use `BashOperator`, because the Python components are CLIs with
argparse and non-zero exit codes. `dbt build` interleaves each model with its own
tests and fails on the first failure, so there is no separate test task.

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

### Trigger the DAGs

The normal manual test cycle:

1. Unpause and trigger **`trade_data_generation`**. Expected: **one** batch file
   appears in `data/` — `trades_batch_001.json` on the first run of a cycle. Trigger
   it again for the next batch; four runs cover the full data set.
2. Unpause and trigger **`trade_pipeline`**. Expected: four green tasks —
   `find_files`, `ingest_trades`, `dbt_build`, `archive_files` — and the files
   have moved to `data/archive/`.
3. Trigger `trade_pipeline` again with `data/` now empty. Expected: `find_files`
   green, the other three **skipped**, run **successful**, log reads
   `No files available for processing.`

To start over, delete files from `data/` and `data/archive/`, delete
`data/.generation_progress`, and trigger generation again. Nothing blocks a
re-run. (Deleting the checkpoint is what forces the cycle back to BATCH_001; the
generator does that by itself once the fourth batch is done.)

| DAG | Param | Default | Meaning |
| --- | --- | --- | --- |
| `trade_data_generation` | `seed` | `42` | Generator seed, keeps the batch reproducible |
| | `records_per_batch` | `200` | Trades per batch file |
| `trade_pipeline` | `force_reload` | `false` | Pass `--force` to the loader; **duplicates RAW rows** |

### Re-run behaviour

| Situation | Result |
| --- | --- |
| Re-run the pipeline with `data/` empty | Clean success. Nothing ingested, no dbt run |
| Retry a failed `ingest_trades` | Safe. A failed `COPY INTO` commits nothing, and the staged file is re-used |
| Retry after a dbt failure | Files are still in `data/`, so the next run picks them up again |
| Re-run generation | Writes the **next** batch file in the cycle into `data/`. The next pipeline run processes it |
| Re-run generation after the fourth batch | Cycle rolls over: `data/archive/` is cleared, the checkpoint reset, `trades_batch_001.json` written again |
| `force_reload = true` | Deliberately reloads content already loaded, which **does** add duplicate rows to RAW |

**The pipeline is business-state idempotent.** Duplicate records arrive as
same-version re-sends and Rule 2 collapses them, so `TRADE_STORE` and
`REJECTED_TRADES` stay correct while RAW grows. Measured during Phase 7: a
deliberate reload took RAW from 800 to 1,000 rows while `TRADE_STORE` stayed at
700 and `REJECTED_TRADES` at 35. RAW behaving this way is consistent with what
RAW is for — an append-only audit record of every message received, including one
received twice.

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

On failure, `email_on_failure` in `default_args` emails `ALERT_EMAIL_RECIPIENT` over SMTP
(section 14), naming the task and linking to its log. This is Airflow's built-in mechanism,
so it needs no code, and being in `default_args` means every task is covered — a task added
later cannot be forgotten. Notification is configuration rather than a node, so the graph
stays the four tasks that actually do work, and nothing has to exit non-zero to keep the run
red.

On success, the DAG-level `on_success_callback` sends one summary per run — unless the run
short-circuited on an empty `data/`, in which case it stays quiet.

That email answers "a task failed". The Snowflake alert below answers "the data is wrong".
They are separate on purpose.

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

It complements rather than duplicates the DAG's failure email: Airflow reports that a
**task** failed, the alert reports that the **outcome in Snowflake** is bad — including
work Airflow never orchestrated, such as a manual `dbt build` or a direct load. They also
deliver by different routes: the alert through the `TRADE_ALERT_EMAIL` integration, Airflow
over SMTP, so neither depends on the other working.

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

1. `dbt build` exits non-zero, so the Airflow task fails and emails the failure.
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

The same integration carries the standalone `send_failure_alert.py` CLI's email, so
verifying the address once covers both. The DAG's own emails go over SMTP and need no
verification — see section 14.

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
# directly - one generator run writes one batch file
cd data_generator
python generate_trades.py --seed 42          # -> data/trades_batch_001.json
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

Two GitHub Actions workflows. A pull request is validated automatically and can change
nothing in Snowflake; deploying is a separate, manual, approved action.

```text
.github/workflows/ci.yml       pull requests + pushes to main
.github/workflows/deploy.yml   manual only, gated by the production environment
```

### `ci.yml` — what runs on every pull request

**Job 1: `validate`** — no credentials, cannot reach Snowflake, about a minute.

| Step | Check |
| --- | --- |
| Credential guard | Fails if `.env`, `profiles.yml`, `*.tfstate`, `*.tfvars`, `*.tfplan` or `.terraform/` is tracked |
| `pytest tests/ -q` | 34 Python tests |
| `dbt parse` | Resolves every model, `ref`, source, macro, generic test and unit test definition |
| `terraform fmt -check -recursive` | Formatting |
| `terraform init -backend=false` + `validate` | Syntax, types, provider schema |

The Snowflake variables in this job are dummy strings on purpose. `dbt parse` needs them to
render the `env_var()` calls in the profile but never opens a connection, so the job that
runs on every push is *structurally* incapable of touching the warehouse. `dbt compile`
would connect, which is why `parse` is the offline check and the real build happens below.

**Job 2: `dbt-integration`** — a full `dbt build` against a throwaway copy of the database.

```sql
CREATE OR REPLACE DATABASE TRADE_DB_CI_<run_id> CLONE TRADE_DB;   -- zero-copy, instant
dbt build            with SNOWFLAKE_DATABASE = TRADE_DB_CI_<run_id>
DROP DATABASE TRADE_DB_CI_<run_id>;                               -- always, even on failure
```

This needed no change to the dbt project. `models/staging/sources.yml` already resolves its
database from `SNOWFLAKE_DATABASE`, and so does the profile, so pointing that one variable
at the clone moves the source *and* every model target with it. `TRADE_DB` is neither read
nor written.

Why a clone rather than a CI schema: `macros/generate_schema_name.sql` deliberately returns
the custom schema name literally, so models always land in `BRONZE` / `SILVER` / `GOLD`.
Isolating by database sidesteps that without touching a business macro. Snowflake's cloning
is metadata-only, so it costs nothing in storage and completes in about a second.

The clone carries whatever RAW holds, so CI is a real integration test rather than a smoke
test — and every test also passes against an empty RAW, so **CI never depends on anyone
having loaded the batch files**. The run id in the name means concurrent pull requests
cannot collide, and the drop step runs `if: always()` so a red build leaves nothing behind.

Verified locally before first push: clone carried 800 RAW rows, `dbt build` 63 nodes passed,
`TRADE_STORE` 700 in the clone, database dropped, zero leftovers, `TRADE_DB` unchanged.

### `deploy.yml` — deploying dbt to the real database

Manual `workflow_dispatch` only, bound to a GitHub Environment named `production` with a
required reviewer. Merging to `main` runs CI and changes nothing in Snowflake; deployment is
a decision, and GitHub pauses for approval before the job starts.

The job runs `dbt build --profiles-dir .` against `TRADE_DB`. It does not load data —
that is the Airflow DAG's job — and it does not run Terraform.

### Terraform: validated in CI, applied locally

Terraform state lives at `terraform/terraform.tfstate` and is gitignored, which is correct:
state describes real resources and must never be committed. The consequence is that a
GitHub Actions runner starts with no state, so a `terraform plan` there would report
"10 to add" — describing an empty account rather than yours — and `apply` would fail on
objects that already exist.

CI therefore runs the Terraform checks that are meaningful without state, and those happen
to be the ones that need no credentials either. `plan` and `apply` stay local:

```bash
terraform -chdir=terraform plan     # review
terraform -chdir=terraform apply    # then apply
```

This is a safety property rather than a shortcut: **no workflow in this repository has a
path to `terraform apply`**, so no pull request, from any branch, can create, change or
destroy infrastructure.

Remote state is deliberately not introduced. The case study lists IaC as optional and says
nothing about state management, and a remote backend would mean another cloud service, more
secrets and a migration of working state, to enable something nothing here needs. If CI ever
had to apply infrastructure, the change is one `backend` block in `providers.tf` plus
`terraform init -migrate-state`.

### Order when standing the project up from scratch

```text
terraform apply  (local)  ->  deploy.yml  (dbt build)  ->  Airflow DAG  (data)
   database + 5 schemas        models inside them          batches into RAW
```

### GitHub Secrets

Six repository secrets, taken straight from your local `.env`:

| Secret | Used by |
| --- | --- |
| `SNOWFLAKE_ACCOUNT` | dbt profile, Snowflake connector |
| `SNOWFLAKE_USER` | dbt profile, Snowflake connector |
| `SNOWFLAKE_PASSWORD` | dbt profile, Snowflake connector |
| `SNOWFLAKE_ROLE` | dbt profile, Snowflake connector |
| `SNOWFLAKE_WAREHOUSE` | dbt profile, Snowflake connector |
| `SNOWFLAKE_DATABASE` | deploy target; CI clones it and builds into the copy |

Plain workflow configuration, not secrets: `DBT_TARGET_SCHEMA=BRONZE`, `DBT_THREADS=4`, and
the CI clone name. These are naming, not credentials.

Not needed by CI at all: `SNOWFLAKE_ORGANIZATION_NAME` and `SNOWFLAKE_ACCOUNT_NAME`
(Terraform provider only, and Terraform never authenticates in CI), `ALERT_EMAIL_RECIPIENT`,
the `SMTP_*` values and the `AIRFLOW_*` values (local runtime only). **CI never sends
mail** — the notification settings live in the DAG, and CI never runs a DAG, so no SMTP
credential is ever added to the repository secrets.

GitHub injects secrets as environment variables on the job. `profiles.yml.example` is copied
to `dbt/profiles.yml` on the runner and its `env_var()` calls resolve against them; nothing
is written anywhere but the runner, which is destroyed when the job ends.

One-time setup in the GitHub UI: add the six secrets under **Settings → Secrets and
variables → Actions**, and create an environment named `production` under
**Settings → Environments** with yourself as a required reviewer.

If Snowflake ever refuses the CI password sign-in — MFA enforcement for password-based
logins is tightening — the fix is key-pair authentication: one `SNOWFLAKE_PRIVATE_KEY`
secret and two lines in the profile.

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
├── data/                          landing area, written by the generation DAG
│   ├── trades_batch_NNN.json      waiting to be processed
│   ├── .generation_progress       generator cycle checkpoint (git-ignored)
│   └── archive/                   successfully processed
│
├── data_generator/
│   ├── generate_trades.py         batch generation CLI, one batch per run
│   ├── trade_scenarios.py         scenario quotas and record builders
│   ├── ingest_trades.py           PUT + COPY INTO loader
│   └── file_lifecycle.py          discovery + data/ -> archive/ move
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
│   │   ├── trade_data_generation.py   writes files into data/
│   │   └── trade_pipeline.py          find -> ingest -> dbt -> archive
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
│   └── send_failure_alert.py      standalone CLI: the Snowflake email route
│
├── tests/
│   ├── test_trade_generator.py
│   ├── test_ingestion_errors.py
│   ├── test_file_lifecycle.py
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
└── .github/
    └── workflows/
        ├── ci.yml                 PR validation + dbt build on a DB clone
        └── deploy.yml             manual, approved dbt deploy
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

**Complete.** Deliberately small: one view, one alert. See section 15 for the full
write-up.

Airflow reports its own outcome by email, using the built-in `email_on_failure` (per task,
via `default_args`) and one `on_success_callback` (per run, on the DAG). This replaced an
earlier `notify_failure` watcher task that routed the mail through Snowflake: notification
is now configuration rather than a node, so the graph keeps only the four tasks that do work
and no task has to exit non-zero to keep a failed run red. Airflow's SMTP mailer is configured with
`AIRFLOW__SMTP__*` in `docker-compose.yml`, so the project now does hold an SMTP credential
in the gitignored `.env` — CI never sends mail and never receives one. The Snowflake objects
below are separate — they watch the data, not the scheduler.

| Delivered | Where |
| --- | --- |
| `TRADE_DB.MONITORING` schema | `terraform/main.tf` — 10th resource |
| `V_PIPELINE_HEALTH` — daily health from COPY_HISTORY, QUERY_HISTORY and live GOLD | `monitoring/sql/monitoring.sql` |
| `ALERT_PIPELINE_FAILURE` — emails on a failed load or dbt node, left suspended | `monitoring/sql/monitoring.sql` |
| `TRADE_ALERT_EMAIL` notification integration — used by the data alert and the manual CLI | `monitoring/sql/monitoring.sql` |
| `email_on_failure` for failures, one success callback — silent on a short-circuited run | `airflow/dags/trade_pipeline.py` |
| `AIRFLOW__SMTP__*` configuration for Airflow's mailer | `docker-compose.yml`, `.env.example` |
| `send_failure_alert.py` — standalone CLI, the Snowflake route, not called by the DAG | `monitoring/send_failure_alert.py` |
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
failure notified correctly and left all six table counts unchanged.

---

### Phase 8 — CI/CD

**Complete.** Two workflow files. Full detail in section 17.

| Delivered | Where |
| --- | --- |
| PR validation with no credentials: credential guard, pytest, `dbt parse`, `terraform fmt`/`validate` | `.github/workflows/ci.yml`, job `validate` |
| Full `dbt build` against a zero-copy clone of `TRADE_DB`, dropped afterwards | `.github/workflows/ci.yml`, job `dbt-integration` |
| Manual, approval-gated dbt deploy | `.github/workflows/deploy.yml` |
| Six GitHub Secrets, no committed credentials | README section 17 |

Three decisions worth stating:

- **CI isolates by database, not by schema.** `generate_schema_name` returns the schema name
  literally, so models always land in BRONZE/SILVER/GOLD. Cloning the database moves the
  source and every target together, because `sources.yml` and the profile both read
  `SNOWFLAKE_DATABASE` — so CI isolation needed no change to the dbt project.
- **No `terraform apply` exists anywhere in CI.** State is local, so CI validates the
  configuration and apply stays a local operation. That is also what makes a pull request
  structurally unable to change infrastructure.
- **Deployment is manual and approved**, not automatic on merge. A README merge should not
  queue a warehouse rebuild.

Verified before first push: clone carried 800 RAW rows, `dbt build` 63 nodes passed inside
the clone, `TRADE_STORE` 700 there, clone dropped with zero leftovers, and `TRADE_DB` plus
the Phase 7 alert unchanged.

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
Phase 8   ██████████  CI/CD (GitHub Actions: PR validation + gated dbt deploy)
Phase 9   ░░░░░░░░░░  Next: Documentation + final submission
```

**Next implementation step:** Phase 9 — architecture diagram, setup and execution guide, and the final documentation pass.

Deferred by decision, each to its own pass: the optional custom trade rules (section 12); run-scoped stage paths to make RAW append-idempotent; an incremental `TRADE_STORE` and append-only `REJECTED_TRADES`; splitting `dbt build` into `dbt run` and `dbt test`; late-arriving dimensions; and the scalability design for a 10,000x increase in volume.
