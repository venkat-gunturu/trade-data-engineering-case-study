No changes made. Here's the walkthrough.

1. Repository folder structure

trade-data-engineering-case-study/
├── data/                       landing area (empty right now)
│   └── archive/                4 processed files
├── data_generator/             Python: generation, ingestion, file lifecycle
├── dbt/                        transformations + business rules
├── airflow/                    DAGs + the container image
├── terraform/                  Snowflake infrastructure
├── monitoring/sql/             one view + one alert
├── tests/                      pytest (47 tests)
├── snowflake/sql/              historical Phase 1 record — NOT part of setup
├── docs/case-study/            the original brief
├── .github/workflows/          ci.yml, deploy.yml
├── docker-compose.yml, requirements.txt, .env.example, README.md
Two things that mislead newcomers: snowflake/sql/ is dead weight kept for history (Terraform provisions everything now), and dbt/profiles.yml + .env exist on your disk but are gitignored — only the .example versions are committed.

2. Where the trade JSON files are generated
data/ — always, and only there. Set by OUTPUT_DIR in generate_trades.py:25, which resolves to <project>/data. Inside the container that path is /opt/airflow/project/data, bind-mounted to your ./data, so files written by Airflow appear on your host immediately.

Files are plain .json. The .gz you see in Snowflake is created in flight by PUT ... AUTO_COMPRESS; nothing local is ever gzipped.

3. How the data generator works
Two files:

trade_scenarios.py — the business content. It holds SCENARIO_PLAN, a fixed quota per batch:

"BATCH_001": {}                                                    # 200 brand-new trades
"BATCH_002": {"higher_version": 15, "same_version": 15, "past_maturity": 5}
"BATCH_003": {"lower_version": 15, "same_version": 10, "past_maturity": 10}
"BATCH_004": {"higher_version": 20, "same_version": 5,  "past_maturity": 5}
Each batch fills its quota, then pads to 200 with new trades. Attributes come from Faker with a seeded RNG, so the values look realistic but the scenarios are guaranteed — every business rule always has data exercising it, regardless of seed.

Only past_maturity records and padding allocate new trade_ids; higher/same/lower reuse existing ones. That's why 800 records carry only 720 distinct trade IDs.

generate_trades.py — the CLI wrapper. Calls build_batches(), writes each batch as a JSON array to data/trades_batch_NNN.json. Flags: --seed, --records-per-batch, --as-of-date, --output-dir.
One run writes all four files. There's no "generate one file" mode. --as-of-date anchors every generated date, so the same seed + same date reproduces byte-identical files.

4. The two DAGs
They share only a directory. No Airflow dependency, no sensor, no trigger between them.

trade_data_generation — airflow/dags/trade_data_generation.py
One task, generate_trades. Runs generate_trades.py --seed --records-per-batch --as-of-date {{ ds }}. schedule=None (manual). Params: seed, records_per_batch.

trade_pipeline — airflow/dags/trade_pipeline.py

find_files → ingest_trades → dbt_build → archive_files
Task	Type	Does
find_files	ShortCircuitOperator	Lists data/trades_batch_*.json; returns the list
ingest_trades	BashOperator	ingest_trades.py --file <the discovered list> → PUT + COPY INTO
dbt_build	BashOperator	dbt build --profiles-dir . — all models and tests
archive_files	BashOperator	file_lifecycle.py --file <same list> → moves them to data/archive/
schedule="*/30 * * * *", retries=2, max_active_runs=1. Only param is force_reload. There is no batch parameter — nothing in this DAG names a batch.

5. How Airflow decides which files to process
find_files_to_process() calls discover_files() from file_lifecycle.py — shared with the loader and the tests, so they can never disagree.


directory.glob("trades_batch_*.json")     # glob, NOT rglob
That single word is the whole safety mechanism. data/archive/ lives inside data/; a recursive scan would re-discover every processed file on every run, forever. glob only looks one level deep.

ShortCircuitOperator uses the return value two ways at once:

as the condition — empty list is falsy → downstream skipped
as XCom — the list is published, and both ingest_trades and archive_files read it via {{ ti.xcom_pull(task_ids='find_files') | join(' ') }}
So the run acts on the exact snapshot taken at the start. If the generation DAG drops a new file midway through, that file is not ingested and not archived — it waits for the next run. This is why the DAG passes explicit paths instead of letting each task re-glob.

Zero files
find_files logs No files available for processing. and returns []. The three downstream tasks are marked skipped. Airflow derives DAG-run state from leaf tasks, and skipped leaves count as fine — so the run is SUCCESS. No Snowflake connection, no warehouse spin-up, no cost. This is a normal outcome every 30 minutes, not an error.

6. How files move to data/archive/
archive_files is the last task, downstream of both ingestion and dbt. That ordering is the guarantee.

Outcome	data/	archive/	DAG
Everything succeeds	emptied	files land	SUCCESS
Ingestion fails	files stay	untouched	FAILED
dbt fails	files stay	untouched	FAILED
archive_files never runs unless both upstream tasks succeeded, so a failed file is physically incapable of being archived. Nothing is ever deleted — the next scheduled run picks the file up and retries it.

The move itself is path.replace(target) — an atomic rename, and it overwrites a same-named file from an earlier cycle, so archive/ holds the most recent copy of each batch.

7. How files get into Snowflake RAW
ingest_trades.py, per file:


1. PUT 'file://…/trades_batch_001.json' @TRADE_DB.RAW.TRADE_STAGE OVERWRITE = TRUE, AUTO_COMPRESS = TRUE
2. COPY INTO TRADE_DB.RAW.RAW_TRADES (payload, batch_id, source_file_name, ingested_at)
   FROM (SELECT $1, 'BATCH_001', METADATA$FILENAME, CURRENT_TIMESTAMP() FROM @stage/…)
   FILE_FORMAT = (FORMAT_NAME = TRADE_DB.RAW.JSON_FILE_FORMAT)
   ON_ERROR = ABORT_STATEMENT  FORCE = FALSE
The staged file is left in place afterwards. Three details worth internalising:

OVERWRITE = TRUE. Batch file names repeat every generation cycle, so without it PUT would report SKIPPED for a name already on the stage and COPY INTO would load the previous cycle's bytes.
The staged file is retained after the load, on purpose. It costs almost nothing and leaves the exact bytes Snowflake loaded available for inspection when reconciling a load after the fact. OVERWRITE means the next cycle replaces it rather than skipping.
ON_ERROR = ABORT_STATEMENT means a bad file loads nothing. FORCE = FALSE means Snowflake's load history skips a file it has already loaded, printing COPY INTO skipped and returning 0 rows.
The file format uses STRIP_OUTER_ARRAY, so each element of the JSON array becomes one row. RAW_TRADES stores the whole trade as an unparsed VARIANT plus surrogate_key (auto-increment arrival order), batch_id, source_file_name, ingested_at.

The batch_id is derived from the filename (trades_batch_001.json → BATCH_001) — it's a label on the row, not something the DAG chooses.

8–9. The dbt layers and models

RAW      VARIANT, schema-on-read     accepts whatever the source sent
BRONZE   typed CAST                  the type gate — bad values fail here
SILVER   sequencing, then rules      classify every record
GOLD     current state + audit
Model	Layer	In simple terms
stg_trades	BRONZE	Pulls 13 fields out of the JSON and casts them. No logic at all. One row in, one row out
int_trade_sequence	SILVER	Reconstructs arrival order. Two window functions per trade: prior_version (highest version seen before this row) and previously_established (did an earlier row reach the store?)
int_trade_validation	SILVER	Applies the rules. Sets validation_status = ACCEPTED/REJECTED plus a rejection_reason. Filters nothing — all 800 rows survive, so the audit trail is complete
trade_store	GOLD	Keeps ACCEPTED rows, ranks each trade, keeps rank 1 → one row per trade
rejected_trades	GOLD	Projects the REJECTED rows with their reason and metadata
Why int_trade_sequence exists at all: the window functions let each row see the state that existed when it arrived, without storing that state anywhere. That's why every model can be a plain full-refresh table rather than an incremental model — the whole history is re-derived from RAW each run.

All five are materialized: table. The generate_schema_name macro is overridden so +schema: SILVER lands in SILVER, not dbt's default BRONZE_SILVER.

10. TRADE_STORE and REJECTED_TRADES
GOLD.TRADE_STORE — current state, exactly one row per trade_id (700 rows). The winner is picked by:


row_number() over (partition by trade_id
                   order by trade_version desc, event_timestamp desc, surrogate_key desc)
… where current_record_rank = 1
Three tie-breakers, so the result is deterministic even with identical timestamps. It also carries status: EXPIRED if maturity_date < current_date(), else VALID.

GOLD.REJECTED_TRADES — the compliance table (35 rows). Every rejected record with its reason, batch, source file and rejected_at. Separate table, as the case study requires.

They don't sum to 800 and shouldn't: 800 records → 765 accepted → collapsed into 700 current rows; 35 rejected sit in the audit table.

11. The five business rules, with one worked example
Take trade T100296 across the batches:

Arrives	Version	Rule	Outcome
BATCH_001	v1, matures 2027	new trade	ACCEPTED, enters store at v1
BATCH_002	v2	higher version	ACCEPTED, store now v2
BATCH_002	v2 again, new price	Rule 2 — same version replaces	ACCEPTED; later event_timestamp wins the ranking, so the new price is what's stored
BATCH_003	v1	Rule 1 — lower version	REJECTED LOWER_VERSION; store still holds v2
And separately:

Rule 3 — past maturity. A trade arriving with maturity_date < ingested_at::date that was never established → REJECTED PAST_MATURITY. 20 of these; none reach the store.
Rule 4 — expiration. A trade already in the store whose maturity date later passes flips to EXPIRED.
Rule 5 — audit. All 35 rejections land in REJECTED_TRADES.
Precedence matters: Rule 3 is checked before Rule 1. An intrinsic property of the record beats a comparison against stored state. A v1 record that is both stale and past maturity is reported as PAST_MATURITY.

Why TRADE_STORE shows 0 EXPIRED, and why that's correct: Rule 3 rejects when maturity < ingested date; Rule 4 expires when maturity < current date. On a fresh load ingested_at is today, so a trade is either rejected by Rule 3 or not yet matured. No amount of loading can produce an EXPIRED row — it needs elapsed time. The earliest maturity in the store is 2026-09-29, so they'll appear naturally from then. This is why Rule 4 is proven by a dbt unit test instead.

12. The pytest tests — 47 total
File	Count	What it proves
test_trade_generator.py	19	Every rule scenario is present regardless of seed; a seed reproduces a run exactly; the JSON shape is right
test_ingestion_errors.py	9	Loader failure paths: missing file, unparseable batch id, missing env vars, COPY errors raised not swallowed
test_file_lifecycle.py	18	Discovery and archiving, plus guards on the design: archive/ never rediscovered, no hardcoded batch, and the graph is only the four tasks that do work
test_send_failure_alert.py	10	The standalone Snowflake CLI never lets a failed run look successful: non-zero exit on every path, missing recipient reported not swallowed, a broken integration logged not raised, and the failed task named
These never touch Snowflake. The connector is stubbed at import; everything else runs against tmp_path. That's deliberate — they run in CI with no credentials and in under a second.

The gap they leave is honest: they cover Python behaviour. Correctness of the business rules is covered by dbt, not here.

13. What "63 dbt nodes PASS" means
dbt build runs models and their tests interleaved, and counts each as a node:


 5 models       stg_trades, int_trade_sequence, int_trade_validation,
                trade_store, rejected_trades
45 data tests   not_null / unique / accepted_values from schema.yml,
                plus 2 singular tests in dbt/tests/
13 unit tests   rule logic asserted against fixture rows
── 
63 nodes
The 13 unit tests are the important ones. They feed hand-written rows into a model and assert the output, so they're deterministic, need no warehouse data, and can build states that loading can't reach — including Rule 4's EXPIRED. They also mean dbt build stays green on a fresh clone with an empty RAW.

dbt build stops at the first failure and skips everything downstream, so a bad model never produces a half-built GOLD.

14. Terraform — exactly 10 resources
terraform/main.tf, provider snowflakedb/snowflake v2.20.0:


snowflake_database.trade_db                    TRADE_DB
snowflake_schema.raw                           RAW
snowflake_schema.transform["BRONZE"|"SILVER"|"GOLD"]     3 resources
snowflake_schema.monitoring                    MONITORING
snowflake_warehouse.trade_wh                   TRADE_WH  (XSMALL, auto-suspend 60s)
snowflake_file_format_json.json_file_format    JSON_FILE_FORMAT (strip_outer_array)
snowflake_stage_internal.trade_stage           TRADE_STAGE
snowflake_table.raw_trades                     RAW.RAW_TRADES
Terraform owns containers, not content. It creates the five schemas but not the models inside them — dbt does that. It creates RAW_TRADES (the only data-bearing object) but never the data.

One line in providers.tf is load-bearing: warehouse = "". Terraform only issues DDL; if it inherited SNOWFLAKE_WAREHOUSE=TRADE_WH it would try to connect to the warehouse it's creating, which fails on an empty account.

State is local (terraform/terraform.tfstate, gitignored). There is no remote backend. Consequence: plan/apply only work from your machine.

15. Monitoring and the failure alert
Two separate things answering two different questions.

"Did a task fail?" → Airflow itself, over SMTP. Failures need no code: email_on_failure in default_args is Airflow's built-in, so every task inherits it and one added later can't be forgotten. Success is one function in the DAG file, send_dag_success_email, wired as on_success_callback. Both send through Airflow's mailer, configured with AIRFLOW__SMTP__* in docker-compose.yml, to ALERT_EMAIL_RECIPIENT. Two details: with retries=2 the failure mail fires once after the final attempt, not per retry, because a task with retries left goes to up_for_retry rather than failed; and a short-circuited run (empty data/) sends nothing, or a 30-minute schedule would mail 48 times a day to say nothing happened. This replaced an earlier notify_failure watcher task — notification is configuration rather than a node, so the graph is just the four tasks that do work and nothing has to exit non-zero to keep a failed run red. monitoring/send_failure_alert.py survives as a standalone CLI: the Snowflake SYSTEM$SEND_EMAIL route, kept as a second path but not called by the DAG.

"Is the data wrong?" → Snowflake. monitoring/sql/monitoring.sql creates:

MONITORING.V_PIPELINE_HEALTH — one row per active day: files_loaded, rows_loaded, load_errors, dbt_nodes, dbt_failures, last_gold_refresh, status. Built from ACCOUNT_USAGE.COPY_HISTORY, ACCOUNT_USAGE.QUERY_HISTORY (dbt is identifiable because dbt-snowflake stamps every query with "app": "dbt" and a node_id), and the live GOLD.TRADE_STORE.
ALERT_PIPELINE_FAILURE — fires when the last 24h contain a DEGRADED day. Left suspended; a running alert costs credits. Run it on demand with EXECUTE ALERT.
TRADE_ALERT_EMAIL — Snowflake's own email integration for that alert.
A day with no activity produces no row and there is no NO_RUN status — with no arrival SLA defined, absence carries no meaning.

16. CI — .github/workflows/ci.yml
Runs on every pull request and every push to main. Two jobs.

validate — no credentials, ~1 minute. A guard that fails if .env/profiles.yml/tfstate/tfvars are tracked; pytest; dbt parse (validates every model, ref and macro offline — the Snowflake variables are dummy strings, so this job structurally cannot reach the warehouse); terraform fmt -check; terraform init -backend=false + validate.

dbt-integration — the clever bit.


CREATE OR REPLACE DATABASE TRADE_DB_CI_<run_id> CLONE TRADE_DB;   -- zero-copy, instant
dbt build   with SNOWFLAKE_DATABASE = TRADE_DB_CI_<run_id>
DROP DATABASE TRADE_DB_CI_<run_id>;                               -- if: always()
This needed no dbt change, because sources.yml and the profile both read SNOWFLAKE_DATABASE — pointing that one variable at the clone moves the source and every model target. TRADE_DB is neither read nor written by CI.

There is no terraform apply anywhere in CI. A pull request structurally cannot change infrastructure.

17. CD — .github/workflows/deploy.yml
workflow_dispatch only — merging to main never deploys. Bound to the production GitHub Environment, so it pauses for approval if you've configured required reviewers.

One job: install deps, generate dbt/profiles.yml from the example, dbt build --profiles-dir . against the real TRADE_DB. It runs no Terraform and loads no data.

18. Running the whole project locally

# --- one-time setup ---------------------------------------------------------
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env                       # then fill in real values
cp dbt/profiles.yml.example dbt/profiles.yml

# --- infrastructure ---------------------------------------------------------
cd terraform
terraform init
terraform plan                             # expect 10 to add on a fresh account
terraform apply
cd ..

# --- monitoring (once, after apply) -----------------------------------------
# edit the one you@example.com in monitoring/sql/monitoring.sql, then run it
# in Snowsight as ACCOUNTADMIN

# --- option A: run the pipeline by hand -------------------------------------
python data_generator/generate_trades.py           # writes data/trades_batch_00N.json
python data_generator/ingest_trades.py             # PUT + COPY INTO everything waiting
cd dbt && dbt build --profiles-dir . && cd ..
python data_generator/file_lifecycle.py --file data/trades_batch_001.json

# --- option B: run it through Airflow ---------------------------------------
docker compose build
docker compose up -d
docker compose ps                                  # postgres, scheduler, webserver healthy
# http://localhost:8080  (airflow / airflow)
#   1. unpause + trigger  trade_data_generation   -> files appear in data/
#   2. unpause + trigger  trade_pipeline          -> 4 green tasks, files -> data/archive/
#   3. trigger trade_pipeline again               -> 3 tasks skipped, run SUCCESS

# --- verification -----------------------------------------------------------
pytest tests/ -q                                   # 47 passed
cd dbt && dbt build --profiles-dir .               # 63 nodes PASS
terraform -chdir=terraform plan                    # No changes

docker compose down                                # stop (add -v to drop the metadata db)
19. Configuration files you may need to change
.env — gitignored, the only file with real secrets
Value	Controls	Comes from	If wrong
SNOWFLAKE_ACCOUNT	Connection for Python + dbt	Snowsight account identifier	Every Snowflake call fails to connect
SNOWFLAKE_USER / PASSWORD / ROLE	Authentication	Your account	Auth error; ACCOUNTADMIN is needed for ACCOUNT_USAGE
SNOWFLAKE_WAREHOUSE	Compute	Terraform creates TRADE_WH	Queries fail or silently use a different warehouse's credits
SNOWFLAKE_DATABASE	Both dbt's target and the source database	TRADE_DB	Points dbt at the wrong database — models build somewhere unexpected
SNOWFLAKE_SCHEMA	PUT/COPY target schema	RAW	Ingestion writes to the wrong schema
SNOWFLAKE_ORGANIZATION_NAME / ACCOUNT_NAME	Terraform only — provider v2 replaced the single identifier	Split your account id on the hyphen	terraform plan cannot authenticate
DBT_TARGET_SCHEMA	dbt's default schema	BRONZE	Models without an explicit +schema land wrong
ALERT_EMAIL_RECIPIENT	Who gets the notification emails — shared by Airflow and the Snowflake sender	A verified email of a user in your Snowflake account, so both senders can use it	The recipient list is empty and no mail is attempted; the DAG still fails visibly
SMTP_HOST / PORT / STARTTLS / USER / PASSWORD / MAIL_FROM	Airflow's mailer, mapped onto AIRFLOW__SMTP__* by docker-compose	Gmail: smtp.gmail.com, 587, True, and a 16-char App Password (not your account password)	No emails; the DAG still runs and a failure is still red in the UI
AIRFLOW_WWW_USER/PASSWORD	Local UI login	Yours to choose	Can't log in to localhost:8080
AIRFLOW_UID	File ownership on Linux hosts	id -u	Files written into data/ are owned by root
dbt/profiles.yml — gitignored
Every value is an env_var() read from your shell. dbt does not read .env — you must export the variables (docker-compose does it for you inside the container). If a variable is missing, dbt fails at parse time with Env var required but not provided.

terraform/terraform.tfvars — optional, gitignored
Only needed to override defaults in variables.tf (database name, warehouse size, auto-suspend). Renaming database_name here without changing .env splits Terraform and dbt across two databases.

docker-compose.yml
The bind mounts are the contract: ./data, ./dbt, ./data_generator → /opt/airflow/project/.... Removing or renaming the ./data mount breaks the whole file lifecycle — Airflow would write into the container's own filesystem and you'd never see the files. DBT_TARGET_PATH: /tmp/dbt_target keeps dbt's cache off the Windows host; removing it makes dbt fail inside the container with path errors.

dbt/dbt_project.yml
The folder→schema mapping. Changing +schema: SILVER moves models to a different schema; sources.yml and the monitoring view still point at the old names, so things quietly break.

airflow/dags/trade_pipeline.py
The schedule="*/30 * * * *" line. Set it to None to stop scheduled runs.

monitoring/sql/monitoring.sql
Contains you@example.com once. Run it unchanged and the alert is created but cannot deliver.

GitHub Secrets — six of them
SNOWFLAKE_ACCOUNT, USER, PASSWORD, ROLE, WAREHOUSE, DATABASE. Missing any one and the dbt-integration CI job fails at the clone step.

20. End-to-end diagram

   ┌──────────────────────────────┐
   │  DAG 1: trade_data_generation│   schedule=None (manual)
   │  └── generate_trades         │
   └──────────────┬───────────────┘
                  │ writes 4 files
                  ▼
        ┌──────────────────────┐
        │  data/               │  trades_batch_001..004.json
        └──────────┬───────────┘
                   │
   ┌───────────────▼────────────────────────────────────────────┐
   │  DAG 2: trade_pipeline          schedule = */30 * * * *    │
   │                                                            │
   │  find_files ─── empty? ──► skip everything, DAG = SUCCESS   │
   │      │ non-empty                                           │
   │      ▼                                                     │
   │  ingest_trades   PUT ──► TRADE_STAGE ──► COPY INTO          │
   │      │                                                     │
   │      ▼                                                     │
   │  dbt_build                                                 │
   │      │                                                     │
   │      ▼                                                     │
   │  archive_files   data/  ──►  data/archive/                 │
   └────────────────────────────────────────────────────────────┘
                   │
                   ▼
   ┌────────────────────────────────────────────────────────┐
   │  SNOWFLAKE  TRADE_DB                                   │
   │                                                        │
   │  RAW.RAW_TRADES      800   VARIANT payload, append-only│
   │        │ stg_trades                                    │
   │  BRONZE.STG_TRADES   800   typed — the type gate       │
   │        │ int_trade_sequence                            │
   │  SILVER.INT_TRADE_SEQUENCE   800   prior_version        │
   │        │ int_trade_validation                          │
   │  SILVER.INT_TRADE_VALIDATION 800   765 ok / 35 rejected│
   │        ├──────────────┬────────────────────────────────│
   │        ▼              ▼                                │
   │  GOLD.TRADE_STORE  GOLD.REJECTED_TRADES                │
   │       700 rows          35 rows                        │
   │   one per trade_id   reason + audit metadata           │
   └────────────────────────────────────────────────────────┘

   Terraform → creates TRADE_DB, 5 schemas, warehouse, stage, file format, RAW_TRADES
   MONITORING.V_PIPELINE_HEALTH + ALERT_PIPELINE_FAILURE watch the result
The three things to remember when maintaining this: discovery is non-recursive (that's what protects archive/); a file moves to archive/ only after both ingestion and dbt succeed; and zero files is a successful run, not a failure.