-- ===========================================================================
-- Phase 7 - operational monitoring for the trade pipeline.
--
-- Creates three objects, all idempotent and safe to re-run:
--
--   1. TRADE_ALERT_EMAIL                          email notification integration
--   2. TRADE_DB.MONITORING.V_PIPELINE_HEALTH      one row per active pipeline day
--   3. TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE alert on an objective failure
--
-- The MONITORING schema itself is created by Terraform (terraform/main.tf).
-- Run this file after `terraform apply`.
--
-- WHY THIS IS SQL AND NOT TERRAFORM OR DBT
--   The view reads SNOWFLAKE.ACCOUNT_USAGE, so it is operational rather than
--   structural, and it is not part of the business transformation. Keeping it
--   as plain SQL means a change to a monitoring query can never fail
--   `terraform apply` or `dbt build`, and the SQL stays readable as SQL.
--
-- HOW TO RUN
--   Replace you@example.com in section 3 with your own address - it appears
--   exactly once - then execute the whole file in Snowsight as ACCOUNTADMIN, or:
--     snowsql -f monitoring/sql/monitoring.sql
--
-- MANUAL PREREQUISITE
--   That address must be a VERIFIED email of a user in this Snowflake account
--   (Snowsight -> profile -> verify email). SYSTEM$SEND_EMAIL refuses to deliver
--   to an unverified address.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. Notification integration
--
-- ALLOWED_RECIPIENTS is deliberately omitted: without it, Snowflake permits
-- every verified user email in the account, so the integration does not have to
-- be edited and no personal address is committed to the repository.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE NOTIFICATION INTEGRATION TRADE_ALERT_EMAIL
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Delivers trade pipeline failure notifications. Used by ALERT_PIPELINE_FAILURE and by the Airflow notify_failure task.';


-- ---------------------------------------------------------------------------
-- 2. V_PIPELINE_HEALTH
--
-- One row for every day on which the pipeline was active, answering "did the
-- pipeline run successfully?" from evidence Snowflake already records:
--
--   ACCOUNT_USAGE.COPY_HISTORY    ingestion - files, rows, load errors
--   ACCOUNT_USAGE.QUERY_HISTORY   dbt       - nodes executed, nodes failed
--   GOLD.TRADE_STORE              freshness - when GOLD was last rebuilt
--
-- IDENTIFYING DBT WITHOUT CHANGING DBT
--   dbt-snowflake prefixes every query it issues with a JSON comment containing
--   "app": "dbt" and the node_id of the model or test being run, so dbt work is
--   already distinguishable in QUERY_HISTORY. No query_tag configuration is
--   needed on our side.
--
-- SCOPING
--   The account contains other warehouses and unrelated query activity, so both
--   CTEs are scoped to this pipeline. Unscoped, the numbers would be noise.
--
-- NO "NO_RUN" STATUS
--   A day on which nothing ran simply produces no row. The case study defines no
--   source arrival frequency or SLA, so the absence of a run carries no meaning
--   and it would be wrong to report it as a problem.
--
-- LATENCY
--   ACCOUNT_USAGE is documented as lagging by up to 45 minutes for QUERY_HISTORY
--   and up to 2 hours for COPY_HISTORY. LAST_GOLD_REFRESH is read from the live
--   GOLD table instead, so it is always current.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW TRADE_DB.MONITORING.V_PIPELINE_HEALTH
    COMMENT = 'Daily operational health of the trade pipeline, derived from Snowflake administrative views. One row per day on which the pipeline was active.'
AS
WITH loads AS (

    SELECT
        CONVERT_TIMEZONE('UTC', last_load_time)::DATE            AS day,
        COUNT(*)                                                 AS files_loaded,
        SUM(COALESCE(row_count, 0))                              AS rows_loaded,
        COUNT_IF(status <> 'Loaded' OR COALESCE(error_count, 0) > 0) AS load_errors
    FROM snowflake.account_usage.copy_history
    WHERE table_catalog_name = 'TRADE_DB'
      AND table_schema_name  = 'RAW'
      AND table_name         = 'RAW_TRADES'
      AND last_load_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY 1

),

dbt_runs AS (

    SELECT
        CONVERT_TIMEZONE('UTC', start_time)::DATE                AS day,
        COUNT(*)                                                 AS dbt_nodes,
        COUNT_IF(execution_status = 'FAIL')                      AS dbt_failures
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
      AND query_text ILIKE '%"app": "dbt"%'
      AND query_text ILIKE '%"node_id"%'
    GROUP BY 1

),

gold_freshness AS (

    SELECT MAX(updated_timestamp) AS last_gold_refresh
    FROM TRADE_DB.GOLD.TRADE_STORE

)

SELECT
    COALESCE(l.day, d.day)              AS day,
    COALESCE(l.files_loaded, 0)         AS files_loaded,
    COALESCE(l.rows_loaded, 0)          AS rows_loaded,
    COALESCE(l.load_errors, 0)          AS load_errors,
    COALESCE(d.dbt_nodes, 0)            AS dbt_nodes,
    COALESCE(d.dbt_failures, 0)         AS dbt_failures,
    g.last_gold_refresh                 AS last_gold_refresh,

    -- DEGRADED means something ran and failed. That is the only judgement this
    -- view makes, and it is the condition ALERT_PIPELINE_FAILURE evaluates.
    CASE
        WHEN COALESCE(l.load_errors, 0) > 0 OR COALESCE(d.dbt_failures, 0) > 0
            THEN 'DEGRADED'
        ELSE 'HEALTHY'
    END                                 AS status

FROM loads l
FULL OUTER JOIN dbt_runs d ON d.day = l.day
CROSS JOIN gold_freshness g
ORDER BY 1 DESC;


-- ---------------------------------------------------------------------------
-- 3. ALERT_PIPELINE_FAILURE
--
-- Fires when the last 24 hours contain a DEGRADED day - that is, a COPY into
-- RAW_TRADES that did not load cleanly, or a dbt node that failed. Both are
-- objective evidence that something ran and failed.
--
-- This complements, rather than duplicates, the Airflow notify_failure task:
-- Airflow reports that a TASK failed, the alert reports that the OUTCOME in
-- Snowflake is bad - including work Airflow never orchestrated, such as a manual
-- `dbt build` or a direct load.
--
-- STATE
--   Snowflake creates alerts suspended, and this one is deliberately left
--   suspended: a resumed alert consumes credits at every evaluation, which is
--   not appropriate for a development account. To demonstrate it on demand:
--
--     EXECUTE ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE;
--     SELECT * FROM TABLE(TRADE_DB.INFORMATION_SCHEMA.ALERT_HISTORY())
--     ORDER BY scheduled_time DESC;
--
--   To run it on its schedule:
--     ALTER ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE RESUME;
--
-- MANUAL STEP: replace you@example.com below with your verified address. It is
-- the only place in this file that needs editing, and it is left as a
-- placeholder so that no personal address is committed to the repository.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE ALERT TRADE_DB.MONITORING.ALERT_PIPELINE_FAILURE
    WAREHOUSE = TRADE_WH
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Emails when a load or a dbt node failed in the last 24 hours. Left suspended in development.'
IF (EXISTS (
        SELECT 1
        FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH
        WHERE status = 'DEGRADED'
          AND day >= DATEADD(day, -1, CURRENT_DATE())
))
THEN
    CALL SYSTEM$SEND_EMAIL(
        'TRADE_ALERT_EMAIL',
        'you@example.com',
        'Trade pipeline: failure detected',
        'TRADE_DB.MONITORING.V_PIPELINE_HEALTH reports DEGRADED within the last 24 hours: a load into RAW.RAW_TRADES did not complete cleanly, or a dbt node failed. Investigate with SELECT * FROM TRADE_DB.MONITORING.V_PIPELINE_HEALTH ORDER BY day DESC, then see README section 15 for the administrative queries that identify the failing file or dbt node.'
    );
