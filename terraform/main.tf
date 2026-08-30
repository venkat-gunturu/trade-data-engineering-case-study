# Snowflake infrastructure for the trade pipeline.
#
# OWNERSHIP BOUNDARY
#   Terraform owns   containers: database, schemas, warehouse, file format,
#                    stage, and the RAW landing table.
#   dbt owns         STG_TRADES, INT_TRADE_SEQUENCE, INT_TRADE_VALIDATION,
#                    TRADE_STORE, REJECTED_TRADES. Terraform never references them.
#   Python owns      the data itself: PUT / COPY INTO.
#
# Terraform CREATES all of these from scratch. An empty Snowflake account plus
# credentials is enough: terraform init / plan / apply needs no pre-existing
# objects and no import step.
#
# No lifecycle.prevent_destroy is set, so the normal development lifecycle
# (apply, then destroy) works out of the box. For a production deployment, add
# it to the data-bearing resources - see README section 16.

resource "snowflake_database" "trade_db" {
  name         = var.database_name
  comment      = "Trade data engineering case study."
  is_transient = false
  # with_managed_access is a schema-level attribute in provider v2; it does not
  # exist on snowflake_database.
}

# Landing zone. Kept separate from the dbt schemas because it holds the stage,
# the file format, and the append-only RAW table.
resource "snowflake_schema" "raw" {
  database            = snowflake_database.trade_db.name
  name                = var.raw_schema_name
  comment             = "Landing zone. Append-only; source of truth for the original payload."
  is_transient        = false
  with_managed_access = false
}

# BRONZE / SILVER / GOLD. Terraform creates the containers; dbt builds the
# models inside them.
resource "snowflake_schema" "transform" {
  for_each = toset(var.transform_schema_names)

  database            = snowflake_database.trade_db.name
  name                = each.value
  comment             = "dbt build target. Models in this schema are owned by dbt, not Terraform."
  is_transient        = false
  with_managed_access = false
}

# Operational monitoring. Terraform owns the schema, exactly as it owns RAW and
# the dbt target schemas; monitoring/sql/monitoring.sql creates the view and the
# alert inside it.
#
# The view and the alert are deliberately kept out of Terraform state. They read
# SNOWFLAKE.ACCOUNT_USAGE, so they are operational rather than structural, and
# keeping them as plain SQL means a change to a monitoring query can never fail
# `terraform apply` or block a deployment of the pipeline itself.
resource "snowflake_schema" "monitoring" {
  database            = snowflake_database.trade_db.name
  name                = var.monitoring_schema_name
  comment             = "Operational monitoring. Views here are created by monitoring/sql/monitoring.sql, not by Terraform or dbt."
  is_transient        = false
  with_managed_access = false
}

resource "snowflake_warehouse" "trade_wh" {
  name           = var.warehouse_name
  comment        = "Compute for ingestion, dbt, and Airflow."
  warehouse_size = var.warehouse_size
  auto_suspend   = var.warehouse_auto_suspend
  auto_resume    = "true"
}

# JSON with strip_outer_array so each element of the batch array becomes one
# row in RAW_TRADES. Changing this would break COPY INTO.
resource "snowflake_file_format_json" "json_file_format" {
  database = snowflake_database.trade_db.name
  schema   = snowflake_schema.raw.name
  name     = var.file_format_name

  strip_outer_array = "true"
}

# Internal named stage that data_generator/ingest_trades.py PUTs batch files to.
resource "snowflake_stage_internal" "trade_stage" {
  database = snowflake_database.trade_db.name
  schema   = snowflake_schema.raw.name
  name     = var.stage_name

  file_format {
    format_name = "\"${var.database_name}\".\"${var.raw_schema_name}\".\"${var.file_format_name}\""
  }

  # Declared explicitly. Omitting it leaves the provider planning to remove a
  # block Snowflake reports anyway, which forces replacement of the stage.
  directory {
    enable       = false
    auto_refresh = "false"
  }
}

# Append-only landing table, written by PUT + COPY INTO and read by dbt.
#
# This is the only data-bearing object Terraform manages, and the one worth
# protecting with lifecycle.prevent_destroy in a production deployment.
#
# Column types are spelled the way Snowflake normalises them (NUMBER becomes
# NUMBER(38,0), TIMESTAMP_NTZ becomes TIMESTAMP_NTZ(9)) so that the plan after
# apply is clean rather than showing a permanent diff.
resource "snowflake_table" "raw_trades" {
  database = snowflake_database.trade_db.name
  schema   = snowflake_schema.raw.name
  name     = var.raw_table_name
  comment  = "Append-only landing table written by PUT + COPY INTO."

  # Explicit so the post-apply plan is clean; left unset the provider plans -1.
  data_retention_time_in_days = 1

  column {
    name     = "SURROGATE_KEY"
    type     = "NUMBER(38,0)"
    nullable = true

    identity {
      start_num = 1
      step_num  = 1
    }
  }

  column {
    name     = "PAYLOAD"
    type     = "VARIANT"
    nullable = false
  }

  column {
    name     = "BATCH_ID"
    type     = "VARCHAR(100)"
    nullable = false
  }

  column {
    name     = "SOURCE_FILE_NAME"
    type     = "VARCHAR(255)"
    nullable = false
  }

  column {
    name     = "INGESTED_AT"
    type     = "TIMESTAMP_NTZ(9)"
    nullable = true

    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}
