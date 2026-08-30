# Fully-qualified names of the objects Terraform manages. Useful as a quick
# confirmation of the ownership boundary, and consumable by later phases.

output "database" {
  description = "Database managed by Terraform."
  value       = snowflake_database.trade_db.name
}

output "raw_schema" {
  description = "Landing-zone schema."
  value       = "${snowflake_database.trade_db.name}.${snowflake_schema.raw.name}"
}

output "transform_schemas" {
  description = "Schemas dbt builds into. Terraform owns the schemas, dbt owns the models."
  value       = [for s in snowflake_schema.transform : "${snowflake_database.trade_db.name}.${s.name}"]
}

output "monitoring_schema" {
  description = "Schema for the operational monitoring view. Terraform owns the schema; monitoring/sql/monitoring.sql owns the view and the alert."
  value       = "${snowflake_database.trade_db.name}.${snowflake_schema.monitoring.name}"
}

output "warehouse" {
  description = "Warehouse used by the loader, dbt, and Airflow."
  value       = snowflake_warehouse.trade_wh.name
}

output "stage" {
  description = "Internal named stage that ingest_trades.py PUTs to."
  value       = "${snowflake_database.trade_db.name}.${snowflake_schema.raw.name}.${snowflake_stage_internal.trade_stage.name}"
}

output "file_format" {
  description = "JSON file format used by COPY INTO."
  value       = "${snowflake_database.trade_db.name}.${snowflake_schema.raw.name}.${snowflake_file_format_json.json_file_format.name}"
}

output "raw_table" {
  description = "Append-only landing table. The only data-bearing object Terraform manages."
  value       = "${snowflake_database.trade_db.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_trades.name}"
}

output "dbt_owned_models" {
  description = "Deliberately NOT managed by Terraform - dbt creates and owns these."
  value = [
    "TRADE_DB.BRONZE.STG_TRADES",
    "TRADE_DB.SILVER.INT_TRADE_SEQUENCE",
    "TRADE_DB.SILVER.INT_TRADE_VALIDATION",
    "TRADE_DB.GOLD.TRADE_STORE",
    "TRADE_DB.GOLD.REJECTED_TRADES",
  ]
}
