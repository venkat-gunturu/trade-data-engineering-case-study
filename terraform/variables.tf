# Non-secret naming and sizing only. Credentials never appear here - the
# provider reads them from the environment (see providers.tf).
#
# Every default matches the object that already exists in Snowflake, so a
# `terraform plan` against the current account reports no changes.

variable "database_name" {
  description = "Snowflake database holding the whole pipeline."
  type        = string
  default     = "TRADE_DB"
}

variable "raw_schema_name" {
  description = "Schema for the landing zone: stage, file format, and RAW_TRADES."
  type        = string
  default     = "RAW"
}

variable "transform_schema_names" {
  description = "Schemas that dbt builds into. Terraform owns the schemas; dbt owns the models inside them."
  type        = list(string)
  default     = ["BRONZE", "SILVER", "GOLD"]
}

variable "monitoring_schema_name" {
  description = "Schema holding the operational monitoring view. Terraform owns the schema; monitoring/sql/monitoring.sql owns the view."
  type        = string
  default     = "MONITORING"
}

variable "warehouse_name" {
  description = "Virtual warehouse used by the loader, dbt, and Airflow."
  type        = string
  default     = "TRADE_WH"
}

variable "warehouse_size" {
  description = "Warehouse compute size. XSMALL is sufficient for this case study."
  type        = string
  default     = "XSMALL"
}

variable "warehouse_auto_suspend" {
  description = "Seconds of inactivity before the warehouse suspends. Keeps credit usage low."
  type        = number
  default     = 60
}

variable "file_format_name" {
  description = "JSON file format used by COPY INTO. strip_outer_array is load-critical."
  type        = string
  default     = "JSON_FILE_FORMAT"
}

variable "stage_name" {
  description = "Internal named stage that PUT uploads batch files to."
  type        = string
  default     = "TRADE_STAGE"
}

variable "raw_table_name" {
  description = "Append-only landing table. The only data-bearing object Terraform manages."
  type        = string
  default     = "RAW_TRADES"
}

variable "alert_email_recipient" {
  description = "Verified email address allowed to receive Snowflake monitoring alerts."
  type        = string
  sensitive   = true
}