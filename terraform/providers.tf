terraform {
  # Import blocks (see imports.tf) require Terraform 1.5 or newer.
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.20"
    }
  }

  # Local state for this case study. terraform.tfstate is gitignored.
  # A real deployment would use an encrypted remote backend - see README section 16.
}

provider "snowflake" {
  # No credentials are declared here, or in any other committed file.
  # The provider reads them from the environment (see .env.example):
  #
  #   SNOWFLAKE_ORGANIZATION_NAME   organization half of the account identifier
  #   SNOWFLAKE_ACCOUNT_NAME        account half of the account identifier
  #   SNOWFLAKE_USER
  #   SNOWFLAKE_PASSWORD
  #   SNOWFLAKE_ROLE
  #
  # Provider v2 replaced the single SNOWFLAKE_ACCOUNT variable with the
  # organization/account pair. SNOWFLAKE_ACCOUNT is still used by the Python
  # loader and by dbt, so it stays in .env untouched.

  # Terraform runs DDL only (CREATE DATABASE / SCHEMA / WAREHOUSE / TABLE ...),
  # which Snowflake executes without compute. The warehouse is pinned to empty on
  # purpose: .env sets SNOWFLAKE_WAREHOUSE=TRADE_WH for the Python loader and dbt,
  # and the provider would otherwise inherit it and try to connect to the very
  # warehouse Terraform is about to create. On an empty account that fails with
  # "requested warehouse does not exist".
  warehouse = ""

  # These resources are preview features in provider v2. The provider refuses to
  # use them unless they are opted into explicitly.
  # snowflake_stage_internal is NOT listed: it was promoted to stable in v2.20,
  # and listing a stable feature here produces a warning.
  preview_features_enabled = [
    "snowflake_table_resource",
    "snowflake_file_format_json_resource",
    "snowflake_email_notification_integration_resource",
  ]
}
