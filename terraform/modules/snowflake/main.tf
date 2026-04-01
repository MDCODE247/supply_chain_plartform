terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.100"
    }
  }
}

# Snowflake Warehouse
resource "snowflake_warehouse" "supplychain_wh" {
  name           = var.snowflake_warehouse
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

# Snowflake Database
resource "snowflake_database" "supplychain360" {
  name    = var.snowflake_database
  comment = "Database for supplychain360 pipeline"
}

# Schemas
resource "snowflake_schema" "raw" {
  database = snowflake_database.supplychain360.name
  name     = "RAW"
  comment  = "Raw data from S3"
}

resource "snowflake_schema" "sheets" {
  database = snowflake_database.supplychain360.name
  name     = "SHEETS"
  comment  = "Data from Google Sheets"
}

resource "snowflake_schema" "supabase" {
  database = snowflake_database.supplychain360.name
  name     = "SUPABASE"
  comment  = "Data from Supabase PostgreSQL"
}
