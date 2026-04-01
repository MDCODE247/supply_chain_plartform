output "snowflake_database" {
  description = "Snowflake database name"
  value       = snowflake_database.supplychain360.name
}

output "snowflake_schemas" {
  description = "Snowflake schemas created"
  value       = [
    snowflake_schema.raw.name,
    snowflake_schema.sheets.name,
    snowflake_schema.supabase.name
  ]
}

output "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.supplychain_wh.name
}
