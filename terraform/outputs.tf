output "s3_source_bucket" {
  description = "Source S3 bucket name"
  value       = module.aws.s3_source_bucket
}

output "s3_destination_bucket" {
  description = "Destination S3 bucket name"
  value       = module.aws.s3_destination_bucket
}

output "snowflake_database" {
  description = "Snowflake database name"
  value       = module.snowflake.snowflake_database
}

output "snowflake_schemas" {
  description = "Snowflake schemas created"
  value       = module.snowflake.snowflake_schemas
}
