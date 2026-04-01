# AWS Variables
variable "aws_region" {
  description = "AWS region"
  default     = "eu-west-2"
}

variable "aws_access_key" {
  description = "AWS access key"
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key"
  sensitive   = true
}

variable "s3_source_bucket" {
  description = "Source S3 bucket name"
  default     = "supplychain360-data"
}

# Snowflake Variables
variable "snowflake_organization" {
  description = "Snowflake organization name"
  default     = "IGVCCSC"
}

variable "snowflake_account_name" {
  description = "Snowflake account name"
  default     = "EQ80017"
}

variable "snowflake_user" {
  description = "Snowflake username"
  default     = "MOHAMMED007"
}

variable "snowflake_password" {
  description = "Snowflake password"
  sensitive   = true
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse"
  default     = "SUPPLYCHAIN_WH"
}

variable "snowflake_database" {
  description = "Snowflake database"
  default     = "SUPPLYCHAIN360"
}

variable "snowflake_role" {
  description = "Snowflake role"
  default     = "SYSADMIN"
}
