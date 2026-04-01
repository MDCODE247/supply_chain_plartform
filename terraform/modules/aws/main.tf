# Source S3 bucket (already exists, just referencing it)
data "aws_s3_bucket" "source" {
  bucket = var.s3_source_bucket
}

# Destination S3 bucket (Parquet output)
resource "aws_s3_bucket" "destination" {
  bucket = "supplychain360-parquet-eu-west-2"

  tags = {
    Project     = "supplychain360"
    Environment = "dev"
  }
}

# Block public access on destination bucket
resource "aws_s3_bucket_public_access_block" "destination" {
  bucket = aws_s3_bucket.destination.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSM Parameter Store - AWS credentials
resource "aws_ssm_parameter" "aws_access_key" {
  name  = "/supplychain360/aws_access_key"
  type  = "SecureString"
  value = "placeholder"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "aws_secret_key" {
  name  = "/supplychain360/aws_secret_key"
  type  = "SecureString"
  value = "placeholder"
  lifecycle { ignore_changes = [value] }
}

# SSM Parameter Store - Snowflake credentials
resource "aws_ssm_parameter" "snowflake_user" {
  name  = "/supplychain360/snowflake_user"
  type  = "SecureString"
  value = "placeholder"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "snowflake_password" {
  name  = "/supplychain360/snowflake_password"
  type  = "SecureString"
  value = "placeholder"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "snowflake_account" {
  name  = "/supplychain360/snowflake_account"
  type  = "String"
  value = "igvccsc-eq80017"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "snowflake_warehouse" {
  name  = "/supplychain360/snowflake_warehouse"
  type  = "String"
  value = "SUPPLYCHAIN_WH"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "snowflake_database" {
  name  = "/supplychain360/snowflake_database"
  type  = "String"
  value = "SUPPLYCHAIN360"
  lifecycle { ignore_changes = [value] }
}

resource "aws_ssm_parameter" "snowflake_role" {
  name  = "/supplychain360/snowflake_role"
  type  = "String"
  value = "SYSADMIN"
  lifecycle { ignore_changes = [value] }
}
