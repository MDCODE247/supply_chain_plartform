terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.100"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
}

module "aws" {
  source           = "./modules/aws"
  aws_region       = var.aws_region
  s3_source_bucket = var.s3_source_bucket
}

module "snowflake" {
  source              = "./modules/snowflake"
  snowflake_warehouse = var.snowflake_warehouse
  snowflake_database  = var.snowflake_database
}
