output "s3_source_bucket" {
  description = "Source S3 bucket name"
  value       = data.aws_s3_bucket.source.bucket
}

output "s3_destination_bucket" {
  description = "Destination S3 bucket name"
  value       = aws_s3_bucket.destination.bucket
}

output "ssm_aws_access_key_arn" {
  description = "ARN of the AWS access key SSM parameter"
  value       = aws_ssm_parameter.aws_access_key.arn
}

output "ssm_aws_secret_key_arn" {
  description = "ARN of the AWS secret key SSM parameter"
  value       = aws_ssm_parameter.aws_secret_key.arn
}
