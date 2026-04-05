################################################################################
# outputs.tf — values exposed after `terraform apply`
################################################################################

output "bucket_id" {
  description = "The name of the S3 bucket."
  value       = aws_s3_bucket.wind_farm_data.id
}

output "bucket_arn" {
  description = "The ARN of the S3 bucket (use this in IAM policies)."
  value       = aws_s3_bucket.wind_farm_data.arn
}

output "bucket_regional_domain_name" {
  description = "Regional domain name — use this for pre-signed URL generation."
  value       = aws_s3_bucket.wind_farm_data.bucket_regional_domain_name
}

