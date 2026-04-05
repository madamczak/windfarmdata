################################################################################
# variables.tf — input variables for the wind farm S3 bucket
################################################################################

variable "aws_region" {
  description = "AWS region where the S3 bucket will be created."
  type        = string
  default     = "eu-west-1"  # Ireland — change to your preferred region
}

variable "bucket_name" {
  description = <<-EOT
    Globally unique name for the S3 bucket.
    Must be lowercase, 3–63 characters, no underscores.
    Example: "windfarmdata-parquet-prod"
  EOT
  type        = string
  # No default — must be supplied via terraform.tfvars or -var flag
}

variable "cors_allowed_origins" {
  description = <<-EOT
    List of origins allowed to make cross-origin requests to the bucket
    (used for pre-signed URL downloads from the Vue frontend).
    Example: ["http://localhost:5173", "https://your-frontend-domain.com"]
  EOT
  type        = list(string)
  default     = ["http://localhost:5173"]
}

variable "common_tags" {
  description = "Tags applied to every resource created by this module."
  type        = map(string)
  default = {
    Project     = "windfarmdata"
    ManagedBy   = "terraform"
    Environment = "production"
  }
}

