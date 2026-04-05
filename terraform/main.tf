################################################################################
# main.tf — Wind Farm Data — S3 bucket for parquet file storage
#
# This file provisions:
#   - An S3 bucket for storing per-farm parquet files
#   - Server-side encryption (AES-256) enabled by default
#   - Versioning enabled so older parquet files can be recovered
#   - Public access fully blocked (all four block-public-access flags)
#   - A lifecycle rule that moves objects to S3 Glacier after 90 days
#     and expires them after 365 days (adjust to taste)
#
# Usage:
#   terraform init
#   terraform plan
#   terraform apply
#
# To destroy all resources:
#   terraform destroy
################################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Optional: uncomment and fill in if you need a named profile from ~/.aws/credentials
  # profile = "default"
}

# ------------------------------------------------------------------------------
# S3 Bucket
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "wind_farm_data" {
  bucket = var.bucket_name

  tags = merge(var.common_tags, {
    Name = var.bucket_name
  })
}

# ------------------------------------------------------------------------------
# Block all public access
# ------------------------------------------------------------------------------

resource "aws_s3_bucket_public_access_block" "wind_farm_data" {
  bucket = aws_s3_bucket.wind_farm_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------------------
# Versioning
# ------------------------------------------------------------------------------

resource "aws_s3_bucket_versioning" "wind_farm_data" {
  bucket = aws_s3_bucket.wind_farm_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ------------------------------------------------------------------------------
# Server-side encryption (AES-256 — no extra cost, no KMS dependency)
# Switch to aws:kms and provide a kms_master_key_id for stricter key management
# ------------------------------------------------------------------------------

resource "aws_s3_bucket_server_side_encryption_configuration" "wind_farm_data" {
  bucket = aws_s3_bucket.wind_farm_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

    # Ensure every object uses the bucket-level default encryption
    bucket_key_enabled = false
  }
}

# ------------------------------------------------------------------------------
# Lifecycle rules — move cold parquet files to cheaper storage over time
# ------------------------------------------------------------------------------

resource "aws_s3_bucket_lifecycle_configuration" "wind_farm_data" {
  bucket = aws_s3_bucket.wind_farm_data.id

  rule {
    id     = "archive-and-expire-parquet"
    status = "Enabled"

    filter {
      prefix = ""  # applies to all objects in the bucket
    }

    # Move to Glacier after 90 days of no access
    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    # Hard-delete after 365 days (change or remove if you need permanent storage)
    expiration {
      days = 365
    }

    # Also clean up incomplete multipart uploads (free space, avoid surprise charges)
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ------------------------------------------------------------------------------
# CORS — allow the frontend to fetch pre-signed URLs directly from the bucket
# Remove this block if the frontend only accesses data via the FastAPI backend
# ------------------------------------------------------------------------------

resource "aws_s3_bucket_cors_configuration" "wind_farm_data" {
  bucket = aws_s3_bucket.wind_farm_data.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = var.cors_allowed_origins
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

