# S3 Bucket for Airflow ML Artifacts
#
# This bucket stores intermediate data between Airflow tasks:
# - CSV data files
# - Trained ML models
# - Test datasets
# - Evaluation metrics
#
# Using S3 (instead of PVCs) ensures data is accessible from any worker pod,
# regardless of which node it's scheduled on.

resource "aws_s3_bucket" "airflow_ml_artifacts" {
  bucket = "airflow-ml-artifacts-${var.environment}"

  tags = {
    Name        = "Airflow ML Artifacts"
    Environment = var.environment
    Project     = "credit-default-prediction"
    ManagedBy   = "terraform"
  }
}

# Enable versioning to protect against accidental deletions
resource "aws_s3_bucket_versioning" "airflow_ml_artifacts" {
  bucket = aws_s3_bucket.airflow_ml_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable server-side encryption for security
resource "aws_s3_bucket_server_side_encryption_configuration" "airflow_ml_artifacts" {
  bucket = aws_s3_bucket.airflow_ml_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access (security best practice)
resource "aws_s3_bucket_public_access_block" "airflow_ml_artifacts" {
  bucket = aws_s3_bucket.airflow_ml_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle policy to clean up old artifacts (optional)
resource "aws_s3_bucket_lifecycle_configuration" "airflow_ml_artifacts" {
  bucket = aws_s3_bucket.airflow_ml_artifacts.id

  rule {
    id     = "delete-old-artifacts"
    status = "Enabled"

    # Delete artifacts older than 90 days
    expiration {
      days = 90
    }

    # Delete old versions after 30 days
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# Outputs
output "s3_bucket_name" {
  description = "Name of the S3 bucket for ML artifacts"
  value       = aws_s3_bucket.airflow_ml_artifacts.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.airflow_ml_artifacts.arn
}

output "s3_bucket_region" {
  description = "AWS region of the S3 bucket"
  value       = aws_s3_bucket.airflow_ml_artifacts.region
}
