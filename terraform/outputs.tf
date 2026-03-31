# ============================================================
# finrisk-360 — Terraform outputs
# ============================================================

output "data_lake_bucket" {
  description = "S3 bucket name for the data lake"
  value       = aws_s3_bucket.data_lake.id
}

output "sns_topic_arn" {
  description = "SNS topic ARN for pipeline alerts"
  value       = aws_sns_topic.alerts.arn
}
