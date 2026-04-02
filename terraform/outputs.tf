output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.bucket
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.finrisk360_stream.arn
}

output "sns_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "lambda_kinesis_sink_arn" {
  value = aws_lambda_function.lambda_kinesis_sink.arn
}

output "lambda_dq_trigger_arn" {
  value = aws_lambda_function.lambda_dq_trigger.arn
}

output "lambda_alert_arn" {
  value = aws_lambda_function.lambda_alert.arn
}

output "glue_catalog_database" {
  value = aws_glue_catalog_database.finrisk360.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.finrisk360.name
}
