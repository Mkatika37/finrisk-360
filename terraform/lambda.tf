# lambda_kinesis_sink
resource "aws_lambda_function" "lambda_kinesis_sink" {
  filename         = "../ingestion/lambda_kinesis_sink.zip"
  function_name    = "lambda_kinesis_sink"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_kinesis_sink.handler"
  runtime          = "python3.11"
  source_code_hash = filebase64sha256("../ingestion/lambda_kinesis_sink.zip")

  environment {
    variables = {
      S3_RAW_BUCKET = aws_s3_bucket.raw.bucket
    }
  }
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn               = aws_kinesis_stream.finrisk360_stream.arn
  function_name                  = aws_lambda_function.lambda_kinesis_sink.arn
  starting_position              = "LATEST"
  batch_size                     = 100
  bisect_batch_on_function_error = true
}

# lambda_dq_trigger
resource "aws_lambda_function" "lambda_dq_trigger" {
  filename         = "../ingestion/lambda_dq_trigger.zip"
  function_name    = "lambda_dq_trigger"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_dq_trigger.handler"
  runtime          = "python3.11"
  source_code_hash = filebase64sha256("../ingestion/lambda_dq_trigger.zip")
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_dq_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.silver.arn
}

# lambda_alert
resource "aws_lambda_function" "lambda_alert" {
  filename         = "../alerting/lambda_alert.zip"
  function_name    = "lambda_alert"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_alert.handler"
  runtime          = "python3.11"
  source_code_hash = filebase64sha256("../alerting/lambda_alert.zip")

  environment {
    variables = {
      SNS_TOPIC_ARN     = aws_sns_topic.alerts.arn
      SNOWFLAKE_ACCOUNT = "PLACEHOLDER"
    }
  }
}
