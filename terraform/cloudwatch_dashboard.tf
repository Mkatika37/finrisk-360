resource "aws_cloudwatch_log_group" "lambda_kinesis_sink" {
  name              = "/aws/lambda/${aws_lambda_function.lambda_kinesis_sink.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "lambda_dq_trigger" {
  name              = "/aws/lambda/${aws_lambda_function.lambda_dq_trigger.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "lambda_alert" {
  name              = "/aws/lambda/${aws_lambda_function.lambda_alert.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_name}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "Alarm when Lambda errors exceed 10 in 5 minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
