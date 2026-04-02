resource "aws_iam_role" "eventbridge_glue_role" {
  name = "finrisk360-eventbridge-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "finrisk360-eventbridge-glue-policy"
  role = aws_iam_role.eventbridge_glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_scheduler_schedule" "glue_job1" {
  name = "finrisk360-glue-job1-trigger"
  schedule_expression = "cron(30 2 * * ? *)"
  
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    input = jsonencode({
      JobName = "finrisk360-raw-to-silver"
    })
  }
}


resource "aws_scheduler_schedule" "glue_job2" {
  name = "finrisk360-glue-job2-trigger"
  schedule_expression = "cron(30 3 * * ? *)"
  
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    input = jsonencode({
      JobName = "finrisk360-silver-to-gold"
    })
  }
}

