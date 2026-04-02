resource "aws_glue_catalog_database" "finrisk360" {
  name        = "finrisk360_catalog"
  description = "FinRisk 360 mortgage risk data catalog"
}

resource "aws_glue_crawler" "raw" {
  name          = "finrisk360-raw-crawler"
  database_name = aws_glue_catalog_database.finrisk360.name
  role          = "arn:aws:iam::322960458535:role/finrisk360-glue-role"
  schedule      = "cron(0 3 * * ? *)"
  table_prefix  = "raw_"
  s3_target {
    path = "s3://finrisk360-raw-322960458535/loans/"
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_glue_crawler" "silver" {
  name          = "finrisk360-silver-crawler"
  database_name = aws_glue_catalog_database.finrisk360.name
  role          = "arn:aws:iam::322960458535:role/finrisk360-glue-role"
  schedule      = "cron(30 3 * * ? *)"
  table_prefix  = "silver_"
  s3_target {
    path = "s3://finrisk360-silver-322960458535/loans/"
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_glue_crawler" "gold" {
  name          = "finrisk360-gold-crawler"
  database_name = aws_glue_catalog_database.finrisk360.name
  role          = "arn:aws:iam::322960458535:role/finrisk360-glue-role"
  schedule      = "cron(0 4 * * ? *)"
  table_prefix  = "gold_"
  s3_target {
    path = "s3://finrisk360-gold-322960458535/risk_scores/"
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_athena_workgroup" "finrisk360" {
  name = "finrisk360-workgroup"
  configuration {
    result_configuration {
      output_location = "s3://finrisk360-raw-322960458535/athena-results/"
    }
  }
}
