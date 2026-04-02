resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-322960458535"
}

resource "aws_s3_bucket_public_access_block" "raw_block" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "transition-to-standard-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-322960458535"
}

resource "aws_s3_bucket_public_access_block" "silver_block" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver_lifecycle" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "transition-to-standard-ia"
    status = "Enabled"

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_notification" "silver_notification" {
  bucket = aws_s3_bucket.silver.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_dq_trigger.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-322960458535"
}

resource "aws_s3_bucket_public_access_block" "gold_block" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "gold_versioning" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold_lifecycle" {
  bucket = aws_s3_bucket.gold.id

  rule {
    id     = "transition-to-standard-ia"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}
