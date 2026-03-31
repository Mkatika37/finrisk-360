# ============================================================
# finrisk-360 — Terraform main configuration
# ============================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }

  backend "s3" {
    bucket = "finrisk-360-tfstate"
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_user
  password = var.snowflake_password
}

# ── Example resources (expand as needed) ─────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "finrisk-360-data-lake"

  tags = {
    Project     = "finrisk-360"
    Environment = var.environment
  }
}

resource "aws_sns_topic" "alerts" {
  name = "finrisk-alerts"

  tags = {
    Project     = "finrisk-360"
    Environment = var.environment
  }
}
