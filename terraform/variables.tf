variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "finrisk360"
}

variable "alert_email" {
  description = "Email for alerts"
  type        = string
  default     = "katikamanohar722@gmail.com"
}
