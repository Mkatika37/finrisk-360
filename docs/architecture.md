# FinRisk 360 Architecture

## System Overview
FinRisk 360 is a production-grade mortgage risk intelligence platform. It processes real-time and batch data to provide a 360-degree view of portfolio risk exposure.

## Component Interactions
1. **Data Producers**: Scripts (HMDA, FRED, Census) simulate live feeds and historical data ingestion into Kafka.
2. **Kinesis Bridge**: Bridges Kafka messages to AWS Kinesis for cloud-native ingestion.
3. **AWS Glue**: Performs Raw-to-Silver and Silver-to-Gold transformations in S3.
4. **Snowflake**: Serves as the primary analytics warehouse for scoring and reporting.
5. **FastAPI**: Provides real-time risk scoring using validated DU formulas.
6. **Grafana/Streamlit**: Visualizes pipeline health and business risk distribution.

## Data Flow
- `Raw Layer`: JSON/CSV in S3 Raw bucket.
- `Silver Layer`: Parquet files with schema enforcement.
- `Gold Layer`: Aggregated views and finalized risk scores in Snowflake.

## Technology Choices
- **Orchestration**: Apache Airflow.
- **Monitoring**: Prometheus & Grafana.
- **Infrastructure**: Terraform.
- **Transformation**: dbt & AWS Glue.
