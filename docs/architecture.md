# FinRisk 360 Architecture

## System Overview
FinRisk 360 is a production-grade mortgage risk 
intelligence platform processing 514K real HMDA 
loans using the Fannie Mae DU risk formula.

## Data Flow
1. Producers → Kafka → Kinesis → Lambda → S3 Raw
2. Glue Crawlers → Glue Data Catalog → Athena
3. Glue Job 1 → S3 Silver (clean Parquet)
4. Glue Job 2 → S3 Gold (risk scores)
5. Snowflake ← S3 Gold (COPY INTO)
6. dbt → mart models → Streamlit + FastAPI

## AWS Services
- Kinesis: Real-time streaming
- S3: Data lake (3 layers)
- Lambda: Event-driven processing
- Glue: ETL + Data Catalog
- Athena: S3 SQL queries
- Snowflake: Data warehouse
- CloudWatch: Monitoring
- SNS: Alerting
- EventBridge: Scheduled triggers
- IAM: Security
