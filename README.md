<p align="center">
  <h1 align="center">📊 finrisk-360</h1>
  <p align="center">
    <strong>End-to-end financial risk data engineering platform</strong>
  </p>
  <p align="center">
    <a href="#architecture">Architecture</a> •
    <a href="#quickstart">Quickstart</a> •
    <a href="#project-structure">Project Structure</a> •
    <a href="#environment-variables">Environment Variables</a> •
    <a href="#development">Development</a> •
    <a href="#ci-cd">CI / CD</a>
  </p>
</p>

---

## Overview

**finrisk-360** is a production-grade data engineering platform that ingests financial and economic data from multiple sources (FRED, Alpha Vantage, US Census), streams it through Kafka, transforms it with dbt on Snowflake, enforces data quality with Great Expectations, orchestrates workflows with Apache Airflow, and exposes a FastAPI service layer — all deployed via Terraform on AWS.

---

## Architecture

```
┌──────────────┐    ┌───────────────┐    ┌──────────────┐
│  Producers   │───▶│     Kafka     │───▶│  Ingestion   │
│  (API pulls) │    │  (streaming)  │    │  (consumers) │
└──────────────┘    └───────────────┘    └──────┬───────┘
                                                │
                                                ▼
                                       ┌────────────────┐
                                       │   Snowflake    │
                                       │  (raw / stage) │
                                       └───────┬────────┘
                                               │
                                               ▼
                                       ┌────────────────┐
                                       │   dbt Models   │
                                       │  (transform)   │
                                       └───────┬────────┘
                                               │
                           ┌───────────────────┼───────────────────┐
                           ▼                   ▼                   ▼
                   ┌───────────────┐   ┌───────────────┐   ┌──────────────┐
                   │ Data Quality  │   │   FastAPI      │   │   Alerting   │
                   │ (Great Exp.)  │   │   (serving)    │   │ (Slack/SNS)  │
                   └───────────────┘   └───────────────┘   └──────────────┘
                                               │
                                               ▼
                                       ┌────────────────┐
                                       │   Airflow      │
                                       │  (orchestrate) │
                                       └────────────────┘
```

---

## Quickstart

```bash
# 1. Clone the repo
git clone https://github.com/<your-org>/finrisk-360.git
cd finrisk-360

# 2. Copy environment template and fill in secrets
cp .env.example .env

# 3. Start the local stack
make up          # docker compose up -d

# 4. Open Airflow UI
open http://localhost:8080

# 5. Open FastAPI docs
open http://localhost:8000/docs
```

---

## Project Structure

```
finrisk-360/
├── producers/              # Data producers — pull from external APIs
│   ├── fred_producer.py
│   ├── alpha_vantage_producer.py
│   └── census_producer.py
├── ingestion/              # Kafka consumers → raw landing zone
│   ├── kafka_consumer.py
│   └── snowflake_loader.py
├── etl/                    # Lightweight Python ETL scripts
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── dbt/                    # dbt project (Snowflake transforms)
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
├── data_quality/           # Great Expectations suites
│   ├── great_expectations.yml
│   └── expectations/
├── airflow/                # Apache Airflow
│   ├── dags/
│   │   └── finrisk_dag.py
│   └── plugins/
├── api/                    # FastAPI service layer
│   ├── main.py
│   ├── routes/
│   ├── schemas/
│   └── Dockerfile
├── alerting/               # Slack + SNS alerting
│   ├── slack_notifier.py
│   └── sns_notifier.py
├── terraform/              # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
├── tests/                  # Unit & integration tests
│   └── test_placeholder.py
├── docker-compose.yml
├── Makefile
├── .env.example
├── .gitignore
└── README.md
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `FRED_API_KEY` | [FRED API](https://fred.stlouisfed.org/docs/api/) key |
| `ALPHA_VANTAGE_KEY` | [Alpha Vantage](https://www.alphavantage.co/) API key |
| `CENSUS_API_KEY` | [US Census](https://api.census.gov/) API key |
| `AWS_ACCESS_KEY_ID` | AWS IAM access key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret key |
| `AWS_REGION` | AWS region (default `us-east-1`) |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_DATABASE` | Snowflake database name |
| `SNOWFLAKE_WAREHOUSE` | Snowflake virtual warehouse |
| `SNOWFLAKE_SCHEMA` | Snowflake schema |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `SNS_TOPIC_ARN` | AWS SNS topic ARN for alerts |

---

## Development

```bash
# Lint
make lint

# Format
make fmt

# Test
make test

# dbt
make dbt-run
make dbt-test

# Clean caches
make clean
```

---

## CI / CD

| Workflow | Trigger | Description |
|---|---|---|
| `ci.yml` | Push / PR to `main` | Lint, test, dbt compile |
| `cd.yml` | Merge to `main` | Build Docker images, Terraform apply |

---

## License

MIT © 2026 finrisk-360 contributors
