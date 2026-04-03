# 🏦 FinRisk 360 — Real-Time Mortgage Risk Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8-black?logo=apachekafka)
![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazonaws)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?logo=apacheairflow)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)
![FastAPI](https://img.shields.io/badge/FastAPI-REST%20API-009688?logo=fastapi)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform)
![License](https://img.shields.io/badge/License-MIT-green)

> Production-grade data engineering platform that detects 
> mortgage loans at risk of delinquency before they stop paying,
> using real US government data and the Fannie Mae 
> Desktop Underwriter risk formula.

---

## 📊 Live Metrics

| Metric | Value |
|--------|-------|
| 📋 Loans Analyzed | 514,307 |
| 🔴 Critical Risk Loans | 11,543 |
| 💰 High Risk Exposure | $25.33 Billion |
| ⚡ API Response Time | <200ms |
| ✅ Data Quality Tests | 30/30 Passing |
| 🔄 Pipeline Runs | Daily at 6AM |
| 🗂️ Glue Catalog Tables | 3 (Raw/Silver/Gold) |
| 🏗️ AWS Resources | 15+ managed via Terraform |

---

## 🏗️ Architecture

┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│  CFPB HMDA API │ FRED API │ Alpha Vantage │ US Census API   │
└────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                  STREAMING LAYER                             │
│      Apache Kafka (local) ──► AWS Kinesis ──► Lambda        │
│           kinesis_bridge.py bridges local to cloud          │
└────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER (AWS S3)                     │
│   Raw (JSON) ──► Silver (Parquet) ──► Gold (Risk Scores)    │
└────────┬───────────────┬──────────────────┬─────────────────┘
│               │                  │
▼               ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│                  CATALOG LAYER                               │
│  Glue Crawlers (3) ──► Glue Data Catalog ──► AWS Athena     │
└────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                  PROCESSING LAYER                            │
│  Glue Job 1 (Raw→Silver) ──► Glue Job 2 (Silver→Gold)       │
│         PySpark ETL              Risk Scoring Formula        │
└────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                  WAREHOUSE LAYER                             │
│         Snowflake DWH ──► dbt models ──► 12 tests           │
│      stg_loan_risk_scores │ mart_risk_summary               │
│      mart_high_risk_loans │ mart_daily_risk_trend           │
└────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                   SERVING LAYER                              │
│   Streamlit Dashboard (5 pages) │ FastAPI REST API (<200ms) │
└─────────────────────────────────────────────────────────────┘
Orchestration : Apache Airflow — 5 DAGs, daily 6AM
IaC           : Terraform — 15+ AWS resources
Backup        : AWS EventBridge — independent Glue triggers
Monitoring    : Grafana + CloudWatch + SNS Alerts
Quality       : Great Expectations — 30 checks
---

## 🧮 Risk Scoring Formula (Fannie Mae DU)
risk_score = LTV × 0.30 + DTI × 0.25 +
RateSpread × 0.25 + MacroStress × 0.20
LTV Score:  <80% → 0.2 | 80-90% → 0.5 | 90-95% → 0.8 | >95% → 1.0
DTI Score:  <36% → 0.2 | 36-43% → 0.5 | 43-50% → 0.8 | >50% → 1.0
Rate Score: <4%  → 0.2 | 4-6%   → 0.5 | 6-8%   → 0.8 | >8%  → 1.0
Macro Stress: Fixed 0.6 (2024 high rate environment)
Risk Tiers:
🟢 LOW      < 0.30  →     409 loans
🟡 MEDIUM   0.30-0.60 → 266,398 loans
🟠 HIGH     0.60-0.80 →  69,926 loans
🔴 CRITICAL > 0.80  →   11,543 loans (avg loan $324,342)
---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion | Apache Kafka | Local message streaming |
| Cloud Streaming | AWS Kinesis | Cloud data streaming |
| Serverless | AWS Lambda | Event-driven S3 writes |
| Storage | AWS S3 (3 layers) | Data lake Raw/Silver/Gold |
| Processing | AWS Glue + PySpark | ETL + risk scoring |
| Data Catalog | AWS Glue Data Catalog | Metadata management |
| Crawlers | AWS Glue Crawlers (3) | Auto schema detection |
| Query Engine | AWS Athena | SQL queries directly on S3 |
| Warehouse | Snowflake | Analytics data warehouse |
| Transform | dbt | SQL models + data lineage |
| Orchestration | Apache Airflow | 5 DAGs + scheduling |
| Backup Triggers | AWS EventBridge | Independent Glue triggers |
| Quality | Great Expectations | 30 data quality checks |
| Dashboard | Streamlit | 6-page business intelligence |
| API | FastAPI | Real-time scoring REST API |
| Monitoring | Grafana + CloudWatch | Full observability |
| Alerts | AWS SNS | Critical loan + failure alerts |
| IaC | Terraform | All AWS infrastructure as code |
| Language | Python 3.11, SQL, HCL | Primary languages |

---

## 📁 Project Structure
finrisk_360/
├── .github/workflows/          # CI/CD pipelines
│   ├── ci.yml                  # Tests + validation
│   └── cd.yml                  # Deploy checks
├── producers/                  # Data ingestion scripts
│   ├── hmda_producer.py        # 514K HMDA loans → Kafka
│   ├── fred_producer.py        # Mortgage rates → Kafka
│   ├── alpha_vantage_producer.py # Macro data → Kafka
│   └── census_producer.py      # County demographics
├── streaming/                  # Streaming bridge
│   └── kinesis_bridge.py       # Kafka → AWS Kinesis
├── terraform/                  # AWS Infrastructure as Code
│   ├── main.tf                 # Provider config
│   ├── s3.tf                   # Data lake buckets
│   ├── kinesis.tf              # Kinesis streams
│   ├── lambda.tf               # Lambda functions
│   ├── iam.tf                  # Roles + policies
│   ├── sns.tf                  # Alert topics
│   ├── glue_catalog.tf         # Catalog + 3 crawlers
│   ├── eventbridge.tf          # Scheduled triggers
│   ├── cloudwatch_dashboard.tf # Monitoring dashboard
│   └── outputs.tf              # Resource ARNs
├── glue_jobs/                  # PySpark ETL scripts
│   ├── glue_raw_to_silver.py   # Clean + validate
│   └── glue_silver_to_gold.py  # Risk scoring formula
├── finrisk_dbt/                # dbt transformation layer
│   └── models/
│       ├── staging/
│       │   └── stg_loan_risk_scores.sql
│       └── marts/
│           ├── mart_risk_summary.sql
│           ├── mart_high_risk_loans.sql
│           └── mart_daily_risk_trend.sql
├── airflow/dags/               # Airflow orchestration
│   ├── finrisk_dag.py          # Main pipeline (7 tasks)
│   ├── finrisk360_alerting_dag.py     # Hourly alerts
│   ├── finrisk360_data_quality_dag.py # Quality checks
│   ├── finrisk360_model_refresh_dag.py # Weekly refresh
│   └── finrisk360_archival_dag.py     # Monthly archival
├── dashboard/
│   └── app.py                  # Streamlit 6-page dashboard
├── api/
│   └── main.py                 # FastAPI risk scoring API
├── great_expectations/
│   └── finrisk360_validations.py # 30 data quality checks
├── grafana/provisioning/       # Grafana dashboards
├── tests/                      # Unit tests
│   ├── test_risk_scoring.py    # Risk formula tests
│   └── test_api.py             # API endpoint tests
├── docs/
│   ├── architecture.md         # System design docs
│   └── setup_guide.md          # Installation guide
├── scripts/
│   ├── setup.sh                # One-command setup
│   └── teardown.sh             # Clean shutdown
├── .env.example                # Environment template
├── .gitignore                  # Comprehensive ignore rules
├── requirements.txt            # All Python dependencies
├── docker-compose.yml          # Local infrastructure
└── README.md
---

## 🔄 Pipeline Flow
INGEST      Airflow triggers producers at 6AM daily
FRED rates + Alpha Vantage → Kafka topics
STREAM      kinesis_bridge.py reads Kafka
→ AWS Kinesis → Lambda auto-triggers
→ Writes partitioned JSON to S3 Raw
CATALOG     Glue Crawlers run at 3-4AM daily
→ Auto-detect schemas in all 3 S3 layers
→ Update Glue Data Catalog tables
→ Athena ready for ad-hoc SQL queries
PROCESS     Glue Job 1: S3 Raw → S3 Silver
Clean nulls, fix types, validate records
SCORE       Glue Job 2: S3 Silver → S3 Gold
Apply Fannie Mae DU formula to all loans
Assign LOW/MEDIUM/HIGH/CRITICAL tiers
WAREHOUSE   S3 Gold → Snowflake (COPY INTO)
dbt run → 4 mart models
dbt test → 12 data quality tests
QUALITY     Great Expectations: 30 checks
Structural + Distribution + Freshness
SERVE       Snowflake → Streamlit Dashboard
Formula  → FastAPI <200ms scoring
MONITOR     Grafana → local pipeline metrics
CloudWatch → AWS service metrics
SNS → alerts for critical loans
---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker Desktop
- AWS Account + CLI configured
- Snowflake Account (free trial at snowflake.com)
- Terraform >= 1.5

### Setup
```bash
# 1. Clone repository
git clone https://github.com/Mkatika37/finrisk-360.git
cd finrisk-360

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your API keys and credentials

# 5. Start local infrastructure
docker-compose up -d

# 6. Deploy AWS infrastructure
cd terraform
terraform init
terraform apply

# 7. Load initial data
python producers/hmda_producer.py
python producers/fred_producer.py
python producers/alpha_vantage_producer.py

# 8. Bridge to AWS
python streaming/kinesis_bridge.py

# 9. Start dashboard
streamlit run dashboard/app.py

# 10. Start API (new terminal)
uvicorn api.main:app --reload --port 8000
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| 🎨 Streamlit Dashboard | http://localhost:8501 | - |
| ⚡ FastAPI Docs | http://localhost:8000/docs | - |
| 🌀 Airflow UI | http://localhost:8080 | airflow/airflow |
| 📊 Grafana | http://localhost:3000 | admin/admin |
| 📨 Kafdrop (Kafka) | http://localhost:9000 | - |

---

## ⚡ API Reference

**Base URL:** `http://localhost:8000`

### Score a Single Loan
```bash
POST /score-loan
Content-Type: application/json

{
  "loan_amount": 450000,
  "ltv": 95.0,
  "dti": 48.0,
  "interest_rate": 8.5,
  "income": 85000
}
```

**Response:**
```json
{
  "risk_score": 0.87,
  "risk_tier": "CRITICAL",
  "ltv_score": 1.0,
  "dti_score": 0.8,
  "rate_spread_score": 1.0,
  "macro_stress_score": 0.6,
  "recommendation": "Immediate rejection recommended.",
  "response_time_ms": 12
}
```

### All Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API overview |
| GET | `/health` | Pipeline health status |
| GET | `/stats` | Risk distribution summary |
| GET | `/loans/critical` | Top critical loans |
| POST | `/score-loan` | Score single loan |
| POST | `/score-batch` | Bulk score up to 100 loans |

---

## 📊 Data Sources

| Source | Dataset | Records | Frequency |
|--------|---------|---------|-----------|
| CFPB HMDA | 2024 VA/MD/DC Mortgages | 514,307 | Annual |
| FRED API | US Mortgage Interest Rates | Daily | Daily |
| Alpha Vantage | GDP, Unemployment, Macro | Daily | Daily |
| US Census | County Demographics | 3,221 counties | Annual |

---

## 📈 Key Findings

Analysis of 348,276 VA/MD/DC mortgage applications (2024):

- **$25.33 Billion** in high-risk loan exposure identified
- **11,543 CRITICAL** loans — avg score 0.836, avg amount $324,342
- **76.5% MEDIUM risk** — reflects 2024 high interest rate environment
- **Only 409 LOW risk** loans — severe market stress visible in data
- CRITICAL loans carry **3x higher avg amounts** vs LOW risk ($324K vs $114K)
- Risk formula mirrors Fannie Mae regulatory approach for explainability

---

## 🔁 Airflow DAGs

| DAG | Schedule | Tasks | Purpose |
|-----|----------|-------|---------|
| `finrisk_360_daily` | Daily 6AM | 7 | Main pipeline |
| `finrisk360_alerting` | Hourly | 4 | Critical loan detection |
| `finrisk360_data_quality` | Daily 6:30AM | 6 | Quality validation |
| `finrisk360_model_refresh` | Sunday midnight | 6 | Weekly risk recalculation |
| `finrisk360_archival` | Monthly 1st | 6 | S3 archival + cost saving |

---

## 📡 Monitoring & Observability

| Tool | Layer | Monitors |
|------|-------|----------|
| Grafana | Local | API latency, Kafka lag, risk distribution |
| CloudWatch | AWS | Lambda errors, Kinesis records, Glue jobs |
| Great Expectations | Data | 30 checks: structural + distribution + freshness |
| Glue Data Catalog | Schema | Auto-detected schema changes across 3 S3 layers |
| AWS Athena | Queries | Ad-hoc SQL directly on S3 data |
| AWS SNS | Alerts | Critical loan detection + pipeline failures |
| Airflow | Pipeline | 7-task DAG with retry + failure alerting |

---

## 🗂️ Data Catalog (Glue)

Three crawlers auto-detect schemas and update the catalog daily:

| Crawler | S3 Source | Schedule | Creates Table |
|---------|-----------|----------|---------------|
| finrisk360-raw-crawler | `/loans/` raw | 3:00AM | `raw_loans` |
| finrisk360-silver-crawler | `/loans/` silver | 3:30AM | `silver_loans` |
| finrisk360-gold-crawler | `/risk_scores/` gold | 4:00AM | `gold_risk_scores` |

All tables queryable via **AWS Athena** without loading to Snowflake.

---

## 💰 Cost Analysis

| Service | Monthly Cost |
|---------|-------------|
| AWS Kinesis (1 shard) | ~$1.46 |
| AWS Glue (nightly jobs) | ~$4.40 |
| AWS S3 (3 buckets ~5GB) | ~$0.12 |
| AWS Lambda (<100K req) | ~$0.20 |
| AWS CloudWatch | ~$0.30 |
| Snowflake (XS warehouse) | ~$8.00 |
| **Total** | **~$14.48/month** |

> 💡 Entire project runs on Snowflake $400 trial credits.

---

## 🏛️ Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rule-based scoring (not ML) | Regulatory explainability — mirrors Fannie Mae DU |
| Kafka + Kinesis (both) | Intentional redundancy — local dev + cloud prod |
| Airflow + EventBridge | Decoupled orchestration vs independent backup trigger |
| Snowflake only (not Redshift) | Overlap with professional Allstate experience |
| dbt + Great Expectations | Defense-in-depth quality at transformation + validation layers |
| Glue Catalog + Athena | Enterprise data discovery + S3 SQL without Snowflake |
| Terraform for everything | Reproducible, version-controlled infrastructure |

---

## 🧪 Tests
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test suites
python -m pytest tests/test_risk_scoring.py -v
python -m pytest tests/test_api.py -v
```

Test Coverage:
- ✅ Risk scoring formula (6 test cases)
- ✅ All 4 risk tiers validated
- ✅ API endpoints (5 test cases)
- ✅ Edge cases and invalid inputs

---

## 👤 Author

**Manohar Katika**
- 🎓 MS Data Analytics Engineering — George Mason University
- 💼 Ex-Allstate: AWS Glue, Lambda, Kinesis, Snowflake, Kafka, dbt, Airflow
- 💼 Ex-Tvisha Technologies: Azure ADF, Databricks, PySpark, MLflow
- 🐙 GitHub: [github.com/Mkatika37](https://github.com/Mkatika37)

---

## 📄 License

MIT License — free to use for learning purposes.

---

⭐ **Star this repo if you find it useful!**
