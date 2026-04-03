# Setup Guide

## Prerequisites
- Python 3.11+
- Docker Desktop
- AWS Account + CLI configured
- Snowflake Account
- Terraform >= 1.5

## Step by Step Setup
1. Clone repo
2. Create venv: python -m venv venv
3. Activate: venv\Scripts\activate
4. Install: pip install -r requirements.txt
5. Copy .env: cp .env.example .env
6. Fill in .env with your credentials
7. Start Docker: docker-compose up -d
8. Deploy AWS: cd terraform && terraform apply
9. Load data: python producers/hmda_producer.py
10. Run bridge: python streaming/kinesis_bridge.py
11. Start dashboard: streamlit run dashboard/app.py
12. Start API: uvicorn api.main:app --port 8000
