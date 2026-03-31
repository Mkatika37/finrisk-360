#!/usr/bin/env bash
# ============================================================
# finrisk-360 — Repository scaffolding script
# ============================================================
# Run this from the repo root:
#   chmod +x scripts/setup_repo.sh && ./scripts/setup_repo.sh
# ============================================================

set -euo pipefail

echo "🚀 Creating finrisk-360 folder structure..."

# ── Directories ──────────────────────────────────────────────
mkdir -p producers
mkdir -p ingestion
mkdir -p etl
mkdir -p dbt/{models/staging,models/marts,macros,seeds,snapshots,tests}
mkdir -p data_quality/expectations
mkdir -p airflow/{dags,plugins}
mkdir -p api/{routes,schemas}
mkdir -p alerting
mkdir -p terraform/modules
mkdir -p .github/workflows
mkdir -p tests
mkdir -p scripts

# ── Placeholder files (touch only if they don't exist) ───────
touch_if_missing() { [ -f "$1" ] || touch "$1"; }

# producers
touch_if_missing producers/__init__.py
touch_if_missing producers/fred_producer.py
touch_if_missing producers/alpha_vantage_producer.py
touch_if_missing producers/census_producer.py

# ingestion
touch_if_missing ingestion/__init__.py
touch_if_missing ingestion/kafka_consumer.py
touch_if_missing ingestion/snowflake_loader.py

# etl
touch_if_missing etl/__init__.py
touch_if_missing etl/extract.py
touch_if_missing etl/transform.py
touch_if_missing etl/load.py

# dbt
touch_if_missing dbt/dbt_project.yml
touch_if_missing dbt/models/staging/stg_fred_series.sql
touch_if_missing dbt/models/marts/fct_risk_indicators.sql
touch_if_missing dbt/macros/.gitkeep
touch_if_missing dbt/seeds/.gitkeep
touch_if_missing dbt/snapshots/.gitkeep
touch_if_missing dbt/tests/.gitkeep

# data_quality
touch_if_missing data_quality/great_expectations.yml
touch_if_missing data_quality/expectations/.gitkeep

# airflow
touch_if_missing airflow/dags/finrisk_dag.py
touch_if_missing airflow/plugins/.gitkeep

# api
touch_if_missing api/main.py
touch_if_missing api/routes/__init__.py
touch_if_missing api/schemas/__init__.py
touch_if_missing api/Dockerfile
touch_if_missing api/requirements.txt

# alerting
touch_if_missing alerting/slack_notifier.py
touch_if_missing alerting/sns_notifier.py

# terraform
touch_if_missing terraform/main.tf
touch_if_missing terraform/variables.tf
touch_if_missing terraform/outputs.tf
touch_if_missing terraform/modules/.gitkeep

# github workflows
touch_if_missing .github/workflows/ci.yml
touch_if_missing .github/workflows/cd.yml

# tests
touch_if_missing tests/test_placeholder.py

# root-level files
touch_if_missing docker-compose.yml
touch_if_missing Makefile
touch_if_missing .env.example
touch_if_missing .gitignore
touch_if_missing README.md

echo ""
echo "✅ finrisk-360 repository structure created!"
echo ""
echo "📁 Directory tree:"
# Use 'find' as a portable tree replacement
find . -not -path './.git/*' -not -path './node_modules/*' -not -path './__pycache__/*' | \
  sed -e 's/[^/]*\//│   /g' -e 's/│   \([^│]\)/├── \1/' | sort

echo ""
echo "🔑 Next steps:"
echo "   1. cp .env.example .env   # fill in your secrets"
echo "   2. make up                # start local Docker stack"
echo "   3. git init && git add -A && git commit -m 'Initial scaffold'"
