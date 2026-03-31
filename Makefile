# ============================================================
# finrisk-360  —  Makefile
# ============================================================

.PHONY: help up down build logs lint test dbt-run dbt-test fmt clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Docker ───────────────────────────────────────────────────
up: ## Start all services
	docker compose up -d

down: ## Stop all services
	docker compose down

build: ## Rebuild all images
	docker compose build --no-cache

logs: ## Tail logs for all services
	docker compose logs -f

# ── Code Quality ─────────────────────────────────────────────
lint: ## Lint Python code (ruff) and Terraform (tflint)
	ruff check .
	cd terraform && tflint

fmt: ## Auto-format Python (ruff) and Terraform (terraform fmt)
	ruff format .
	cd terraform && terraform fmt -recursive

# ── Testing ──────────────────────────────────────────────────
test: ## Run all Python tests
	pytest tests/ -v --tb=short

# ── dbt ──────────────────────────────────────────────────────
dbt-run: ## Run dbt models
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

# ── Cleanup ──────────────────────────────────────────────────
clean: ## Remove caches and temp files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf dbt/target dbt/dbt_packages dbt/logs
