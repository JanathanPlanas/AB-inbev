# BEES Data Engineering - Breweries Pipeline
# Makefile for common development tasks

.PHONY: help install test lint run-bronze run-silver run-gold run-all docker-up docker-down docker-logs clean

# Default target
help:
	@echo "BEES Data Engineering - Breweries Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo ""
	@echo "  Setup:"
	@echo "    make install        Install Python dependencies"
	@echo ""
	@echo "  Local Execution:"
	@echo "    make run-bronze     Run Bronze layer pipeline"
	@echo "    make run-silver     Run Silver layer pipeline"
	@echo "    make run-gold       Run Gold layer pipeline"
	@echo "    make run-all        Run complete pipeline (Bronze → Silver → Gold)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test           Run all tests"
	@echo "    make test-cov       Run tests with coverage report"
	@echo ""
	@echo "  Docker (Airflow):"
	@echo "    make docker-up      Start Airflow services"
	@echo "    make docker-down    Stop Airflow services"
	@echo "    make docker-logs    View Airflow logs"
	@echo "    make docker-run     Run pipeline in standalone container"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make lint           Run linters (black, isort, flake8)"
	@echo "    make format         Format code with black and isort"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean          Remove generated files and caches"
	@echo "    make clean-data     Remove all data (bronze, silver, gold)"

# =============================================================================
# Setup
# =============================================================================

install:
	pip install -r requirements.txt

# =============================================================================
# Local Pipeline Execution
# =============================================================================

run-bronze:
	@echo "Running Bronze Layer..."
	python -m src.pipelines.bronze_layer

run-silver:
	@echo "Running Silver Layer..."
	python -m src.pipelines.silver_layer

run-gold:
	@echo "Running Gold Layer..."
	python -m src.pipelines.gold_layer

run-all: run-bronze run-silver run-gold
	@echo "Complete pipeline finished!"

# =============================================================================
# Testing
# =============================================================================

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/index.html"

# =============================================================================
# Docker / Airflow
# =============================================================================

docker-up:
	@echo "Starting Airflow services..."
	@echo "Creating .env file if not exists..."
	@test -f .env || cp .env.example .env
	docker-compose up -d
	@echo ""
	@echo "Airflow UI: http://localhost:8080"
	@echo "Username: airflow"
	@echo "Password: airflow"

docker-down:
	@echo "Stopping Airflow services..."
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-run:
	@echo "Running pipeline in standalone container..."
	docker-compose --profile standalone up pipeline-runner

docker-build:
	docker-compose build

# =============================================================================
# Code Quality
# =============================================================================

lint:
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/

format:
	black src/ tests/
	isort src/ tests/

# =============================================================================
# Cleanup
# =============================================================================

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov/ .coverage 2>/dev/null || true
	@echo "Cleaned up cache files"

clean-data:
	@echo "WARNING: This will delete all data in bronze, silver, and gold layers!"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	rm -rf data/bronze/breweries/*
	rm -rf data/silver/breweries/*
	rm -rf data/gold/breweries/*
	@echo "Data cleaned"