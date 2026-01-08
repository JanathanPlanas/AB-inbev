# BEES Data Engineering – Breweries Pipeline

A data pipeline solution for the BEES/AB-InBev Data Engineering case. This project consumes data from the [Open Brewery DB API](https://www.openbrewerydb.org/), transforms it following the **Medallion Architecture**, and provides an aggregated analytical layer.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Pipeline Layers](#pipeline-layers)
- [Orchestration](#orchestration)
- [Monitoring & Alerting](#monitoring--alerting)
- [Testing](#testing)
- [Design Decisions](#design-decisions)

## Overview

This pipeline fetches brewery data from a public API and processes it through three layers:

1. **Bronze (Raw)**: Raw data persisted as-is from the API
2. **Silver (Curated)**: Cleaned and transformed data in Parquet format, partitioned by location
3. **Gold (Aggregated)**: Analytical layer with brewery counts by type and location

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Open Brewery   │────▶│     Bronze      │────▶│     Silver      │────▶│      Gold       │
│    DB API       │     │   (Raw JSON)    │     │   (Parquet)     │     │  (Aggregated)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │                        │
                              ▼                        ▼                        ▼
                        Native format           Partitioned by            Breweries per
                                               state/country             type & location
```

## Project Structure

```
AB-INBEV/
│
├── app/                     # Application entry points
│
├── config/                  # Configuration files (YAML, env templates)
│
├── data/                    # Data Lake (Medallion Architecture)
│   ├── bronze/              # Raw data from API
│   ├── silver/              # Transformed parquet files
│   └── gold/                # Aggregated analytical data
│
├── doc/                     # Documentation and case description
│   └── DE_Case_Atualizado.pdf
│
├── orchestration/           # Orchestration DAGs and workflows
│   └── dags/                # Airflow DAGs (or Mage pipelines)
│
├── src/                     # Source code
│   ├── clients/             # API clients (Open Brewery DB)
│   ├── config/              # Config loaders and settings
│   ├── io/                  # I/O operations (readers, writers)
│   ├── pipelines/           # Pipeline definitions
│   ├── transforms/          # Data transformations (bronze→silver→gold)
│   └── utils/               # Utility functions and helpers
│
├── tests/                   # Unit and integration tests
│   ├── unit/
│   └── integration/
│
├── .gitignore
├── docker-compose.yml       # Docker Compose for local execution
├── Dockerfile               # Container definition
├── requirements.txt         # Python dependencies
└── README.md
```

## Tech Stack

| Component       | Technology                     |
|-----------------|--------------------------------|
| Language        | Python 3.11+                   |
| Data Processing | Pandas / PySpark               |
| Storage Format  | Parquet                        |
| Orchestration   | Apache Airflow / Mage          |
| Containerization| Docker & Docker Compose        |
| Testing         | pytest                         |
| Code Quality    | black, isort, flake8           |

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Make (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/ab-inbev-breweries.git
   cd ab-inbev-breweries
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate   # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run with Docker (recommended)**
   ```bash
   docker-compose up --build
   ```

### Running the Pipeline

```bash
# Run the full pipeline
python -m app.main

# Or run individual layers
python -m src.pipelines.bronze_pipeline
python -m src.pipelines.silver_pipeline
python -m src.pipelines.gold_pipeline
```

## Pipeline Layers

### Bronze Layer (Raw)

- Fetches all breweries from the Open Brewery DB API
- Handles pagination automatically
- Persists raw JSON data
- Includes metadata: ingestion timestamp, source URL

### Silver Layer (Curated)

- Converts JSON to Parquet (columnar format)
- **Partitioned by**: `country` and `state`
- Transformations applied:
  - Data type standardization
  - Null handling
  - Column renaming (snake_case)
  - Deduplication

### Gold Layer (Aggregated)

- Aggregated view: **quantity of breweries per type and location**
- Optimized for analytical queries
- Output schema:
  ```
  | country | state | brewery_type | brewery_count |
  ```

## Orchestration

The pipeline is orchestrated using **[Airflow/Mage]** with the following features:

- **Scheduling**: Daily execution at 00:00 UTC
- **Retries**: 3 attempts with exponential backoff
- **Error Handling**: Alerts on failure, graceful degradation
- **Dependencies**: Bronze → Silver → Gold (sequential)

```
[Extract API] → [Load Bronze] → [Transform Silver] → [Aggregate Gold]
```

## Monitoring & Alerting

### Strategy

| Aspect             | Implementation                                      |
|--------------------|-----------------------------------------------------|
| Pipeline Failures  | Airflow alerts via email/Slack on task failure      |
| Data Quality       | Row count validation, schema checks, null monitoring|
| Latency            | SLA monitoring for each layer                       |
| Logging            | Structured logs with correlation IDs                |

### Data Quality Checks

- **Bronze**: API response validation, schema consistency
- **Silver**: Row count comparison, null percentage thresholds
- **Gold**: Aggregation integrity (sum validation)

### Recommended Tools (Production)

- **Observability**: Datadog, Grafana
- **Data Quality**: Great Expectations, dbt tests
- **Alerting**: PagerDuty, Slack webhooks

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_api_client.py
```

### Test Coverage

- `src/clients/` - API client mocking and response handling
- `src/transforms/` - Transformation logic validation
- `src/pipelines/` - Integration tests for pipeline flow

## Design Decisions

| Decision                  | Rationale                                                  |
|---------------------------|------------------------------------------------------------|
| Parquet over Delta        | Simpler setup for local execution; Delta adds complexity   |
| Partition by state        | Balanced partition size; avoids small files problem        |
| Pandas over PySpark       | Dataset size (~8k rows) doesn't justify Spark overhead     |
| Airflow for orchestration | Industry standard; rich ecosystem; good for demonstration  |
| Docker Compose            | Easy local setup; reproducible environment                 |

### Trade-offs

1. **Local storage vs Cloud**: Used local filesystem for simplicity. In production, would use S3/GCS/ADLS.
2. **Batch vs Streaming**: Batch processing chosen as brewery data doesn't change frequently.
3. **Single node vs Distributed**: Pandas sufficient for current data volume; PySpark ready if needed.

## License

This project was created as part of a technical assessment for AB-InBev/BEES.

---

**Author**: [Your Name]  
**Date**: January 2025