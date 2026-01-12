# BEES Data Engineering – Breweries Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.8](https://img.shields.io/badge/airflow-2.8-orange.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/tests-73%20passed-green.svg)]()

A data pipeline solution for the BEES/AB-InBev Data Engineering case. This project consumes data from the [Open Brewery DB API](https://www.openbrewerydb.org/), transforms it following the **Medallion Architecture**, and provides an aggregated analytical layer.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Pipeline Layers](#pipeline-layers)
- [Orchestration](#orchestration)
- [Testing](#testing)
- [Monitoring & Alerting](#monitoring--alerting)
- [Design Decisions](#design-decisions)
- [Trade-offs](#trade-offs)

---

## Overview

This pipeline fetches brewery data from a public API and processes it through three layers:

1. **Bronze (Raw)**: Raw data persisted as-is from the API in JSONL.gz format
2. **Silver (Curated)**: Cleaned and transformed data in Parquet format, partitioned by location
3. **Gold (Aggregated)**: Analytical layer with brewery counts by type and location

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Open Brewery   │────▶│     Bronze      │────▶│     Silver      │────▶│      Gold       │
│    DB API       │     │   (JSONL.gz)    │     │   (Parquet)     │     │  (Aggregated)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │                        │
                              ▼                        ▼                        ▼
                        Raw JSON data           Partitioned by            Breweries per
                        + metadata             country/state             type & location
```

### Airflow DAG Flow

```
start → extract_bronze → transform_silver → aggregate_gold → validate → end
```

---

## Project Structure

```
AB-INBEV/
│
├── config/                      # Configuration files
│   └── config.yaml              # API and pipeline settings
│
├── data/                        # Data Lake (Medallion Architecture)
│   ├── bronze/                  # Raw data from API
│   │   └── breweries/
│   │       └── ingestion_date=YYYY-MM-DD/
│   │           └── run_id=YYYYMMDD_HHMMSS/
│   │               ├── page=0001.jsonl.gz
│   │               └── _manifest.json
│   ├── silver/                  # Transformed parquet files
│   │   └── breweries/
│   │       ├── country=United States/
│   │       │   └── state_province=California/
│   │       └── _SUCCESS
│   └── gold/                    # Aggregated analytical data
│       └── breweries/
│           ├── breweries_by_type_and_location.parquet
│           ├── breweries_by_type.parquet
│           ├── breweries_by_country.parquet
│           └── _summary.json
│
├── doc/                         # Documentation
│   ├── MONITORING.md            # Monitoring & alerting strategy
│   ├── test_brewery_api_client.md
│   ├── test_raw_writer.md
│   ├── test_silver_transforms.md
│   └── test_gold_transforms.md
│
├── orchestration/               # Airflow DAGs
│   └── dags/
│       └── breweries_pipeline.py
│
├── src/                         # Source code
│   ├── clients/                 # API clients
│   │   ├── __init__.py
│   │   └── brewery_api_client.py
│   ├── config/                  # Configuration loader
│   │   ├── __init__.py
│   │   └── configuration.py
│   ├── io/                      # I/O operations
│   │   ├── __init__.py
│   │   ├── raw_writer.py        # Bronze layer writer
│   │   ├── bronze_reader.py     # Bronze layer reader
│   │   └── silver_reader.py     # Silver layer reader
│   ├── pipelines/               # Pipeline implementations
│   │   ├── __init__.py
│   │   ├── bronze_layer.py
│   │   ├── silver_layer.py
│   │   └── gold_layer.py
│   └── transforms/              # Data transformations
│       ├── __init__.py
│       ├── silver_transforms.py
│       └── gold_transforms.py
│
├── tests/                       # Unit tests
│   └── unit/
│       ├── test_brewery_api_client.py
│       ├── test_raw_writer.py
│       ├── test_silver_transforms.py
│       └── test_gold_transforms.py
│
├── .env                         # Environment variables
├── .env.example                 # Environment template
├── .dockerignore
├── .gitignore
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Application container
├── Makefile                     # Development commands
├── requirements.txt             # Python dependencies
└── README.md
```

---

## Tech Stack

| Component        | Technology                     |
|------------------|--------------------------------|
| Language         | Python 3.11+                   |
| Data Processing  | Pandas                         |
| Storage Format   | Parquet (PyArrow)              |
| Orchestration    | Apache Airflow 2.8             |
| Containerization | Docker & Docker Compose        |
| Database         | PostgreSQL 15 (Airflow metadata)|
| Testing          | pytest                         |

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Git

### Option 1: Running with Docker (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/your-username/ab-inbev-breweries.git
cd ab-inbev-breweries

# 2. Create environment file
cp .env.example .env

# 3. Start Airflow
docker-compose up -d

# 4. Access Airflow UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow
```

### Option 2: Running Locally (Without Docker)

```bash
# 1. Clone the repository
git clone https://github.com/your-username/ab-inbev-breweries.git
cd ab-inbev-breweries

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the pipeline
python -m src.pipelines.bronze_layer
python -m src.pipelines.silver_layer
python -m src.pipelines.gold_layer
```

---

## Running the Pipeline

### With Airflow (Docker)

1. Access Airflow UI: http://localhost:8080
2. Find the DAG: `breweries_pipeline`
3. Click the **▶️ Trigger DAG** button
4. Monitor execution in the Graph view

### With Command Line

```bash
# Run complete pipeline
python -m src.pipelines.bronze_layer
python -m src.pipelines.silver_layer
python -m src.pipelines.gold_layer

# Or use Makefile (Linux/Mac)
make run-all
```

### Docker Commands (used)

| Action       | Command                  |
|--------------|--------------------------|
| Start        | `docker-compose up -d`   |
| Stop         | `docker-compose down`    |
| View logs    | `docker-compose logs -f` |
| Check status | `docker-compose ps`      |
| Restart      | `docker-compose restart` |

---

## Pipeline Layers

### Bronze Layer (Raw)

- **Source**: Open Brewery DB API
- **Format**: JSONL.gz (gzipped JSON Lines)
- **Features**:
  - Automatic pagination handling
  - Retry logic with exponential backoff
  - Ingestion metadata (_ingestion_date, _run_id, _ingested_at)
  - Manifest file with run statistics

**Output Structure**:
```
data/bronze/breweries/
└── ingestion_date=2025-01-12/
    └── run_id=20250112_003000/
        ├── page=0001.jsonl.gz
        ├── page=0002.jsonl.gz
        ├── ...
        └── _manifest.json
```

### Silver Layer (Curated)

- **Format**: Parquet (columnar storage)
- **Partitioning**: By `country` and `state_province`
- **Transformations Applied**:
  - Column selection (removed deprecated fields: `state`, `street`)
  - Data type standardization (strings, floats)
  - Null handling (standardized to pandas NA)
  - Coordinate validation (lat: -90 to 90, lon: -180 to 180)
  - Deduplication by ID
  - String cleaning (whitespace trimming)

**Output Structure**:
```
data/silver/breweries/
├── country=United States/
│   ├── state_province=California/
│   │   └── *.parquet
│   ├── state_province=Oregon/
│   └── ...
├── country=Ireland/
└── _SUCCESS
```

### Gold Layer (Aggregated)

- **Format**: Parquet
- **Main Output**: Quantity of breweries per type and location

**Output Files**:
| File | Description |
|------|-------------|
| `breweries_by_type_and_location.parquet` | Main aggregation (type + country + state) |
| `breweries_by_type.parquet` | Global count by brewery type |
| `breweries_by_country.parquet` | Count by country |
| `_summary.json` | Comprehensive summary |

**Schema** (`breweries_by_type_and_location.parquet`):
```
| country       | state_province | brewery_type | brewery_count |
|---------------|----------------|--------------|---------------|
| United States | California     | micro        | 523           |
| United States | California     | brewpub      | 187           |
| Ireland       | Dublin         | micro        | 28            |
```

---

## Orchestration

The pipeline is orchestrated using **Apache Airflow** with the following features:

### DAG Configuration

| Setting | Value |
|---------|-------|
| Schedule | `@daily` (00:00 UTC) |
| Retries | 3 |
| Retry Delay | 5 minutes (exponential backoff) |
| Execution Timeout | 1 hour |
| Catchup | Disabled |

### Tasks

1. **start** - Pipeline start marker
2. **extract_bronze** - Fetch data from API → Bronze layer
3. **transform_silver** - Transform Bronze → Silver (Parquet)
4. **aggregate_gold** - Aggregate Silver → Gold
5. **validate_pipeline** - Data quality validation
6. **end** - Pipeline end marker

### Error Handling

- Automatic retries with exponential backoff
- Failure callbacks for alerting
- XCom for passing metrics between tasks
- Validation task to check data consistency

---

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_brewery_api_client.py -v
```

### Test Coverage

| Module | Tests | Coverage |
|--------|-------|----------|
| API Client | 15 | ✅ |
| Raw Writer | 17 | ✅ |
| Silver Transforms | 26 | ✅ |
| Gold Transforms | 15 | ✅ |
| **Total** | **73** | ✅ |

### What's Tested

- API client pagination and error handling
- File I/O operations (read/write)
- Data transformations (types, nulls, validation)
- Aggregation logic
- Edge cases (empty data, unicode, duplicates)

---

## Monitoring & Alerting

See [doc/MONITORING.md](doc/monitoring.md) for the complete monitoring strategy.

### Summary

| Aspect | Implementation |
|--------|----------------|
| Pipeline Failures | Airflow alerts, retry logic, failure callbacks |
| Data Quality | Row count validation, schema checks, null monitoring |
| Logging | Structured logs with timestamps |
| Metrics | XCom for cross-task metrics |

### Recommended Tools (Production)

- **Observability**: Datadog, Grafana, Prometheus
- **Data Quality**: Great Expectations
- **Alerting**: Slack, PagerDuty

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **JSONL.gz for Bronze** | Preserves raw data, good compression (~70-80%), streaming reads |
| **Parquet for Silver/Gold** | Columnar format optimized for analytics, efficient compression |
| **Partition by country/state** | Balanced partition sizes, enables efficient querying |
| **Pandas over PySpark** | Dataset size (~8k rows) doesn't justify Spark overhead |
| **Airflow for orchestration** | Industry standard, rich ecosystem, good UI for monitoring |
| **Docker Compose** | Easy local setup, reproducible environment |
| **Singleton Config** | Single source of truth for configuration |

---

## Trade-offs

| Trade-off | Decision | Alternative |
|-----------|----------|-------------|
| **Storage** | Local filesystem | S3/GCS/ADLS for production |
| **Processing** | Batch (daily) | Streaming if real-time needed |
| **Compute** | Single node (Pandas) | Spark for larger datasets |
| **Database** | PostgreSQL (Airflow) | Managed service (Cloud Composer, MWAA) |
| **Secrets** | .env file | Vault, AWS Secrets Manager |

---

## Future Improvements

- [ ] Add Great Expectations for data quality
- [ ] Implement incremental loading (only new breweries)
- [ ] Add Slack/Teams notifications
- [ ] Deploy to cloud (AWS/GCP/Azure)
- [ ] Add CI/CD pipeline (GitHub Actions)
- [ ] Implement data versioning (Delta Lake)

---



## Author
**Janathan Junior**  
Data Engineer

---
## License

This project was created as part of a technical assessment for AB-InBev/BEES.

```mermaid
flowchart TD
    A[Open Brewery DB API] --> B[Bronze Layer<br>Raw JSONL (.jsonl.gz)]
    B --> C[Silver Layer<br>Parquet<br>+ Partition by state]
    C --> D[Gold Layer<br>Aggregation<br>Count per type/location]

    subgraph Airflow DAG
        E[extract_bronze]
        F[transform_silver]
        G[transform_gold]
        H[check_nulls_in_silver]
    end

    E --> B
    F --> C
    G --> D
    H --> C

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#d0f0c0,stroke:#333
    style C fill:#add8e6,stroke:#333
    style D fill:#ffd580,stroke:#333
    style AirflowDAG fill:#f0f0f0,stroke:#bbb,stroke-dasharray: 5 5
