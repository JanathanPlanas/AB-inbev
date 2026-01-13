
---

# BEES Data Engineering – Breweries Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.8](https://img.shields.io/badge/airflow-2.8-orange.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![DuckDB](https://img.shields.io/badge/duckdb-sql%20engine-yellow.svg)](https://duckdb.org/)
[![Delta Lake](https://img.shields.io/badge/delta-lake-acid-blueviolet.svg)](https://delta.io/)
[![Tests](https://img.shields.io/badge/tests-65%20passed-green.svg)]()

A data pipeline solution for the BEES/AB-InBev Data Engineering case. This project consumes data from the [Open Brewery DB API](https://www.openbrewerydb.org/), transforms it following the **Medallion Architecture**, and provides a **transactional and versioned analytical layer**.

---

## 📋 Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Project Structure](#project-structure)
* [Tech Stack](#tech-stack)
* [Getting Started](#getting-started)
* [Running the Pipeline](#running-the-pipeline)
* [Pipeline Layers](#pipeline-layers)
* [Orchestration](#orchestration)
* [Testing](#testing)
* [Monitoring & Alerting](#monitoring--alerting)
* [Design Decisions](#design-decisions)
* [Trade-offs](#trade-offs)

---

## Overview

This pipeline fetches brewery data from a public API and processes it through three layers:

1. **Bronze (Raw)**
   Raw data persisted as-is from the API in `JSONL.gz` format.

2. **Silver (Curated)**
   Cleaned and standardized data processed using **DuckDB** and stored as a **Delta Lake table**, partitioned by location.

3. **Gold (Aggregated)**
   Analytical layer built with **DuckDB SQL aggregations** and persisted as **Delta Lake tables**, providing reliable and idempotent analytical outputs.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌────────────────────────┐     ┌────────────────────────┐
│  Open Brewery   │────▶│     Bronze       │────▶│        Silver           │────▶│          Gold           │
│    DB API       │     │   (JSONL.gz)     │     │  (Delta Lake + DuckDB)  │     │ (Delta Lake Aggregates) │
└─────────────────┘     └─────────────────┘     └────────────────────────┘     └────────────────────────┘
                              │                        │                                │
                              ▼                        ▼                                ▼
                        Raw JSON data           Partitioned by                   Breweries per
                        + ingestion metadata    country/state                    type & location
                                                (_delta_log)
```

### Airflow DAG Flow

```
start → extract_bronze → transform_silver → aggregate_gold → validate → end
```

---

## Project Structure

> **Note:** Although some folders still contain Parquet files from early iterations, the current Silver and Gold layers are implemented using **Delta Lake**.

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
│   ├── silver/                  # Curated Delta Lake table
│   │   └── breweries/
│   │       ├── _delta_log/
│   │       ├── country=United States/
│   │       │   └── state_province=California/
│   │       └── ...
│   └── gold/                    # Aggregated Delta Lake tables
│       └── breweries/
│           ├── breweries_by_type_and_location/
│           ├── breweries_by_type/
│           ├── breweries_by_country/
│           └── _summary.json
│
├── doc/                         # Documentation
│   ├── MONITORING.md
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
│   ├── clients/
│   ├── config/
│   ├── io/
│   ├── pipelines/
│   └── transforms/
│
├── tests/
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Component        | Technology                       |
| ---------------- | -------------------------------- |
| Language         | Python 3.11+                     |
| Data Processing  | **DuckDB (SQL Engine)**          |
| Storage Format   | **Delta Lake (delta-rs)**        |
| Columnar Engine  | PyArrow                          |
| Orchestration    | Apache Airflow 2.8               |
| Containerization | Docker & Docker Compose          |
| Database         | PostgreSQL 15 (Airflow metadata) |
| Testing          | pytest                           |

---

## Pipeline Layers

### Bronze Layer (Raw)

* **Source**: Open Brewery DB API
* **Format**: JSONL.gz
* **Purpose**: Preserve raw data for auditability and reprocessing.

---

### Silver Layer (Curated)

* **Engine**: DuckDB (SQL transformations)
* **Storage**: Delta Lake
* **Partitioning**: `country`, `state_province`
* **Key Characteristics**:

  * Schema standardization
  * Deduplication
  * Coordinate validation
  * Null handling
  * **ACID guarantees and versioning via `_delta_log`**
  * Idempotent reprocessing

---

### Gold Layer (Aggregated)

* **Engine**: DuckDB SQL
* **Storage**: Delta Lake
* **Main Output**: Breweries per type and location
* **Guarantees**:

  * Idempotent overwrite mode
  * Safe re-runs without duplication
  * Versioned analytical outputs

---

## Design Decisions

| Decision                          | Rationale                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------- |
| **DuckDB over Pandas**            | SQL-based transformations, better performance characteristics, closer to real ELT workflows |
| **Delta Lake over plain Parquet** | ACID transactions, versioning, and safe reads across multiple parquet files                 |
| **JSONL.gz for Bronze**           | Raw data preservation with good compression                                                 |
| **Partition by country/state**    | Efficient analytical queries                                                                |
| **Airflow**                       | Industry-standard orchestration                                                             |
| **Docker Compose**                | Reproducible local environment                                                              |

---

## Trade-offs

| Trade-off  | Decision           | Alternative             |
| ---------- | ------------------ | ----------------------- |
| Storage    | Local filesystem   | S3 / GCS / ADLS         |
| Processing | Batch              | Streaming               |
| Compute    | Single-node DuckDB | Spark                   |
| Secrets    | `.env`             | Vault / Secrets Manager |

---

## Future Improvements

* [ ] Great Expectations
* [ ] CI/CD (GitHub Actions)
* [ ] Cloud deployment
* [ ] Data catalog integration

---

## Author
**Janathan Junior**
Data Engineer

---

## License

This project was created as part of a technical assessment for **AB-InBev / BEES**.

---