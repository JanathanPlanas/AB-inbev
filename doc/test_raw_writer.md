# Testing Strategy - Raw Writer (Bronze Layer I/O)

## Scope

This document covers **unit tests** for the `RawJsonlGzWriter` class, responsible for persisting raw API data into the Bronze layer.

The goal is to validate file creation, compression, content integrity, and metadata tracking without relying on external systems.

---

## What is Covered

### 1) Initialization and Path Handling
| Test | Purpose |
|------|---------|
| `test_init_with_string_path` | Ensures string paths are converted to `Path` objects |
| `test_init_with_path_object` | Validates direct `Path` object initialization |

### 2) Directory Structure Creation
| Test | Purpose |
|------|---------|
| `test_run_dir_creates_directory` | Verifies the partitioned directory structure is created correctly (`ingestion_date=YYYY-MM-DD/run_id=...`) |

### 3) File Writing and Compression
| Test | Purpose |
|------|---------|
| `test_write_page_creates_gzipped_file` | Confirms `.jsonl.gz` files are created |
| `test_write_page_content_is_valid_jsonl` | Validates that content is valid JSON Lines format |
| `test_write_page_numbering_format` | Ensures page numbers are zero-padded (e.g., `page=0001.jsonl.gz`) |

### 4) Metadata Injection
| Test | Purpose |
|------|---------|
| `test_write_page_adds_metadata` | Verifies ingestion metadata (`_ingestion_date`, `_run_id`, `_ingested_at`) is added to records |
| `test_write_page_without_metadata` | Confirms metadata can be disabled via `add_metadata=False` |

### 5) Internal State Tracking
| Test | Purpose |
|------|---------|
| `test_write_page_tracks_written_pages` | Ensures each written page is tracked internally for manifest generation |
| `test_get_summary` | Validates the summary method returns correct totals |

### 6) Manifest Generation
| Test | Purpose |
|------|---------|
| `test_write_manifest_creates_file` | Confirms `_manifest.json` is created |
| `test_write_manifest_content` | Validates manifest contains correct structure (totals, pages, timestamps) |
| `test_write_manifest_with_extra_metadata` | Ensures custom metadata can be injected into manifest |

### 7) Edge Cases
| Test | Purpose |
|------|---------|
| `test_empty_records` | Handles empty record lists gracefully |
| `test_unicode_content` | Ensures proper UTF-8 encoding for international characters (e.g., Portuguese, Japanese) |

### 8) Default Values
| Test | Purpose |
|------|---------|
| `test_default_ingestion_date_format` | Validates default date format is `YYYY-MM-DD` |
| `test_default_run_id_format` | Validates default run_id format is `YYYYMMDD_HHMMSS` |

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Initialization | 2 | ✅ |
| Directory Structure | 1 | ✅ |
| File Writing | 3 | ✅ |
| Metadata | 2 | ✅ |
| State Tracking | 2 | ✅ |
| Manifest | 3 | ✅ |
| Edge Cases | 2 | ✅ |
| Defaults | 2 | ✅ |
| **Total** | **17** | ✅ |

---

## How to Run

From the project root:

```bash
# Run all raw_writer tests
pytest tests/unit/test_raw_writer.py -v

# Run with coverage
pytest tests/unit/test_raw_writer.py --cov=src.io.raw_writer --cov-report=term-missing
```

---

## Design Decisions

1. **Gzip compression**: Reduces storage footprint for raw JSON data (~70-80% compression ratio)
2. **JSONL format**: One record per line enables streaming reads and easy debugging
3. **Partitioning by date/run**: Allows for incremental processing and easy data recovery
4. **Manifest file**: Provides data lineage and enables validation in downstream layers
5. **Zero-padded page numbers**: Ensures correct alphabetical sorting up to 9999 pages