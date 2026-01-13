# Testing Strategy - Silver Layer Transformations (DuckDB + PyArrow)

## Scope

This document covers **unit tests** for the Silver layer transformation functions in `src/transforms/silver_transforms.py`.

The goal is to validate that Bronze data is correctly cleaned, standardized, and validated when converting to the Silver layer format using a **DuckDB SQL-based transformer** with **PyArrow Tables** as the main in-memory representation.

> **Note:** These tests focus on transformation logic (in-memory).  
> Persistence (Delta Lake write) is validated indirectly by pipeline execution and integration runs.

---

## What is Covered

### 1) Transformation Entrypoint (`transform_bronze_to_silver`)
| Test | Purpose |
|------|---------|
| `test_transform_from_list` | Validates that a `list[dict]` input is supported and returns a `pyarrow.Table` |
| `test_transform_from_arrow` | Validates that a `pyarrow.Table` input is supported |
| `test_brewery_type_lowercase` | Ensures brewery types are normalized to lowercase |
| `test_string_trimming` | Ensures string columns are trimmed (e.g., `"  Brewery One  "` → `"Brewery One"`) |
| `test_deduplication` | Ensures duplicate records are removed by `id` |
| `test_null_id_removed` | Ensures records with null `id` are removed |
| `test_empty_country_becomes_unknown` | Ensures empty `country` becomes `"Unknown"` |
| `test_empty_state_becomes_unknown` | Ensures empty `state_province` becomes `"Unknown"` |

---

### 2) Coordinate Validation (Latitude/Longitude Rules)
| Test | Purpose |
|------|---------|
| `test_valid_coordinates` | Confirms valid coordinates are preserved and converted to numeric values |
| `test_invalid_latitude_null` | Validates latitudes outside [-90, 90] become `NULL` |
| `test_invalid_longitude_null` | Validates longitudes outside [-180, 180] become `NULL` |
| `test_boundary_coordinates` | Ensures boundary values (±90, ±180) are accepted |

---

### 3) Helper Functions (Arrow Utilities)
| Test | Purpose |
|------|---------|
| `test_arrow_table_from_pylist` | Validates list-of-dicts → PyArrow Table conversion |
| `test_arrow_table_to_pylist` | Validates PyArrow Table → list-of-dicts conversion |
| `test_get_column_as_list` | Validates column extraction helper function |

---

### 4) Empty Input Handling
| Test | Purpose |
|------|---------|
| `test_empty_list` | Ensures empty input results in an empty PyArrow Table |

---

### 5) Transformation Summary (`get_transformation_summary`)
| Test | Purpose |
|------|---------|
| `test_summary_keys` | Validates summary structure contains expected keys |
| `test_summary_counts` | Ensures record counts match expected values |

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Transformation Entrypoint | 8 | ✅ |
| Coordinate Validation | 4 | ✅ |
| Helper Functions | 3 | ✅ |
| Empty Input Handling | 1 | ✅ |
| Summary | 2 | ✅ |
| **Total** | **18** | ✅ |

> The total above reflects the current unit test implementation using PyArrow Tables and DuckDB-based transformations.

---

## How to Run

```bash
# Run all silver transform tests
pytest tests/unit/test_silver_transforms.py -v

# Run specific test class
pytest tests/unit/test_silver_transforms.py::TestCoordinateValidation -v

# Run with coverage
pytest tests/unit/test_silver_transforms.py --cov=src.transforms.silver_transforms --cov-report=term-missing
