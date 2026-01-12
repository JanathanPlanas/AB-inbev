# Testing Strategy - Silver Layer Transformations

## Scope

This document covers **unit tests** for the Silver layer transformation functions in `src/transforms/silver_transforms.py`.

The goal is to validate each transformation step independently, ensuring data quality and consistency when converting Bronze data to Silver format.

---

## What is Covered

### 1) Column Selection (`select_columns`)
| Test | Purpose |
|------|---------|
| `test_selects_only_silver_columns` | Ensures deprecated columns (state, street) and metadata columns (_ingestion_date, _run_id) are removed |
| `test_handles_missing_columns` | Validates graceful handling when source data is missing expected columns |
| `test_custom_column_list` | Verifies custom column selection works correctly |

### 2) Type Standardization (`standardize_types`)
| Test | Purpose |
|------|---------|
| `test_coordinates_converted_to_float` | Confirms longitude/latitude are converted to float64 |
| `test_string_columns_converted` | Validates string columns use pandas string dtype |
| `test_invalid_coordinates_become_nan` | Ensures non-numeric coordinate strings become NaN |

### 3) Null Handling (`handle_nulls`)
| Test | Purpose |
|------|---------|
| `test_none_converted_to_na` | Validates None values become pandas NA |
| `test_empty_string_converted_to_na` | Ensures empty strings are treated as null |

### 4) Coordinate Validation (`validate_coordinates`)
| Test | Purpose |
|------|---------|
| `test_valid_coordinates_unchanged` | Confirms valid coordinates are preserved |
| `test_invalid_latitude_set_to_nan` | Validates latitudes outside [-90, 90] become NaN |
| `test_invalid_longitude_set_to_nan` | Validates longitudes outside [-180, 180] become NaN |
| `test_boundary_values_are_valid` | Ensures boundary values (±90, ±180) are accepted |

### 5) Brewery Type Validation (`validate_brewery_type`)
| Test | Purpose |
|------|---------|
| `test_valid_types_unchanged` | Confirms known types pass through unchanged |
| `test_all_valid_types_accepted` | Validates all 10 documented brewery types are recognized |

### 6) Deduplication (`deduplicate`)
| Test | Purpose |
|------|---------|
| `test_removes_duplicate_ids` | Ensures duplicate records (by ID) are removed, keeping first |
| `test_no_duplicates_unchanged` | Validates data without duplicates is unchanged |
| `test_custom_subset_columns` | Tests deduplication with custom column combinations |

### 7) String Cleaning (`clean_string_values`)
| Test | Purpose |
|------|---------|
| `test_strips_whitespace` | Validates leading/trailing whitespace is removed |
| `test_empty_strings_become_na` | Ensures whitespace-only strings become NA |

### 8) Partition Column Preparation (`add_partition_columns`)
| Test | Purpose |
|------|---------|
| `test_null_country_becomes_unknown` | Validates null countries are replaced with "Unknown" |
| `test_null_state_becomes_unknown` | Validates null states are replaced with "Unknown" |
| `test_empty_string_becomes_unknown` | Ensures empty strings are also replaced with "Unknown" |

### 9) Full Transformation Pipeline (`transform_bronze_to_silver`)
| Test | Purpose |
|------|---------|
| `test_full_transformation_pipeline` | End-to-end test of all transformations |
| `test_handles_empty_dataframe` | Validates graceful handling of empty input |

### 10) Transformation Summary (`get_transformation_summary`)
| Test | Purpose |
|------|---------|
| `test_summary_contains_expected_keys` | Validates summary structure |
| `test_summary_counts_are_correct` | Ensures record counts are accurate |

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Column Selection | 3 | ✅ |
| Type Standardization | 3 | ✅ |
| Null Handling | 2 | ✅ |
| Coordinate Validation | 4 | ✅ |
| Brewery Type Validation | 2 | ✅ |
| Deduplication | 3 | ✅ |
| String Cleaning | 2 | ✅ |
| Partition Columns | 3 | ✅ |
| Full Pipeline | 2 | ✅ |
| Summary | 2 | ✅ |
| **Total** | **26** | ✅ |

---

## How to Run

```bash
# Run all silver transform tests
pytest tests/unit/test_silver_transforms.py -v

# Run specific test class
pytest tests/unit/test_silver_transforms.py::TestValidateCoordinates -v

# Run with coverage
pytest tests/unit/test_silver_transforms.py --cov=src.transforms.silver_transforms --cov-report=term-missing
```

---

## Transformations Applied (Bronze → Silver)

| Step | Transformation | Rationale |
|------|----------------|-----------|
| 1 | Column selection | Remove deprecated (state, street) and metadata columns |
| 2 | Type standardization | Ensure consistent dtypes for downstream processing |
| 3 | Null handling | Standardize null representation (pandas NA) |
| 4 | String cleaning | Remove whitespace artifacts |
| 5 | Coordinate validation | Ensure valid geographic coordinates |
| 6 | Brewery type validation | Log any unknown types (data quality check) |
| 7 | Deduplication | Remove duplicate records by ID |
| 8 | Partition preparation | Ensure partition columns have no nulls |

---

## Data Quality Rules

- **Coordinates**: Latitude must be in [-90, 90], Longitude in [-180, 180]
- **Deduplication**: Based on `id` field (keeps first occurrence)
- **Partitioning**: `country` and `state_province` cannot be null (default to "Unknown")
- **Brewery Types**: Must be one of: micro, nano, regional, brewpub, large, planning, bar, contract, proprietor, closed
