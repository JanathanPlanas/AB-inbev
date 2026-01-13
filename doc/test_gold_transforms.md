# Testing Strategy - Gold Layer Transformations 

## Scope

This document covers **unit tests** for the Gold layer aggregation functions in `src/transforms/gold_transforms.py`.

The goal is to validate that aggregations produce accurate counts, groupings, and summary metrics from **Silver layer data represented as a PyArrow Table** (curated layer).

> **Note:** The production pipeline persists Gold outputs as **Delta Lake tables**.  
> These unit tests focus on **pure transformation/aggregation logic**, independent of storage.

---

## What is Covered

### 1) Main Aggregation (`aggregate_by_type_and_location`)
| Test | Purpose |
|------|---------|
| `test_aggregates_correctly` | Validates the aggregation output is a PyArrow Table and contains expected columns |
| `test_total_matches_input` | Ensures sum of aggregated counts equals input record count |
| `test_custom_group_columns` | Tests aggregation with custom grouping columns |
| `test_california_micro_count` | Validates a specific group result (United States/California/micro = 2) |

### 2) Type Aggregation (`aggregate_by_type`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_type` | Validates correct counts per brewery type |
| `test_micro_count` | Ensures "micro" count matches expected (4) |
| `test_sorted_descending` | Ensures results are sorted by count (highest first) |

### 3) Country Aggregation (`aggregate_by_country`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_country` | Validates correct counts per country |
| `test_us_count` | Ensures US count matches expected (5) |
| `test_ireland_count` | Ensures Ireland count matches expected (2) |

### 4) State Aggregation (`aggregate_by_state`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_state` | Validates correct counts per state/province |
| `test_filter_by_country` | Tests filtering by a specific country |
| `test_nonexistent_country` | Validates handling of non-existent country filter (returns empty table) |

### 5) Gold Summary (`create_gold_summary`)
| Test | Purpose |
|------|---------|
| `test_summary_keys` | Validates summary structure (expected keys present) |
| `test_summary_values` | Ensures summary metrics are accurate (countries/states/types/breweries) |

### 6) Aggregation Stats (`get_aggregation_stats`)
| Test | Purpose |
|------|---------|
| `test_stats_keys` | Validates stats structure (expected keys present) |
| `test_stats_values` | Ensures stat values are correct (total breweries, unique countries) |

### 7) Empty Data Handling
| Test | Purpose |
|------|---------|
| `test_empty_table` | Ensures aggregations handle an empty PyArrow Table gracefully |

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Main Aggregation | 4 | ✅ |
| Type Aggregation | 3 | ✅ |
| Country Aggregation | 3 | ✅ |
| State Aggregation | 3 | ✅ |
| Gold Summary | 2 | ✅ |
| Aggregation Stats | 2 | ✅ |
| Empty Handling | 1 | ✅ |
| **Total** | **18** | ✅ |

> The numbers above reflect the current test module implementation for PyArrow/DuckDB.

---

## How to Run

```bash
# Run all gold transform tests
pytest tests/unit/test_gold_transforms.py -v

# Run with coverage
pytest tests/unit/test_gold_transforms.py --cov=src.transforms.gold_transforms --cov-report=term-missing
