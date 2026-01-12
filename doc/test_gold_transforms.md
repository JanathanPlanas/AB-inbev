# Testing Strategy - Gold Layer Transformations

## Scope

This document covers **unit tests** for the Gold layer aggregation functions in `src/transforms/gold_transforms.py`.

The goal is to validate that aggregations produce accurate counts and summaries from Silver layer data.

---

## What is Covered

### 1) Main Aggregation (`aggregate_by_type_and_location`)
| Test | Purpose |
|------|---------|
| `test_aggregates_correctly` | Validates correct brewery counts per type/location combination |
| `test_total_count_matches_input` | Ensures sum of aggregated counts equals input record count |
| `test_custom_group_columns` | Tests aggregation with custom grouping columns |
| `test_empty_dataframe` | Validates graceful handling of empty input |

### 2) Type Aggregation (`aggregate_by_type`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_type` | Validates correct counts per brewery type |
| `test_sorted_by_count_descending` | Ensures results are sorted by count (highest first) |

### 3) Country Aggregation (`aggregate_by_country`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_country` | Validates correct counts per country |
| `test_total_matches_input` | Ensures sum matches total input records |

### 4) State Aggregation (`aggregate_by_state`)
| Test | Purpose |
|------|---------|
| `test_aggregates_by_state` | Validates correct counts per state |
| `test_filter_by_country` | Tests filtering by specific country |
| `test_nonexistent_country` | Validates handling of non-existent country filter |

### 5) Gold Summary (`create_gold_summary`)
| Test | Purpose |
|------|---------|
| `test_summary_contains_expected_keys` | Validates summary structure |
| `test_summary_counts_are_correct` | Ensures summary counts are accurate |

### 6) Aggregation Stats (`get_aggregation_stats`)
| Test | Purpose |
|------|---------|
| `test_stats_contain_expected_keys` | Validates stats structure |
| `test_stats_values_are_correct` | Ensures stat values are accurate |

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Main Aggregation | 4 | ✅ |
| Type Aggregation | 2 | ✅ |
| Country Aggregation | 2 | ✅ |
| State Aggregation | 3 | ✅ |
| Gold Summary | 2 | ✅ |
| Aggregation Stats | 2 | ✅ |
| **Total** | **15** | ✅ |

---

## How to Run

```bash
# Run all gold transform tests
pytest tests/unit/test_gold_transforms.py -v

# Run with coverage
pytest tests/unit/test_gold_transforms.py --cov=src.transforms.gold_transforms --cov-report=term-missing
```

---

## Gold Layer Output Schema

### Main Aggregation (`breweries_by_type_and_location.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `country` | string | Country name |
| `state_province` | string | State or province name |
| `brewery_type` | string | Type of brewery (micro, brewpub, etc.) |
| `brewery_count` | int | Number of breweries |

### Example Output

```
| country       | state_province | brewery_type | brewery_count |
|---------------|----------------|--------------|---------------|
| United States | California     | micro        | 523           |
| United States | California     | brewpub      | 187           |
| United States | California     | nano         | 45            |
| United States | Oregon         | micro        | 312           |
| Ireland       | Dublin         | micro        | 28            |
```

---

## Additional Aggregations

The Gold layer also produces:

1. **`breweries_by_type.parquet`** - Global count by brewery type
2. **`breweries_by_country.parquet`** - Count by country
3. **`_summary.json`** - Comprehensive summary with all aggregations
