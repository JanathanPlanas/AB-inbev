# Gold Transforms - Test Documentation

## Overview

Unit tests for the Gold layer aggregation module using **DuckDB + PyArrow** (no Pandas dependency).

**Test File:** `tests/unit/test_gold_transforms.py`

---

## Test Classes

### TestAggregateByTypeAndLocation

Tests for the main aggregation function.

| Test | Description |
|------|-------------|
| `test_aggregates_correctly` | Verifies correct output schema |
| `test_total_matches_input` | Sum of counts equals input rows |
| `test_custom_group_columns` | Custom grouping works |
| `test_california_micro_count` | Specific aggregation value correct |

### TestAggregateByType

Tests for type-only aggregation.

| Test | Description |
|------|-------------|
| `test_aggregates_by_type` | Produces correct schema |
| `test_micro_count` | Micro brewery count correct |
| `test_sorted_descending` | Results sorted by count DESC |

### TestAggregateByCountry

Tests for country-level aggregation.

| Test | Description |
|------|-------------|
| `test_aggregates_by_country` | Produces correct schema |
| `test_us_count` | US brewery count correct |
| `test_ireland_count` | Ireland brewery count correct |

### TestAggregateByState

Tests for state-level aggregation.

| Test | Description |
|------|-------------|
| `test_aggregates_by_state` | Produces correct schema |
| `test_filter_by_country` | Country filter works |
| `test_nonexistent_country` | Handles missing country gracefully |

### TestGoldSummary

Tests for comprehensive summary.

| Test | Description |
|------|-------------|
| `test_summary_keys` | All expected keys present |
| `test_summary_values` | Values are accurate |

### TestAggregationStats

Tests for aggregation statistics.

| Test | Description |
|------|-------------|
| `test_stats_keys` | All stat keys present |
| `test_stats_values` | Stats are accurate |

### TestEmptyData

Tests for edge cases.

| Test | Description |
|------|-------------|
| `test_empty_table` | Handles empty input gracefully |

---

## Aggregations Tested

| Aggregation | Group By | Output |
|-------------|----------|--------|
| Main | country, state, type | brewery_count |
| By Type | brewery_type | brewery_count |
| By Country | country | brewery_count |
| By State | country, state | brewery_count |

---

## Running Tests

```bash
# Run Gold transform tests
pytest tests/unit/test_gold_transforms.py -v

# Run with coverage
pytest tests/unit/test_gold_transforms.py -v --cov=src.transforms.gold_transforms
```

---

## Technology

- **DuckDB**: SQL aggregations (GROUP BY, COUNT)
- **PyArrow**: Data format (no Pandas)
- **pytest**: Test framework

---

## Test Summary

| Category | Tests | Status |
|----------|-------|--------|
| Aggregate By Type And Location | 4 | ✅ |
| Aggregate By Type | 3 | ✅ |
| Aggregate By Country | 3 | ✅ |
| Aggregate By State | 3 | ✅ |
| Gold Summary | 2 | ✅ |
| Aggregation Stats | 2 | ✅ |
| Empty Input Handling | 1 | ✅ |
| **Total** | **18** | ✅ |
