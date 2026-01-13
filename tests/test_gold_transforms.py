"""Unit tests for Gold layer transformations (DuckDB + PyArrow)."""

import pytest
import pyarrow as pa

from src.transforms.gold_transforms import (
    DuckDBAggregator,
    aggregate_by_type_and_location,
    aggregate_by_type,
    aggregate_by_country,
    aggregate_by_state,
    create_gold_summary,
    get_aggregation_stats,
)


@pytest.fixture
def sample_silver_table():
    """Create sample Silver layer data as PyArrow Table."""
    data = [
        {"id": "1", "name": "Brewery A", "brewery_type": "micro", "country": "United States", "state_province": "California"},
        {"id": "2", "name": "Brewery B", "brewery_type": "micro", "country": "United States", "state_province": "California"},
        {"id": "3", "name": "Brewery C", "brewery_type": "brewpub", "country": "United States", "state_province": "California"},
        {"id": "4", "name": "Brewery D", "brewery_type": "micro", "country": "United States", "state_province": "Oregon"},
        {"id": "5", "name": "Brewery E", "brewery_type": "nano", "country": "United States", "state_province": "Oregon"},
        {"id": "6", "name": "Brewery F", "brewery_type": "micro", "country": "Ireland", "state_province": "Dublin"},
        {"id": "7", "name": "Brewery G", "brewery_type": "brewpub", "country": "Ireland", "state_province": "Dublin"},
    ]
    return pa.Table.from_pylist(data)


class TestAggregateByTypeAndLocation:
    """Tests for aggregate_by_type_and_location."""
    
    def test_aggregates_correctly(self, sample_silver_table):
        """Test aggregation produces correct counts."""
        result = aggregate_by_type_and_location(sample_silver_table)
        
        assert isinstance(result, pa.Table)
        assert "country" in result.column_names
        assert "state_province" in result.column_names
        assert "brewery_type" in result.column_names
        assert "brewery_count" in result.column_names
    
    def test_total_matches_input(self, sample_silver_table):
        """Test total count matches input."""
        result = aggregate_by_type_and_location(sample_silver_table)
        total = sum(result.column("brewery_count").to_pylist())
        assert total == 7
    
    def test_custom_group_columns(self, sample_silver_table):
        """Test custom grouping columns."""
        result = aggregate_by_type_and_location(sample_silver_table, group_cols=["country", "brewery_type"])
        
        assert "country" in result.column_names
        assert "brewery_type" in result.column_names
        assert "state_province" not in result.column_names
    
    def test_california_micro_count(self, sample_silver_table):
        """Test specific aggregation value."""
        result = aggregate_by_type_and_location(sample_silver_table)
        data = result.to_pylist()
        
        ca_micro = [r for r in data if r["country"] == "United States" 
                    and r["state_province"] == "California" 
                    and r["brewery_type"] == "micro"]
        
        assert len(ca_micro) == 1
        assert ca_micro[0]["brewery_count"] == 2


class TestAggregateByType:
    """Tests for aggregate_by_type."""
    
    def test_aggregates_by_type(self, sample_silver_table):
        """Test aggregation by type."""
        result = aggregate_by_type(sample_silver_table)
        
        assert isinstance(result, pa.Table)
        assert "brewery_type" in result.column_names
        assert "brewery_count" in result.column_names
    
    def test_micro_count(self, sample_silver_table):
        """Test micro brewery count."""
        result = aggregate_by_type(sample_silver_table)
        data = result.to_pylist()
        
        micro = [r for r in data if r["brewery_type"] == "micro"]
        assert micro[0]["brewery_count"] == 4
    
    def test_sorted_descending(self, sample_silver_table):
        """Test results are sorted by count descending."""
        result = aggregate_by_type(sample_silver_table)
        counts = result.column("brewery_count").to_pylist()
        
        assert counts == sorted(counts, reverse=True)


class TestAggregateByCountry:
    """Tests for aggregate_by_country."""
    
    def test_aggregates_by_country(self, sample_silver_table):
        """Test aggregation by country."""
        result = aggregate_by_country(sample_silver_table)
        
        assert isinstance(result, pa.Table)
        assert "country" in result.column_names
        assert "brewery_count" in result.column_names
    
    def test_us_count(self, sample_silver_table):
        """Test US brewery count."""
        result = aggregate_by_country(sample_silver_table)
        data = result.to_pylist()
        
        us = [r for r in data if r["country"] == "United States"]
        assert us[0]["brewery_count"] == 5
    
    def test_ireland_count(self, sample_silver_table):
        """Test Ireland brewery count."""
        result = aggregate_by_country(sample_silver_table)
        data = result.to_pylist()
        
        ireland = [r for r in data if r["country"] == "Ireland"]
        assert ireland[0]["brewery_count"] == 2


class TestAggregateByState:
    """Tests for aggregate_by_state."""
    
    def test_aggregates_by_state(self, sample_silver_table):
        """Test aggregation by state."""
        result = aggregate_by_state(sample_silver_table)
        
        assert isinstance(result, pa.Table)
        assert "country" in result.column_names
        assert "state_province" in result.column_names
        assert "brewery_count" in result.column_names
    
    def test_filter_by_country(self, sample_silver_table):
        """Test filtering by country."""
        result = aggregate_by_state(sample_silver_table, country="United States")
        data = result.to_pylist()
        
        assert all(r["country"] == "United States" for r in data)
        assert len(data) == 2  # California and Oregon
    
    def test_nonexistent_country(self, sample_silver_table):
        """Test filtering by non-existent country."""
        result = aggregate_by_state(sample_silver_table, country="Brazil")
        assert result.num_rows == 0


class TestGoldSummary:
    """Tests for create_gold_summary."""
    
    def test_summary_keys(self, sample_silver_table):
        """Test summary contains expected keys."""
        result = create_gold_summary(sample_silver_table)
        
        assert "total_breweries" in result
        assert "total_countries" in result
        assert "total_states" in result
        assert "total_types" in result
        assert "by_type" in result
        assert "by_country" in result
    
    def test_summary_values(self, sample_silver_table):
        """Test summary values are correct."""
        result = create_gold_summary(sample_silver_table)
        
        assert result["total_breweries"] == 7
        assert result["total_countries"] == 2
        assert result["total_states"] == 3
        assert result["total_types"] == 3


class TestAggregationStats:
    """Tests for get_aggregation_stats."""
    
    def test_stats_keys(self, sample_silver_table):
        """Test stats contain expected keys."""
        gold = aggregate_by_type_and_location(sample_silver_table)
        stats = get_aggregation_stats(gold)
        
        assert "total_rows" in stats
        assert "total_breweries" in stats
        assert "unique_countries" in stats
        assert "avg_breweries_per_group" in stats
        assert "max_breweries_in_group" in stats
        assert "min_breweries_in_group" in stats
    
    def test_stats_values(self, sample_silver_table):
        """Test stats values are correct."""
        gold = aggregate_by_type_and_location(sample_silver_table)
        stats = get_aggregation_stats(gold)
        
        assert stats["total_breweries"] == 7
        assert stats["unique_countries"] == 2


class TestEmptyData:
    """Tests for empty data handling."""
    
    def test_empty_table(self):
        """Test aggregation with empty table."""
        empty = pa.Table.from_pylist([])
        result = aggregate_by_type_and_location(empty)
        assert result.num_rows == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
