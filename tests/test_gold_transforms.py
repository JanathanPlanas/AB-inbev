"""
Unit tests for Gold layer transformations.
"""

import pytest
import pandas as pd

from src.transforms.gold_transforms import (
    aggregate_by_type_and_location,
    aggregate_by_type,
    aggregate_by_country,
    aggregate_by_state,
    create_gold_summary,
    get_aggregation_stats,
)


@pytest.fixture
def sample_silver_df():
    """Create a sample Silver DataFrame for testing."""
    return pd.DataFrame([
        {"id": "1", "name": "Brewery A", "brewery_type": "micro", "country": "United States", "state_province": "California"},
        {"id": "2", "name": "Brewery B", "brewery_type": "micro", "country": "United States", "state_province": "California"},
        {"id": "3", "name": "Brewery C", "brewery_type": "brewpub", "country": "United States", "state_province": "California"},
        {"id": "4", "name": "Brewery D", "brewery_type": "micro", "country": "United States", "state_province": "Oregon"},
        {"id": "5", "name": "Brewery E", "brewery_type": "nano", "country": "United States", "state_province": "Oregon"},
        {"id": "6", "name": "Brewery F", "brewery_type": "micro", "country": "Ireland", "state_province": "Dublin"},
        {"id": "7", "name": "Brewery G", "brewery_type": "brewpub", "country": "Ireland", "state_province": "Dublin"},
    ])


class TestAggregateByTypeAndLocation:
    """Tests for aggregate_by_type_and_location function."""
    
    def test_aggregates_correctly(self, sample_silver_df):
        """Test that aggregation produces correct counts."""
        result = aggregate_by_type_and_location(sample_silver_df)
        
        # Check structure
        assert "country" in result.columns
        assert "state_province" in result.columns
        assert "brewery_type" in result.columns
        assert "brewery_count" in result.columns
        
        # Check California micro count (should be 2)
        ca_micro = result[
            (result["country"] == "United States") & 
            (result["state_province"] == "California") & 
            (result["brewery_type"] == "micro")
        ]
        assert ca_micro["brewery_count"].iloc[0] == 2
    
    def test_total_count_matches_input(self, sample_silver_df):
        """Test that total count matches input records."""
        result = aggregate_by_type_and_location(sample_silver_df)
        
        assert result["brewery_count"].sum() == len(sample_silver_df)
    
    def test_custom_group_columns(self, sample_silver_df):
        """Test aggregation with custom grouping columns."""
        result = aggregate_by_type_and_location(
            sample_silver_df, 
            group_cols=["country", "brewery_type"]
        )
        
        assert "country" in result.columns
        assert "brewery_type" in result.columns
        assert "state_province" not in result.columns
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(columns=["id", "country", "state_province", "brewery_type"])
        result = aggregate_by_type_and_location(df)
        
        assert len(result) == 0
        assert "brewery_count" in result.columns


class TestAggregateByType:
    """Tests for aggregate_by_type function."""
    
    def test_aggregates_by_type(self, sample_silver_df):
        """Test aggregation by brewery type."""
        result = aggregate_by_type(sample_silver_df)
        
        assert "brewery_type" in result.columns
        assert "brewery_count" in result.columns
        
        # Check micro count (should be 4)
        micro = result[result["brewery_type"] == "micro"]
        assert micro["brewery_count"].iloc[0] == 4
    
    def test_sorted_by_count_descending(self, sample_silver_df):
        """Test that results are sorted by count descending."""
        result = aggregate_by_type(sample_silver_df)
        
        counts = result["brewery_count"].tolist()
        assert counts == sorted(counts, reverse=True)


class TestAggregateByCountry:
    """Tests for aggregate_by_country function."""
    
    def test_aggregates_by_country(self, sample_silver_df):
        """Test aggregation by country."""
        result = aggregate_by_country(sample_silver_df)
        
        assert "country" in result.columns
        assert "brewery_count" in result.columns
        
        # Check US count (should be 5)
        us = result[result["country"] == "United States"]
        assert us["brewery_count"].iloc[0] == 5
        
        # Check Ireland count (should be 2)
        ireland = result[result["country"] == "Ireland"]
        assert ireland["brewery_count"].iloc[0] == 2
    
    def test_total_matches_input(self, sample_silver_df):
        """Test that total matches input records."""
        result = aggregate_by_country(sample_silver_df)
        
        assert result["brewery_count"].sum() == len(sample_silver_df)


class TestAggregateByState:
    """Tests for aggregate_by_state function."""
    
    def test_aggregates_by_state(self, sample_silver_df):
        """Test aggregation by state."""
        result = aggregate_by_state(sample_silver_df)
        
        assert "country" in result.columns
        assert "state_province" in result.columns
        assert "brewery_count" in result.columns
    
    def test_filter_by_country(self, sample_silver_df):
        """Test filtering by country."""
        result = aggregate_by_state(sample_silver_df, country="United States")
        
        # Should only have US states
        assert all(result["country"] == "United States")
        assert len(result) == 2  # California and Oregon
    
    def test_nonexistent_country(self, sample_silver_df):
        """Test filtering by non-existent country."""
        result = aggregate_by_state(sample_silver_df, country="Brazil")
        
        assert len(result) == 0


class TestCreateGoldSummary:
    """Tests for create_gold_summary function."""
    
    def test_summary_contains_expected_keys(self, sample_silver_df):
        """Test that summary contains all expected keys."""
        result = create_gold_summary(sample_silver_df)
        
        assert "total_breweries" in result
        assert "total_countries" in result
        assert "total_states" in result
        assert "total_types" in result
        assert "by_type" in result
        assert "by_country" in result
        assert "top_states" in result
    
    def test_summary_counts_are_correct(self, sample_silver_df):
        """Test that summary counts are accurate."""
        result = create_gold_summary(sample_silver_df)
        
        assert result["total_breweries"] == 7
        assert result["total_countries"] == 2  # US and Ireland
        assert result["total_states"] == 3  # CA, OR, Dublin
        assert result["total_types"] == 3  # micro, brewpub, nano


class TestGetAggregationStats:
    """Tests for get_aggregation_stats function."""
    
    def test_stats_contain_expected_keys(self, sample_silver_df):
        """Test that stats contain all expected keys."""
        gold_df = aggregate_by_type_and_location(sample_silver_df)
        result = get_aggregation_stats(gold_df)
        
        assert "total_rows" in result
        assert "total_breweries" in result
        assert "unique_countries" in result
        assert "unique_states" in result
        assert "unique_types" in result
        assert "avg_breweries_per_group" in result
        assert "max_breweries_in_group" in result
        assert "min_breweries_in_group" in result
    
    def test_stats_values_are_correct(self, sample_silver_df):
        """Test that stat values are accurate."""
        gold_df = aggregate_by_type_and_location(sample_silver_df)
        result = get_aggregation_stats(gold_df)
        
        assert result["total_breweries"] == 7
        assert result["unique_countries"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
