"""
Unit tests for Silver layer transformations.
"""

import pytest
import pandas as pd
import numpy as np

from src.transforms.silver_transforms import (
    select_columns,
    standardize_types,
    handle_nulls,
    validate_coordinates,
    validate_brewery_type,
    deduplicate,
    clean_string_values,
    add_partition_columns,
    transform_bronze_to_silver,
    get_transformation_summary,
    SILVER_COLUMNS,
    VALID_BREWERY_TYPES,
)


@pytest.fixture
def sample_bronze_df():
    """Create a sample Bronze DataFrame for testing."""
    return pd.DataFrame([
        {
            "id": "brewery-1",
            "name": "Test Brewery One",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "address_2": None,
            "address_3": None,
            "city": "Portland",
            "state_province": "Oregon",
            "postal_code": "97201",
            "country": "United States",
            "longitude": "-122.6784",
            "latitude": "45.5152",
            "phone": "5031234567",
            "website_url": "https://testbrewery.com",
            "state": "Oregon",  # Deprecated column
            "street": "123 Main St",  # Deprecated column
            "_ingestion_date": "2025-01-08",  # Metadata column
            "_run_id": "20250108_120000",
        },
        {
            "id": "brewery-2",
            "name": "Test Brewery Two",
            "brewery_type": "brewpub",
            "address_1": "456 Oak Ave",
            "address_2": "Suite 100",
            "address_3": None,
            "city": "Seattle",
            "state_province": "Washington",
            "postal_code": "98101",
            "country": "United States",
            "longitude": "-122.3321",
            "latitude": "47.6062",
            "phone": None,
            "website_url": None,
            "state": "Washington",
            "street": "456 Oak Ave",
            "_ingestion_date": "2025-01-08",
            "_run_id": "20250108_120000",
        },
    ])


class TestSelectColumns:
    """Tests for select_columns function."""
    
    def test_selects_only_silver_columns(self, sample_bronze_df):
        """Test that only SILVER_COLUMNS are selected."""
        result = select_columns(sample_bronze_df)
        
        # Should not contain deprecated or metadata columns
        assert "state" not in result.columns
        assert "street" not in result.columns
        assert "_ingestion_date" not in result.columns
        assert "_run_id" not in result.columns
        
        # Should contain expected columns
        assert "id" in result.columns
        assert "name" in result.columns
        assert "brewery_type" in result.columns
    
    def test_handles_missing_columns(self):
        """Test handling of missing columns."""
        df = pd.DataFrame([{"id": "1", "name": "Test"}])
        result = select_columns(df)
        
        assert "id" in result.columns
        assert "name" in result.columns
        # Missing columns should not cause error
    
    def test_custom_column_list(self, sample_bronze_df):
        """Test with custom column list."""
        result = select_columns(sample_bronze_df, columns=["id", "name"])
        
        assert list(result.columns) == ["id", "name"]


class TestStandardizeTypes:
    """Tests for standardize_types function."""
    
    def test_coordinates_converted_to_float(self, sample_bronze_df):
        """Test that coordinates are converted to float."""
        df = select_columns(sample_bronze_df)
        result = standardize_types(df)
        
        assert result["longitude"].dtype == "float64"
        assert result["latitude"].dtype == "float64"
    
    def test_string_columns_converted(self, sample_bronze_df):
        """Test that string columns are converted properly."""
        df = select_columns(sample_bronze_df)
        result = standardize_types(df)
        
        assert result["name"].dtype == "string"
        assert result["city"].dtype == "string"
    
    def test_invalid_coordinates_become_nan(self):
        """Test that invalid coordinate strings become NaN."""
        df = pd.DataFrame([{
            "id": "1",
            "longitude": "invalid",
            "latitude": "not_a_number"
        }])
        result = standardize_types(df)
        
        assert pd.isna(result["longitude"].iloc[0])
        assert pd.isna(result["latitude"].iloc[0])


class TestHandleNulls:
    """Tests for handle_nulls function."""
    
    def test_none_converted_to_na(self):
        """Test that None values are converted to pandas NA."""
        df = pd.DataFrame([{
            "id": "1",
            "name": None,
            "city": "Portland"
        }])
        df = standardize_types(df)
        result = handle_nulls(df)
        
        assert pd.isna(result["name"].iloc[0])
    
    def test_empty_string_converted_to_na(self):
        """Test that empty strings are converted to NA."""
        df = pd.DataFrame([{
            "id": "1",
            "name": "",
            "city": "Portland"
        }])
        df = standardize_types(df)
        result = handle_nulls(df)
        
        assert pd.isna(result["name"].iloc[0])


class TestValidateCoordinates:
    """Tests for validate_coordinates function."""
    
    def test_valid_coordinates_unchanged(self):
        """Test that valid coordinates are not modified."""
        df = pd.DataFrame([{
            "latitude": 45.5152,
            "longitude": -122.6784
        }])
        result = validate_coordinates(df)
        
        assert result["latitude"].iloc[0] == 45.5152
        assert result["longitude"].iloc[0] == -122.6784
    
    def test_invalid_latitude_set_to_nan(self):
        """Test that invalid latitude values are set to NaN."""
        df = pd.DataFrame([
            {"latitude": 91.0, "longitude": 0.0},  # Too high
            {"latitude": -91.0, "longitude": 0.0},  # Too low
        ])
        result = validate_coordinates(df)
        
        assert pd.isna(result["latitude"].iloc[0])
        assert pd.isna(result["latitude"].iloc[1])
    
    def test_invalid_longitude_set_to_nan(self):
        """Test that invalid longitude values are set to NaN."""
        df = pd.DataFrame([
            {"latitude": 0.0, "longitude": 181.0},  # Too high
            {"latitude": 0.0, "longitude": -181.0},  # Too low
        ])
        result = validate_coordinates(df)
        
        assert pd.isna(result["longitude"].iloc[0])
        assert pd.isna(result["longitude"].iloc[1])
    
    def test_boundary_values_are_valid(self):
        """Test that boundary values are considered valid."""
        df = pd.DataFrame([
            {"latitude": 90.0, "longitude": 180.0},
            {"latitude": -90.0, "longitude": -180.0},
        ])
        result = validate_coordinates(df)
        
        assert result["latitude"].iloc[0] == 90.0
        assert result["longitude"].iloc[0] == 180.0
        assert result["latitude"].iloc[1] == -90.0
        assert result["longitude"].iloc[1] == -180.0


class TestValidateBreweryType:
    """Tests for validate_brewery_type function."""
    
    def test_valid_types_unchanged(self, sample_bronze_df):
        """Test that valid brewery types are unchanged."""
        df = select_columns(sample_bronze_df)
        result = validate_brewery_type(df)
        
        assert result["brewery_type"].iloc[0] == "micro"
        assert result["brewery_type"].iloc[1] == "brewpub"
    
    def test_all_valid_types_accepted(self):
        """Test that all documented brewery types are valid."""
        for brewery_type in VALID_BREWERY_TYPES:
            df = pd.DataFrame([{"brewery_type": brewery_type}])
            result = validate_brewery_type(df)
            assert result["brewery_type"].iloc[0] == brewery_type


class TestDeduplicate:
    """Tests for deduplicate function."""
    
    def test_removes_duplicate_ids(self):
        """Test that duplicate records are removed."""
        df = pd.DataFrame([
            {"id": "1", "name": "Brewery A"},
            {"id": "1", "name": "Brewery A Updated"},  # Duplicate
            {"id": "2", "name": "Brewery B"},
        ])
        result = deduplicate(df)
        
        assert len(result) == 2
        assert result["name"].iloc[0] == "Brewery A"  # First occurrence kept
    
    def test_no_duplicates_unchanged(self, sample_bronze_df):
        """Test that data without duplicates is unchanged."""
        df = select_columns(sample_bronze_df)
        result = deduplicate(df)
        
        assert len(result) == len(df)
    
    def test_custom_subset_columns(self):
        """Test deduplication with custom subset."""
        df = pd.DataFrame([
            {"id": "1", "name": "Brewery A", "city": "Portland"},
            {"id": "2", "name": "Brewery A", "city": "Portland"},  # Same name, different id
        ])
        result = deduplicate(df, subset=["name", "city"])
        
        assert len(result) == 1


class TestCleanStringValues:
    """Tests for clean_string_values function."""
    
    def test_strips_whitespace(self):
        """Test that whitespace is stripped from strings."""
        df = pd.DataFrame([{
            "id": "1",
            "name": "  Test Brewery  ",
            "city": "Portland  "
        }])
        df = standardize_types(df)
        result = clean_string_values(df)
        
        assert result["name"].iloc[0] == "Test Brewery"
        assert result["city"].iloc[0] == "Portland"
    
    def test_empty_strings_become_na(self):
        """Test that empty strings after strip become NA."""
        df = pd.DataFrame([{
            "id": "1",
            "name": "   ",  # Only whitespace
        }])
        df = standardize_types(df)
        result = clean_string_values(df)
        
        assert pd.isna(result["name"].iloc[0])


class TestAddPartitionColumns:
    """Tests for add_partition_columns function."""
    
    def test_null_country_becomes_unknown(self):
        """Test that null country is replaced with 'Unknown'."""
        df = pd.DataFrame([{
            "id": "1",
            "country": None,
            "state_province": "Oregon"
        }])
        result = add_partition_columns(df)
        
        assert result["country"].iloc[0] == "Unknown"
    
    def test_null_state_becomes_unknown(self):
        """Test that null state_province is replaced with 'Unknown'."""
        df = pd.DataFrame([{
            "id": "1",
            "country": "United States",
            "state_province": None
        }])
        result = add_partition_columns(df)
        
        assert result["state_province"].iloc[0] == "Unknown"
    
    def test_empty_string_becomes_unknown(self):
        """Test that empty strings are replaced with 'Unknown'."""
        df = pd.DataFrame([{
            "id": "1",
            "country": "",
            "state_province": ""
        }])
        result = add_partition_columns(df)
        
        assert result["country"].iloc[0] == "Unknown"
        assert result["state_province"].iloc[0] == "Unknown"


class TestTransformBronzeToSilver:
    """Tests for the main transformation function."""
    
    def test_full_transformation_pipeline(self, sample_bronze_df):
        """Test the complete transformation pipeline."""
        result = transform_bronze_to_silver(sample_bronze_df)
        
        # Check columns are correct
        assert "state" not in result.columns  # Deprecated removed
        assert "_ingestion_date" not in result.columns  # Metadata removed
        
        # Check types
        assert result["longitude"].dtype == "float64"
        assert result["latitude"].dtype == "float64"
        
        # Check no duplicates
        assert len(result) == len(result["id"].unique())
    
    def test_handles_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(columns=SILVER_COLUMNS)
        result = transform_bronze_to_silver(df)
        
        assert len(result) == 0
        assert list(result.columns) == SILVER_COLUMNS


class TestGetTransformationSummary:
    """Tests for get_transformation_summary function."""
    
    def test_summary_contains_expected_keys(self, sample_bronze_df):
        """Test that summary contains all expected keys."""
        silver_df = transform_bronze_to_silver(sample_bronze_df)
        summary = get_transformation_summary(sample_bronze_df, silver_df)
        
        assert "bronze_record_count" in summary
        assert "silver_record_count" in summary
        assert "records_removed" in summary
        assert "columns_removed" in summary
        assert "null_counts" in summary
        assert "brewery_types" in summary
        assert "countries" in summary
    
    def test_summary_counts_are_correct(self, sample_bronze_df):
        """Test that summary counts are accurate."""
        silver_df = transform_bronze_to_silver(sample_bronze_df)
        summary = get_transformation_summary(sample_bronze_df, silver_df)
        
        assert summary["bronze_record_count"] == len(sample_bronze_df)
        assert summary["silver_record_count"] == len(silver_df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
