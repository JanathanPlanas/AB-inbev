"""Unit tests for Silver layer transformations (DuckDB + PyArrow)."""

import pytest
import pyarrow as pa

from src.transforms.silver_transforms import (
    DuckDBTransformer,
    transform_bronze_to_silver,
    get_transformation_summary,
    arrow_table_from_pylist,
    arrow_table_to_pylist,
    get_column_as_list,
)


@pytest.fixture
def sample_bronze_data():
    """Create sample Bronze layer data."""
    return [
        {"id": "1", "name": "  Brewery One  ", "brewery_type": "MICRO", "country": "United States", "state_province": "California", "longitude": "-122.4194", "latitude": "37.7749"},
        {"id": "2", "name": "Brewery Two", "brewery_type": "brewpub", "country": "United States", "state_province": "Oregon", "longitude": "-122.6765", "latitude": "45.5231"},
        {"id": "3", "name": "Dublin Brew", "brewery_type": "micro", "country": "Ireland", "state_province": "Dublin", "longitude": "-6.2603", "latitude": "53.3498"},
    ]


class TestDuckDBTransformer:
    """Tests for DuckDBTransformer class."""
    
    def test_transform_from_list(self, sample_bronze_data):
        """Test transformation from list."""
        result = transform_bronze_to_silver(sample_bronze_data)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
    
    def test_transform_from_arrow(self, sample_bronze_data):
        """Test transformation from PyArrow Table."""
        table = pa.Table.from_pylist(sample_bronze_data)
        result = transform_bronze_to_silver(table)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
    
    def test_brewery_type_lowercase(self, sample_bronze_data):
        """Test brewery types are lowercase."""
        result = transform_bronze_to_silver(sample_bronze_data)
        types = get_column_as_list(result, "brewery_type")
        assert "micro" in types
        assert "MICRO" not in types
    
    def test_string_trimming(self, sample_bronze_data):
        """Test strings are trimmed."""
        result = transform_bronze_to_silver(sample_bronze_data)
        names = get_column_as_list(result, "name")
        assert "Brewery One" in names
    
    def test_deduplication(self):
        """Test duplicates are removed."""
        data = [
            {"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA"},
            {"id": "1", "name": "A2", "brewery_type": "micro", "country": "US", "state_province": "CA"},
            {"id": "2", "name": "B", "brewery_type": "nano", "country": "US", "state_province": "OR"},
        ]
        result = transform_bronze_to_silver(data)
        assert result.num_rows == 2
    
    def test_null_id_removed(self):
        """Test records with null ID are removed."""
        data = [
            {"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA"},
            {"id": None, "name": "B", "brewery_type": "micro", "country": "US", "state_province": "CA"},
        ]
        result = transform_bronze_to_silver(data)
        assert result.num_rows == 1
    
    def test_empty_country_becomes_unknown(self):
        """Test empty country becomes 'Unknown'."""
        data = [{"id": "1", "name": "A", "brewery_type": "micro", "country": "", "state_province": "CA"}]
        result = transform_bronze_to_silver(data)
        countries = get_column_as_list(result, "country")
        assert countries[0] == "Unknown"
    
    def test_empty_state_becomes_unknown(self):
        """Test empty state becomes 'Unknown'."""
        data = [{"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": ""}]
        result = transform_bronze_to_silver(data)
        states = get_column_as_list(result, "state_province")
        assert states[0] == "Unknown"


class TestCoordinateValidation:
    """Tests for coordinate validation."""
    
    def test_valid_coordinates(self):
        """Test valid coordinates are kept."""
        data = [{"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA", "latitude": "37.7749", "longitude": "-122.4194"}]
        result = transform_bronze_to_silver(data)
        lat = get_column_as_list(result, "latitude")[0]
        lon = get_column_as_list(result, "longitude")[0]
        assert abs(lat - 37.7749) < 0.001
        assert abs(lon - (-122.4194)) < 0.001
    
    def test_invalid_latitude_null(self):
        """Test invalid latitude becomes NULL."""
        data = [{"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA", "latitude": "100", "longitude": "-122"}]
        result = transform_bronze_to_silver(data)
        lat = get_column_as_list(result, "latitude")[0]
        assert lat is None
    
    def test_invalid_longitude_null(self):
        """Test invalid longitude becomes NULL."""
        data = [{"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA", "latitude": "37", "longitude": "-200"}]
        result = transform_bronze_to_silver(data)
        lon = get_column_as_list(result, "longitude")[0]
        assert lon is None
    
    def test_boundary_coordinates(self):
        """Test boundary coordinates are valid."""
        data = [
            {"id": "1", "name": "A", "brewery_type": "micro", "country": "US", "state_province": "CA", "latitude": "90", "longitude": "180"},
            {"id": "2", "name": "B", "brewery_type": "micro", "country": "US", "state_province": "CA", "latitude": "-90", "longitude": "-180"},
        ]
        result = transform_bronze_to_silver(data)
        lats = get_column_as_list(result, "latitude")
        assert lats[0] == 90
        assert lats[1] == -90


class TestHelperFunctions:
    """Tests for helper functions."""
    
    def test_arrow_table_from_pylist(self, sample_bronze_data):
        """Test arrow_table_from_pylist."""
        table = arrow_table_from_pylist(sample_bronze_data)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
    
    def test_arrow_table_to_pylist(self, sample_bronze_data):
        """Test arrow_table_to_pylist."""
        table = pa.Table.from_pylist(sample_bronze_data)
        result = arrow_table_to_pylist(table)
        assert isinstance(result, list)
        assert len(result) == 3
    
    def test_get_column_as_list(self, sample_bronze_data):
        """Test get_column_as_list."""
        table = pa.Table.from_pylist(sample_bronze_data)
        ids = get_column_as_list(table, "id")
        assert ids == ["1", "2", "3"]


class TestEmptyData:
    """Tests for empty data handling."""
    
    def test_empty_list(self):
        """Test transformation with empty list."""
        result = transform_bronze_to_silver([])
        assert isinstance(result, pa.Table)
        assert result.num_rows == 0


class TestTransformationSummary:
    """Tests for transformation summary."""
    
    def test_summary_keys(self, sample_bronze_data):
        """Test summary contains expected keys."""
        bronze = pa.Table.from_pylist(sample_bronze_data)
        silver = transform_bronze_to_silver(sample_bronze_data)
        summary = get_transformation_summary(bronze, silver)
        
        assert "bronze_record_count" in summary
        assert "silver_record_count" in summary
        assert "records_removed" in summary
    
    def test_summary_counts(self, sample_bronze_data):
        """Test summary counts are correct."""
        bronze = pa.Table.from_pylist(sample_bronze_data)
        silver = transform_bronze_to_silver(sample_bronze_data)
        summary = get_transformation_summary(bronze, silver)
        
        assert summary["bronze_record_count"] == 3
        assert summary["silver_record_count"] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
