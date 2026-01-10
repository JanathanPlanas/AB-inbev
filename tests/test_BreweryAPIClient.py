"""
Unit tests for the BreweryAPIClient.

These tests use mocking to simulate API responses, allowing testing
without actual network calls.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from src.config.configuration import config as cfg
from src.clients.BreweryAPIClient import (
    BreweryAPIClient,
    BreweryAPIError,
    APIConfig,
    fetch_all_breweries,
)


# Sample test data
SAMPLE_BREWERY = {
    "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
    "name": "10-56 Brewing Company",
    "brewery_type": "micro",
    "address_1": "400 Brown Cir",
    "address_2": None,
    "address_3": None,
    "city": "Knox",
    "state_province": "Indiana",
    "postal_code": "46534",
    "country": "United States",
    "longitude": "-86.627954",
    "latitude": "41.289715",
    "phone": "6305781056",
    "website_url": None,
    "state": "Indiana",
    "street": "400 Brown Cir"
}

SAMPLE_METADATA = {
    "total": "8343",
    "page": "1",
    "per_page": "50"
}


class TestBreweryAPIClient:
    """Test suite for BreweryAPIClient."""
    
    def test_init_default_config(self):
        """Test client initialization with default config."""
        client = BreweryAPIClient()
        
        assert client.config.base_url == "https://api.openbrewerydb.org/v1"
        assert client.config.per_page == 200
        assert client.config.max_retries == 3
    
    def test_init_custom_config(self):
        """Test client initialization with custom config."""
        custom_config = APIConfig(
            base_url="https://custom.api.com",
            per_page=100,
            max_retries=5
        )
        client = BreweryAPIClient(config=custom_config)
        
        assert client.config.base_url == "https://custom.api.com"
        assert client.config.per_page == 100
        assert client.config.max_retries == 5
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_get_metadata_success(self, mock_session_class):
        """Test successful metadata fetch."""
        # Setup mock
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_METADATA
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        result = client.get_metadata()
        
        assert result == SAMPLE_METADATA
        assert result["total"] == "8343"
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_get_breweries_page_success(self, mock_session_class):
        """Test successful single page fetch."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = [SAMPLE_BREWERY]
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        result = client.get_breweries_page(page=1, per_page=50)
        
        assert len(result) == 1
        assert result[0]["name"] == "10-56 Brewing Company"
        assert result[0]["brewery_type"] == "micro"
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_get_brewery_by_id_success(self, mock_session_class):
        """Test fetching a single brewery by ID."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_BREWERY
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        result = client.get_brewery_by_id("5128df48-79fc-4f0f-8b52-d06be54d0cec")
        
        assert result["id"] == "5128df48-79fc-4f0f-8b52-d06be54d0cec"
        assert result["name"] == "10-56 Brewing Company"
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_get_all_breweries_pagination(self, mock_session_class):
        """Test pagination handling in get_all_breweries."""
        mock_session = MagicMock()
        
        # Create sample breweries for pagination test
        page1_breweries = [{"id": f"brewery-{i}", "name": f"Brewery {i}"} for i in range(200)]
        page2_breweries = [{"id": f"brewery-{i}", "name": f"Brewery {i}"} for i in range(200, 350)]
        
        # Mock responses for metadata, page 1, page 2, and empty page 3
        responses = [
            Mock(json=Mock(return_value={"total": "350"}), raise_for_status=Mock()),  # metadata
            Mock(json=Mock(return_value=page1_breweries), raise_for_status=Mock()),   # page 1
            Mock(json=Mock(return_value=page2_breweries), raise_for_status=Mock()),   # page 2
        ]
        mock_session.get.side_effect = responses
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        result = client.get_all_breweries()
        
        assert len(result) == 350
        assert result[0]["id"] == "brewery-0"
        assert result[349]["id"] == "brewery-349"
    
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_api_timeout_error(self, mock_session_class):
        """Test handling of timeout errors."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.Timeout("Connection timed out")
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        with pytest.raises(BreweryAPIError) as exc_info:
            client.get_metadata()
        
        assert "timeout" in str(exc_info.value).lower()
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_api_http_error(self, mock_session_class):
        """Test handling of HTTP errors."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=Mock(status_code=500, text="Internal Server Error")
        )
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        with pytest.raises(BreweryAPIError) as exc_info:
            client.get_metadata()
        
        assert "HTTP error" in str(exc_info.value)
    
    @patch('src.clients.BreweryAPIClient.requests.Session')
    def test_filters_passed_correctly(self, mock_session_class):
        """Test that filters are passed correctly to API."""
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = [SAMPLE_BREWERY]
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = BreweryAPIClient()
        client.session = mock_session
        
        client.get_breweries_page(
            page=1,
            per_page=50,
            by_state="California",
            by_type="micro"
        )
        
        # Check that the correct parameters were passed
        call_args = mock_session.get.call_args
        params = call_args.kwargs.get('params') or call_args[1].get('params')
        
        assert params["by_state"] == "California"
        assert params["by_type"] == "micro"
        assert params["page"] == 1


class TestFetchAllBreweriesFunction:
    """Test suite for the convenience function."""
    
    @patch('src.clients.BreweryAPIClient.BreweryAPIClient')
    def test_fetch_all_breweries_calls_client(self, mock_client_class):
        """Test that the convenience function uses the client correctly."""
        mock_client = Mock()
        mock_client.get_all_breweries.return_value = [SAMPLE_BREWERY]
        mock_client_class.return_value = mock_client
        
        result = fetch_all_breweries(by_type="micro")
        
        mock_client.get_all_breweries.assert_called_once_with(by_type="micro")
        assert len(result) == 1


class TestAPIConfig:
    """Test suite for APIConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = APIConfig()
        
        assert config.base_url == cfg.api.base_url
        assert config.per_page == cfg.params.per_page
        assert config.max_retries == cfg.params.max_retries
        assert config.backoff_factor == cfg.params.backoff_factor
        assert config.timeout == cfg.params.timeout
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = APIConfig(
            base_url="https://test.api.com",
            per_page=100,
            max_retries=5,
            backoff_factor=1.0,
            timeout=60
        )
        
        assert config.base_url == "https://test.api.com"
        assert config.per_page == 100
        assert config.max_retries == 5
        assert config.backoff_factor == 1.0
        assert config.timeout == 60


if __name__ == "__main__":
    pytest.main([__file__, "-v"])