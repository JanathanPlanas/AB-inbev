
"""
OpenBreweryDB API Client

A robust client for consuming the Open Brewery DB API with support for:
- Pagination handling
- Retry logic with exponential backoff
- Error handling and logging
- Rate limiting awareness

API Documentation: https://www.openbrewerydb.org/documentation
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.config.configuration import config

from datetime import datetime

from src.io.raw_writer import RawJsonlGzWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configuration for the API client."""
    base_url: str = config.api.base_url
    per_page: int = config.params.per_page  # Maximum allowed by API
    max_retries: int = config.params.max_retries
    backoff_factor: float = config.params.backoff_factor
    timeout: int = config.params.timeout


class BreweryAPIError(Exception):
    """Custom exception for API errors."""
    pass


class BreweryAPIClient:
    """
    Features:
        - Automatic pagination to fetch all breweries
        - Retry logic with exponential backoff
        - Comprehensive error handling
        - Metadata fetching for total count
    """
    
    def __init__(self, config: Optional[APIConfig] = None):
        """ Initialize the API client. """
        self.config = config or APIConfig()
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> Any:
        """
        Make a GET request to the API.  
        Args:
            endpoint: API endpoint (e.g., "/breweries")
            params: Optional query parameters
            
        Returns:
            JSON response from the API
            
        """
        url = f"{self.config.base_url}{endpoint}"
        
        try:
            logger.debug(f"Making request to {url} with params {params}")
            
            response = self.session.get(
                url,
                params=params,
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout: {url}")
            raise BreweryAPIError(f"Request timeout: {e}")
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise BreweryAPIError(f"HTTP error: {e}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise BreweryAPIError(f"Request failed: {e}")
    
    def get_metadata(self, **filters) -> dict:
        """
        Get metadata about breweries (total count, etc.).
        Args:
            **filters: Optional filters (by_city, by_state, by_type, etc.)
        Returns:
            Dictionary with metadata including total count
        Example:
            >>> client = BreweryAPIClient()
            >>> meta = client.get_metadata()
            >>> print(f"Total breweries: {meta['total']}")
        """
        logger.info("Fetching metadata from API")
        return self._make_request("/breweries/meta", params=filters)
    
     
    def get_brewery_by_id(self, brewery_id: str) -> dict:
        """ Get a single brewery by its ID. """
        logger.info(f"Fetching brewery with ID: {brewery_id}")
        return self._make_request(f"/breweries/{brewery_id}")
 
    
    def get_breweries_page(
        self, 
        page: int = 1, 
        per_page: Optional[int] = None,
        **filters
    ) -> list[dict]:
        """ Get a single page of breweries."""
        params = {
            "page": page,
            "per_page": per_page or self.config.per_page,
            **filters
        }
        
        logger.debug(f"Fetching page {page}")
        return self._make_request("/breweries", params=params)
    
    def get_all_breweries(self, writer: Optional[RawJsonlGzWriter] = None, **filters) -> list[dict]:
        """
        Fetch all breweries with automatic pagination.
        Se writer for passado, salva RAW por página em JSONL.GZ.
        """
        all_breweries = []  # só usado se você realmente quiser manter em memória
        page = 1
        total = "unknown"

        # metadata (se disponível)
        try:
            metadata = self.get_metadata(**filters)
            total = int(metadata.get("total", "unknown"))
            logger.info(f"Total breweries to fetch: {total}")
        except Exception as e:
            logger.warning(f"Could not fetch metadata: {e}")

        start_ts = datetime.now().isoformat(timespec="seconds")

        while True:
            logger.info(f"Fetching page {page}...")

            breweries = self.get_breweries_page(page=page, **filters)

            if not breweries:
                logger.info(f"No more results at page {page}. Pagination complete.")
                break

            # ✅ salva RAW por página (sem transformar)
            if writer is not None:
                out_path = writer.write_page(page=page, records=breweries)
                logger.info(f"Saved RAW page {page} -> {out_path}")
            else:
                # se não passar writer, mantém comportamento antigo
                all_breweries.extend(breweries)

            logger.info(
                f"Page {page}: fetched {len(breweries)} breweries "
                f"(total so far: {len(all_breweries) if writer is None else 'streaming'})"
            )

            # última página
            if len(breweries) < self.config.per_page:
                logger.info("Last page reached (partial results).")
                break

            page += 1
            time.sleep(0.1)

        end_ts = datetime.now().isoformat(timespec="seconds")

        # manifest (opcional e muito bom pro case)
        if writer is not None:
            writer.write_manifest(
                {
                    "source": "openbrewerydb",
                    "endpoint": "/breweries",
                    "filters": filters,
                    "started_at": start_ts,
                    "finished_at": end_ts,
                    "total_metadata": total,
                    "pages_written": page,
                    "per_page": self.config.per_page,
                }
            )

        logger.info("Finished fetching breweries.")
        return all_breweries  # se writer != None, vai voltar [] (por design)
        

# Convenience function for quick usage
def fetch_all_breweries(**filters) -> list[dict]:
    """ Convenience function to fetch all breweries  """
    client = BreweryAPIClient()
    return client.get_all_breweries(**filters)


if __name__ == "__main__":
    # Example usage and quick test
    client = BreweryAPIClient()
    
    # Fetch metadata
    print("=" * 50)
    print("Fetching metadata...")
    meta = client.get_metadata()
    print(f"Total breweries in database: {meta}")
    
    # Fetch first page
    print("=" * 50)
    print("Fetching first page...")
    first_page = client.get_breweries_page(page=1, per_page=5)
    print(f"First 5 breweries:")
    for brewery in first_page:
        print(f"  - {brewery['name']} ({brewery['brewery_type']}) - {brewery['city']}, {brewery['state_province']}")
    
    # Fetch all breweries (commented out for quick test)
    print("=" * 50)
    print("Fetching all breweries...")
    all_breweries = client.get_all_breweries()
    print(f"Total fetched: {len(all_breweries)}")