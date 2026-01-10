"""
Bronze Layer Pipeline - Raw Data Ingestion

This pipeline fetches all breweries from the Open Brewery DB API
and persists them in the bronze layer as raw JSONL files.

Usage:
    python -m src.pipelines.bronze_layer
    
Output:
    data/bronze/breweries/ingestion_date=YYYY-MM-DD/run_id=YYYYMMDD_HHMMSS/
        ├── page=0001.jsonl.gz
        ├── page=0002.jsonl.gz
        ├── ...
        └── _manifest.json
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.clients.BreweryAPIClient import BreweryAPIClient, BreweryAPIError, APIConfig
from src.io.raw_writer import RawJsonlGzWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BronzeLayerPipeline:
    """
    Pipeline for ingesting raw brewery data into the bronze layer.
    
    This pipeline:
    1. Fetches all breweries from the API with pagination
    2. Writes each page as a separate gzipped JSONL file
    3. Creates a manifest with ingestion metadata
    
    Attributes:
        client: API client for fetching data
        writer: Writer for persisting raw data
        
    Example:
        >>> pipeline = BronzeLayerPipeline()
        >>> result = pipeline.run()
        >>> print(f"Ingested {result['total_records']} breweries")
    """
    
    def __init__(
        self,
        base_dir: str = "data/bronze/breweries",
        api_config: Optional[APIConfig] = None
    ):
        """
        Initialize the bronze layer pipeline.
        
        Args:
            base_dir: Base directory for bronze layer storage
            api_config: Optional API configuration
        """
        self.client = BreweryAPIClient(config=api_config)
        self.writer = RawJsonlGzWriter(base_dir=base_dir)
        
    def run(self) -> dict:
        """
        Execute the bronze layer ingestion pipeline.
        
        Returns:
            Dictionary with ingestion summary including:
            - total_records: Total number of records ingested
            - total_pages: Number of pages fetched
            - run_dir: Path to the output directory
            - status: "success" or "failed"
            
        Raises:
            BreweryAPIError: If API calls fail after retries
        """
        logger.info("=" * 60)
        logger.info("Starting Bronze Layer Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Step 1: Get metadata for logging
            logger.info("Fetching metadata from API...")
            metadata = self._get_metadata_safe()
            total_expected = metadata.get("total", "unknown") if metadata else "unknown"
            logger.info(f"Expected total breweries: {total_expected}")
            
            # Step 2: Fetch and write pages
            logger.info("Starting paginated data fetch...")
            self._fetch_and_write_all_pages()
            
            # Step 3: Write manifest
            logger.info("Writing manifest...")
            manifest_metadata = {
                "source": "https://api.openbrewerydb.org/v1/breweries",
                "expected_total": total_expected,
                "pipeline": "bronze_layer",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now(timezone.utc).isoformat()
            }
            self.writer.write_manifest(extra_metadata=manifest_metadata)
            
            # Step 4: Summary
            summary = self.writer.get_summary()
            summary["status"] = "success"
            summary["expected_total"] = total_expected
            
            logger.info("=" * 60)
            logger.info("Bronze Layer Pipeline Completed Successfully!")
            logger.info(f"Total records: {summary['total_records']}")
            logger.info(f"Total pages: {summary['total_pages']}")
            logger.info(f"Output directory: {summary['run_dir']}")
            logger.info("=" * 60)
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def _get_metadata_safe(self) -> Optional[dict]:
        """Fetch metadata, returning None if it fails."""
        try:
            return self.client.get_metadata()
        except BreweryAPIError as e:
            logger.warning(f"Could not fetch metadata: {e}")
            return None
    
    def _fetch_and_write_all_pages(self) -> None:
        """Fetch all pages from API and write each to bronze layer."""
        page = 1
        per_page = self.client.config.per_page
        
        while True:
            logger.info(f"Fetching page {page}...")
            
            breweries = self.client.get_breweries_page(page=page, per_page=per_page)
            
            if not breweries:
                logger.info(f"No more results at page {page}. Pagination complete.")
                break
            
            # Write page to bronze layer
            self.writer.write_page(page=page, records=breweries)
            
            logger.info(f"Page {page}: fetched and written {len(breweries)} breweries")
            
            # Check if we've reached the last page
            if len(breweries) < per_page:
                logger.info("Last page reached (partial results).")
                break
            
            page += 1
        
        logger.info(f"Completed fetching {page} pages")


def run_bronze_pipeline(base_dir: str = "data/bronze/breweries") -> dict:
    """
    Convenience function to run the bronze layer pipeline.
    
    Args:
        base_dir: Base directory for output
        
    Returns:
        Pipeline execution summary
    """
    pipeline = BronzeLayerPipeline(base_dir=base_dir)
    return pipeline.run()


if __name__ == "__main__":
    # Run the pipeline
    try:
        result = run_bronze_pipeline()
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Records: {result['total_records']}")
        print(f"   Pages: {result['total_pages']}")
        print(f"   Output: {result['run_dir']}")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)