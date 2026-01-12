"""
Silver Layer Pipeline - Data Transformation.

This pipeline reads raw data from the Bronze layer, applies
transformations, and writes partitioned Parquet files to Silver.

Usage:
    python -m src.pipelines.silver_layer
    
Output:
    data/silver/breweries/
        ├── country=United States/
        │   ├── state_province=California/
        │   │   └── part-0001.parquet
        │   ├── state_province=Texas/
        │   │   └── part-0001.parquet
        │   └── ...
        ├── country=Ireland/
        │   └── ...
        └── _SUCCESS
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.io.bronze_reader import BronzeReader
from src.transforms.silver_transforms import (
    transform_bronze_to_silver,
    get_transformation_summary,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SilverLayerPipeline:
    """
    Pipeline for transforming Bronze data into Silver layer.
    
    This pipeline:
    1. Reads raw data from the Bronze layer
    2. Applies cleaning and standardization transformations
    3. Writes partitioned Parquet files to Silver layer
    
    Attributes:
        bronze_dir: Path to Bronze layer data
        silver_dir: Path to Silver layer output
        partition_cols: Columns to use for partitioning
        
    Example:
        >>> pipeline = SilverLayerPipeline()
        >>> result = pipeline.run()
        >>> print(f"Transformed {result['record_count']} records")
    """
    
    def __init__(
        self,
        bronze_dir: str = "data/bronze/breweries",
        silver_dir: str = "data/silver/breweries",
        partition_cols: Optional[List[str]] = None
    ):
        """
        Initialize the Silver layer pipeline.
        
        Args:
            bronze_dir: Path to Bronze layer data
            silver_dir: Path to Silver layer output
            partition_cols: Columns for partitioning (default: country, state_province)
        """
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.partition_cols = partition_cols or ["country", "state_province"]
        
        self.reader = BronzeReader(base_dir=bronze_dir)
    
    def run(
        self,
        ingestion_date: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> dict:
        """
        Execute the Silver layer transformation pipeline.
        
        Args:
            ingestion_date: Specific date to process (optional, uses latest if not provided)
            run_id: Specific run to process (optional)
            
        Returns:
            Dictionary with transformation summary
        """
        logger.info("=" * 60)
        logger.info("Starting Silver Layer Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Step 1: Read Bronze data
            logger.info("Step 1: Reading Bronze layer data...")
            bronze_df = self._read_bronze_data(ingestion_date, run_id)
            logger.info(f"Read {len(bronze_df)} records from Bronze layer")
            
            # Step 2: Transform data
            logger.info("Step 2: Applying transformations...")
            silver_df = transform_bronze_to_silver(bronze_df)
            
            # Step 3: Write to Silver layer
            logger.info("Step 3: Writing to Silver layer...")
            self._write_silver_data(silver_df)
            
            # Step 4: Write success marker
            self._write_success_marker()
            
            # Generate summary
            summary = get_transformation_summary(bronze_df, silver_df)
            summary["status"] = "success"
            summary["silver_dir"] = str(self.silver_dir)
            summary["start_time"] = start_time.isoformat()
            summary["end_time"] = datetime.now(timezone.utc).isoformat()
            summary["partition_columns"] = self.partition_cols
            
            logger.info("=" * 60)
            logger.info("Silver Layer Pipeline Completed Successfully!")
            logger.info(f"Records transformed: {summary['silver_record_count']}")
            logger.info(f"Output directory: {self.silver_dir}")
            logger.info("=" * 60)
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def _read_bronze_data(
        self,
        ingestion_date: Optional[str],
        run_id: Optional[str]
    ) -> pd.DataFrame:
        """Read data from Bronze layer."""
        if ingestion_date and run_id:
            return self.reader.read_run_as_dataframe(ingestion_date, run_id)
        else:
            return self.reader.read_latest_run()
    
    def _write_silver_data(self, df: pd.DataFrame) -> None:
        """
        Write DataFrame to Silver layer as partitioned Parquet.
        
        Args:
            df: Transformed DataFrame to write
        """
        # Ensure output directory exists
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        # Write partitioned parquet
        df.to_parquet(
            self.silver_dir,
            engine="pyarrow",
            partition_cols=self.partition_cols,
            index=False,
            existing_data_behavior="delete_matching"
        )
        
        logger.info(f"Written partitioned Parquet to {self.silver_dir}")
        
        # Log partition info
        partition_counts = df.groupby(self.partition_cols).size()
        logger.info(f"Created {len(partition_counts)} partitions")
    
    def _write_success_marker(self) -> None:
        """Write a _SUCCESS marker file."""
        success_path = self.silver_dir / "_SUCCESS"
        success_path.write_text(
            f"Completed at {datetime.now(timezone.utc).isoformat()}\n"
        )
        logger.info("Written _SUCCESS marker")


def run_silver_pipeline(
    bronze_dir: str = "data/bronze/breweries",
    silver_dir: str = "data/silver/breweries"
) -> dict:
    """
    Convenience function to run the Silver layer pipeline.
    
    Args:
        bronze_dir: Path to Bronze layer
        silver_dir: Path to Silver layer output
        
    Returns:
        Pipeline execution summary
    """
    pipeline = SilverLayerPipeline(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir
    )
    return pipeline.run()


if __name__ == "__main__":
    try:
        result = run_silver_pipeline()
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Records: {result['silver_record_count']}")
        print(f"   Output: {result['silver_dir']}")
    except FileNotFoundError as e:
        print(f"\n⚠️  No Bronze data found. Run Bronze pipeline first.")
        print(f"   Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)
