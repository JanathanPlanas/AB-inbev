"""
Gold Layer Pipeline - Aggregated Analytics.

This pipeline reads transformed data from the Silver layer and
creates aggregated analytical views in the Gold layer.

Usage:
    python -m src.pipelines.gold_layer
    
Output:
    data/gold/breweries/
        ├── breweries_by_type_and_location.parquet
        ├── breweries_by_type.parquet
        ├── breweries_by_country.parquet
        ├── _summary.json
        └── _SUCCESS
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.io.silver_reader import SilverReader
from src.transforms.gold_transforms import (
    aggregate_by_type_and_location,
    aggregate_by_type,
    aggregate_by_country,
    get_aggregation_stats,
    create_gold_summary,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GoldLayerPipeline:
    """
    Pipeline for creating Gold layer aggregations.
    
    This pipeline:
    1. Reads transformed data from the Silver layer
    2. Creates multiple aggregated views
    3. Writes Parquet files to Gold layer
    
    Main output: breweries_by_type_and_location.parquet
    - Aggregated view with quantity of breweries per type and location
    
    Example:
        >>> pipeline = GoldLayerPipeline()
        >>> result = pipeline.run()
        >>> print(f"Created {result['total_rows']} aggregation rows")
    """
    
    def __init__(
        self,
        silver_dir: str = "data/silver/breweries",
        gold_dir: str = "data/gold/breweries"
    ):
        """
        Initialize the Gold layer pipeline.
        
        Args:
            silver_dir: Path to Silver layer data
            gold_dir: Path to Gold layer output
        """
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.reader = SilverReader(base_dir=silver_dir)
    
    def run(self) -> dict:
        """
        Execute the Gold layer aggregation pipeline.
        
        Returns:
            Dictionary with aggregation summary
        """
        logger.info("=" * 60)
        logger.info("Starting Gold Layer Pipeline")
        logger.info("=" * 60)
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Step 1: Check Silver layer is ready
            if not self.reader.is_ready():
                logger.warning("Silver layer _SUCCESS marker not found. Proceeding anyway...")
            
            # Step 2: Read Silver data
            logger.info("Step 1: Reading Silver layer data...")
            silver_df = self.reader.read_all()
            logger.info(f"Read {len(silver_df)} records from Silver layer")
            
            # Step 3: Create aggregations
            logger.info("Step 2: Creating aggregations...")
            
            # Main aggregation: by type and location
            main_agg = aggregate_by_type_and_location(silver_df)
            
            # Additional aggregations
            by_type = aggregate_by_type(silver_df)
            by_country = aggregate_by_country(silver_df)
            
            # Step 4: Write to Gold layer
            logger.info("Step 3: Writing to Gold layer...")
            self._ensure_gold_dir()
            
            self._write_parquet(main_agg, "breweries_by_type_and_location.parquet")
            self._write_parquet(by_type, "breweries_by_type.parquet")
            self._write_parquet(by_country, "breweries_by_country.parquet")
            
            # Step 5: Write summary
            summary = create_gold_summary(silver_df)
            self._write_summary(summary)
            
            # Step 6: Write success marker
            self._write_success_marker()
            
            # Get stats for return
            stats = get_aggregation_stats(main_agg)
            stats["status"] = "success"
            stats["gold_dir"] = str(self.gold_dir)
            stats["start_time"] = start_time.isoformat()
            stats["end_time"] = datetime.now(timezone.utc).isoformat()
            
            logger.info("=" * 60)
            logger.info("Gold Layer Pipeline Completed Successfully!")
            logger.info(f"Total aggregation rows: {stats['total_rows']}")
            logger.info(f"Total breweries: {stats['total_breweries']}")
            logger.info(f"Output directory: {self.gold_dir}")
            logger.info("=" * 60)
            
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def _ensure_gold_dir(self) -> None:
        """Create Gold layer directory if it doesn't exist."""
        self.gold_dir.mkdir(parents=True, exist_ok=True)
    
    def _write_parquet(self, df: pd.DataFrame, filename: str) -> Path:
        """Write a DataFrame to Parquet in Gold layer."""
        output_path = self.gold_dir / filename
        df.to_parquet(output_path, engine="pyarrow", index=False)
        logger.info(f"Written: {output_path}")
        return output_path
    
    def _write_summary(self, summary: dict) -> Path:
        """Write summary JSON file."""
        output_path = self.gold_dir / "_summary.json"
        output_path.write_text(
            json.dumps(summary, indent=2, default=str),
            encoding="utf-8"
        )
        logger.info(f"Written summary: {output_path}")
        return output_path
    
    def _write_success_marker(self) -> None:
        """Write a _SUCCESS marker file."""
        success_path = self.gold_dir / "_SUCCESS"
        success_path.write_text(
            f"Completed at {datetime.now(timezone.utc).isoformat()}\n"
        )
        logger.info("Written _SUCCESS marker")


def run_gold_pipeline(
    silver_dir: str = "data/silver/breweries",
    gold_dir: str = "data/gold/breweries"
) -> dict:
    """
    Convenience function to run the Gold layer pipeline.
    
    Args:
        silver_dir: Path to Silver layer
        gold_dir: Path to Gold layer output
        
    Returns:
        Pipeline execution summary
    """
    pipeline = GoldLayerPipeline(
        silver_dir=silver_dir,
        gold_dir=gold_dir
    )
    return pipeline.run()


if __name__ == "__main__":
    try:
        result = run_gold_pipeline()
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Aggregation rows: {result['total_rows']}")
        print(f"   Total breweries: {result['total_breweries']}")
        print(f"   Output: {result['gold_dir']}")
    except FileNotFoundError as e:
        print(f"\n⚠️  No Silver data found. Run Silver pipeline first.")
        print(f"   Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)
