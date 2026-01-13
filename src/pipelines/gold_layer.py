"""
Gold Layer Pipeline - Aggregated Analytics with DuckDB + Delta Lake.

This pipeline reads transformed data from the Silver layer (Delta Lake),
creates aggregated analytical views using DuckDB, and writes to Gold layer.

NO PANDAS DEPENDENCY - Pure DuckDB + PyArrow + Delta Lake implementation.

Features:
- DuckDB for high-performance SQL aggregations
- PyArrow for efficient data handling
- Delta Lake for reading Silver and writing Gold
- Multiple aggregation views

Usage:
    python -m src.pipelines.gold_layer
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.transforms.gold_transforms import (
    DuckDBAggregator,
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


class SilverDeltaReader:
    """Reader for Silver layer Delta Lake tables."""
    
    def __init__(self, silver_dir: str = "data/silver/breweries"):
        self.silver_dir = Path(silver_dir)
    
    def read_all(self) -> pa.Table:
        """Read all data from Silver Delta Lake table as PyArrow Table."""
        if not self.silver_dir.exists():
            raise FileNotFoundError(f"Silver layer not found: {self.silver_dir}")
        
        # Check if it's a Delta table
        delta_log = self.silver_dir / "_delta_log"
        if delta_log.exists():
            logger.info(f"Reading Silver Delta Lake table from: {self.silver_dir}")
            dt = DeltaTable(str(self.silver_dir))
            table = dt.to_pyarrow_table()
            logger.info(f"Read {table.num_rows} records from Silver layer (Delta)")
            return table
        else:
            # Fallback to Parquet if not Delta
            logger.info(f"Reading Silver Parquet from: {self.silver_dir}")
            import pyarrow.parquet as pq
            table = pq.read_table(self.silver_dir)
            logger.info(f"Read {table.num_rows} records from Silver layer (Parquet)")
            return table
    
    def is_ready(self) -> bool:
        """Check if Silver layer exists."""
        return self.silver_dir.exists()
    
    def get_version(self) -> Optional[int]:
        """Get current Delta table version."""
        try:
            dt = DeltaTable(str(self.silver_dir))
            return dt.version()
        except:
            return None


class GoldLayerPipeline:
    """
    Pipeline for creating Gold layer aggregations with Delta Lake.
    
    NO PANDAS - Uses DuckDB + PyArrow + Delta Lake.
    
    This pipeline:
    1. Reads transformed data from the Silver layer (Delta)
    2. Creates multiple aggregated views using DuckDB
    3. Writes Delta Lake tables to Gold layer
    
    Main output: breweries_by_type_and_location (Delta Lake)
    
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
            silver_dir: Path to Silver layer data (Delta Lake)
            gold_dir: Path to Gold layer output
        """
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.reader = SilverDeltaReader(silver_dir)
        self.aggregator = DuckDBAggregator()
    
    def run(self, mode: str = "overwrite") -> dict:
        """
        Execute the Gold layer aggregation pipeline.
        
        Args:
            mode: Write mode - "overwrite" or "append"
            
        Returns:
            Dictionary with aggregation summary
        """
        logger.info("=" * 60)
        logger.info("Starting Gold Layer Pipeline (DuckDB + PyArrow + Delta)")
        logger.info("=" * 60)
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Step 1: Read Silver data
            logger.info("Step 1: Reading Silver layer (Delta Lake)...")
            silver_table = self.reader.read_all()
            logger.info(f"Read {silver_table.num_rows} records from Silver layer")
            
            # Step 2: Create aggregations using DuckDB
            logger.info("Step 2: Creating aggregations with DuckDB...")
            
            # Main aggregation: by type and location
            main_agg = self.aggregator.aggregate_by_type_and_location(silver_table)
            
            # Additional aggregations
            by_type = self.aggregator.aggregate_by_type(silver_table)
            by_country = self.aggregator.aggregate_by_country(silver_table)
            
            # Step 3: Write to Gold layer (Delta Lake)
            logger.info("Step 3: Writing to Gold layer (Delta Lake)...")
            self._ensure_gold_dir()
            
            self._write_delta(main_agg, "breweries_by_type_and_location", mode)
            self._write_delta(by_type, "breweries_by_type", mode)
            self._write_delta(by_country, "breweries_by_country", mode)
            
            # Step 4: Write summary
            summary_data = self.aggregator.create_gold_summary(silver_table)
            self._write_summary(summary_data)
            
            # Get stats for return
            stats = self.aggregator.get_aggregation_stats(main_agg)
            stats["status"] = "success"
            stats["gold_dir"] = str(self.gold_dir)
            stats["format"] = "delta"
            stats["engine"] = "duckdb+pyarrow"
            stats["start_time"] = start_time.isoformat()
            stats["end_time"] = datetime.now(timezone.utc).isoformat()
            
            logger.info("=" * 60)
            logger.info("Gold Layer Pipeline Completed Successfully!")
            logger.info(f"Total aggregation rows: {stats['total_rows']}")
            logger.info(f"Total breweries: {stats['total_breweries']}")
            logger.info(f"Format: Delta Lake (DuckDB + PyArrow)")
            logger.info(f"Output directory: {self.gold_dir}")
            logger.info("=" * 60)
            
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.aggregator.close()
    
    def _ensure_gold_dir(self) -> None:
        """Create Gold layer directory if it doesn't exist."""
        self.gold_dir.mkdir(parents=True, exist_ok=True)
    
    def _write_delta(self, table: pa.Table, table_name: str, mode: str = "overwrite") -> Path:
        """Write a PyArrow Table as Delta Lake table in Gold layer."""
        output_path = self.gold_dir / table_name
        output_path.mkdir(parents=True, exist_ok=True)
        
        write_deltalake(
            str(output_path),
            table,
            mode=mode
        )
        
        logger.info(f"Written Delta table: {output_path}")
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
    
    def read_gold_table(self, table_name: str) -> pa.Table:
        """
        Read a Gold layer Delta table.
        
        Args:
            table_name: Name of the table to read
            
        Returns:
            PyArrow Table from the Delta table
        """
        table_path = self.gold_dir / table_name
        dt = DeltaTable(str(table_path))
        return dt.to_pyarrow_table()
    
    def get_table_history(self, table_name: str, limit: int = 10) -> list:
        """Get history of a Gold layer Delta table."""
        table_path = self.gold_dir / table_name
        dt = DeltaTable(str(table_path))
        return dt.history(limit)


def run_gold_pipeline(
    silver_dir: str = "data/silver/breweries",
    gold_dir: str = "data/gold/breweries",
    mode: str = "overwrite"
) -> dict:
    """
    Convenience function to run the Gold layer pipeline.
    
    Args:
        silver_dir: Path to Silver layer
        gold_dir: Path to Gold layer output
        mode: Write mode ("overwrite" or "append")
        
    Returns:
        Pipeline execution summary
    """
    pipeline = GoldLayerPipeline(
        silver_dir=silver_dir,
        gold_dir=gold_dir
    )
    return pipeline.run(mode=mode)


if __name__ == "__main__":
    try:
        result = run_gold_pipeline()
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Aggregation rows: {result['total_rows']}")
        print(f"   Total breweries: {result['total_breweries']}")
        print(f"   Format: Delta Lake (DuckDB + PyArrow)")
        print(f"   Output: {result['gold_dir']}")
    except FileNotFoundError as e:
        print(f"\n⚠️  No Silver data found. Run Silver pipeline first.")
        print(f"   Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)
