"""
Silver Layer Pipeline - Data Transformation with DuckDB + Delta Lake.

This pipeline reads raw data from the Bronze layer, applies
transformations using DuckDB, and writes Delta Lake format to Silver.

NO PANDAS DEPENDENCY - Pure DuckDB + PyArrow + Delta Lake implementation.

Features:
- DuckDB for high-performance SQL transformations
- PyArrow for efficient data handling
- Delta Lake for ACID transactions and time travel
- Partitioning by country and state_province

Usage:
    python -m src.pipelines.silver_layer
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.io.bronze_reader import BronzeReader
from src.transforms.silver_transforms import (
    DuckDBTransformer,
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
    Pipeline for transforming Bronze data into Silver layer with Delta Lake.
    
    NO PANDAS - Uses DuckDB + PyArrow + Delta Lake.
    
    This pipeline:
    1. Reads raw data from the Bronze layer
    2. Applies cleaning and standardization using DuckDB
    3. Writes Delta Lake format partitioned by location
    
    Features:
    - ACID transactions
    - Schema enforcement
    - Time travel capability
    
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
            silver_dir: Path to Silver layer output (Delta Lake)
            partition_cols: Columns for partitioning (default: country, state_province)
        """
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.partition_cols = partition_cols or ["country", "state_province"]
        
        self.reader = BronzeReader(base_dir=bronze_dir)
        self.transformer = DuckDBTransformer()
    
    def run(
        self,
        ingestion_date: Optional[str] = None,
        run_id: Optional[str] = None,
        mode: str = "overwrite"
    ) -> dict:
        """
        Execute the Silver layer transformation pipeline.
        
        Args:
            ingestion_date: Specific date to process (optional)
            run_id: Specific run to process (optional)
            mode: Write mode - "overwrite" or "append"
            
        Returns:
            Dictionary with transformation summary
        """
        logger.info("=" * 60)
        logger.info("Starting Silver Layer Pipeline (DuckDB + PyArrow + Delta)")
        logger.info("=" * 60)
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Step 1: Read Bronze data as PyArrow Table
            logger.info("Step 1: Reading Bronze layer data...")
            bronze_table = self._read_bronze_data(ingestion_date, run_id)
            logger.info(f"Read {bronze_table.num_rows} records from Bronze layer")
            
            # Step 2: Transform data using DuckDB
            logger.info("Step 2: Transforming with DuckDB...")
            silver_table = self.transformer.transform_bronze_to_silver(bronze_table)
            
            # Step 3: Write to Delta Lake
            logger.info("Step 3: Writing to Delta Lake...")
            self._write_delta_lake(silver_table, mode)
            
            # Generate summary
            summary = self.transformer.get_transformation_summary(bronze_table, silver_table)
            summary["status"] = "success"
            summary["silver_dir"] = str(self.silver_dir)
            summary["format"] = "delta"
            summary["engine"] = "duckdb+pyarrow"
            summary["start_time"] = start_time.isoformat()
            summary["end_time"] = datetime.now(timezone.utc).isoformat()
            summary["partition_columns"] = self.partition_cols
            
            # Get Delta Lake info
            summary["delta_info"] = self._get_delta_info()
            
            logger.info("=" * 60)
            logger.info("Silver Layer Pipeline Completed Successfully!")
            logger.info(f"Records transformed: {summary['silver_record_count']}")
            logger.info(f"Format: Delta Lake (DuckDB + PyArrow)")
            logger.info(f"Output directory: {self.silver_dir}")
            logger.info("=" * 60)
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.transformer.close()
    
    def _read_bronze_data(
        self,
        ingestion_date: Optional[str],
        run_id: Optional[str]
    ) -> pa.Table:
        """Read data from Bronze layer as PyArrow Table."""
        if ingestion_date and run_id:
            data = self.reader.read_run_as_list(ingestion_date, run_id)
        else:
            data = self.reader.read_latest_run_as_list()
        
        # Convert list of dicts to PyArrow Table
        return pa.Table.from_pylist(data)
    
    def _write_delta_lake(self, table: pa.Table, mode: str = "overwrite") -> None:
        """
        Write PyArrow Table to Silver layer as Delta Lake.
        
        Args:
            table: Transformed PyArrow Table to write
            mode: "overwrite" or "append"
        """
        # Ensure output directory exists
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing Delta Lake table with mode='{mode}'")
        logger.info(f"Partitioning by: {self.partition_cols}")
        
        # Write as Delta Lake (accepts PyArrow Table directly)
        write_deltalake(
            str(self.silver_dir),
            table,
            mode=mode,
            partition_by=self.partition_cols
        )
        
        logger.info(f"Written Delta Lake table to {self.silver_dir}")
        
        # Log partition info using DuckDB
        conn = duckdb.connect(":memory:")
        conn.register("t", table)
        partition_count = conn.execute(f"""
            SELECT COUNT(DISTINCT ({', '.join(self.partition_cols)}))
            FROM t
        """).fetchone()[0]
        conn.close()
        
        logger.info(f"Created {partition_count} unique partitions")
    
    def _get_delta_info(self) -> dict:
        """Get Delta Lake table information."""
        try:
            dt = DeltaTable(str(self.silver_dir))
            return {
                "version": dt.version(),
                "num_rows": dt.to_pyarrow_table().num_rows  # 

            }
        except Exception as e:
            logger.warning(f"Could not get Delta info: {e}")
            return {}
    
    def get_history(self, limit: int = 10) -> list:
        """Get Delta Lake table history (time travel)."""
        try:
            dt = DeltaTable(str(self.silver_dir))
            return dt.history(limit)
        except Exception as e:
            logger.error(f"Could not get history: {e}")
            return []
    
    def read_version(self, version: int) -> pa.Table:
        """Read a specific version of the Delta table (time travel)."""
        dt = DeltaTable(str(self.silver_dir), version=version)
        return dt.to_pyarrow_table()


def run_silver_pipeline(
    bronze_dir: str = "data/bronze/breweries",
    silver_dir: str = "data/silver/breweries",
    mode: str = "overwrite"
) -> dict:
    """
    Convenience function to run the Silver layer pipeline.
    
    Args:
        bronze_dir: Path to Bronze layer
        silver_dir: Path to Silver layer output
        mode: Write mode ("overwrite" or "append")
        
    Returns:
        Pipeline execution summary
    """
    pipeline = SilverLayerPipeline(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir
    )
    return pipeline.run(mode=mode)


if __name__ == "__main__":
    try:
        result = run_silver_pipeline()
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Records: {result['silver_record_count']}")
        print(f"   Format: Delta Lake (DuckDB + PyArrow)")
        print(f"   Output: {result['silver_dir']}")
        if result.get('delta_info'):
            print(f"   Version: {result['delta_info'].get('version', 'N/A')}")
    except FileNotFoundError as e:
        print(f"\n⚠️  No Bronze data found. Run Bronze pipeline first.")
        print(f"   Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)
