"""
Bronze Layer Reader.

This module handles reading raw data from the Bronze layer
(gzipped JSONL files) for transformation into Silver layer.
"""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BronzeReader:
    """
    Reader for Bronze layer data (JSONL.gz files).
    
    Supports reading individual files, directories, or the latest run.
    
    Example:
        >>> reader = BronzeReader(base_dir="data/bronze/breweries")
        >>> df = reader.read_latest_run()
        >>> print(f"Read {len(df)} records")
    """
    
    def __init__(self, base_dir: str | Path = "data/bronze/breweries"):
        """
        Initialize the Bronze reader.
        
        Args:
            base_dir: Base directory for bronze layer data
        """
        self.base_dir = Path(base_dir)
    
    def get_available_runs(self) -> List[Dict[str, Any]]:
        """
        Get all available ingestion runs.
        
        Returns:
            List of dicts with run information, sorted by date (newest first)
        """
        runs = []
        
        if not self.base_dir.exists():
            logger.warning(f"Bronze directory does not exist: {self.base_dir}")
            return runs
        
        for date_dir in self.base_dir.glob("ingestion_date=*"):
            ingestion_date = date_dir.name.split("=")[1]
            
            for run_dir in date_dir.glob("run_id=*"):
                run_id = run_dir.name.split("=")[1]
                
                # Count files
                page_files = list(run_dir.glob("page=*.jsonl.gz"))
                manifest_path = run_dir / "_manifest.json"
                
                runs.append({
                    "ingestion_date": ingestion_date,
                    "run_id": run_id,
                    "path": run_dir,
                    "page_count": len(page_files),
                    "has_manifest": manifest_path.exists()
                })
        
        # Sort by date and run_id (newest first)
        runs.sort(key=lambda x: (x["ingestion_date"], x["run_id"]), reverse=True)
        return runs
    
    def get_latest_run_path(self) -> Optional[Path]:
        """
        Get the path to the most recent run.
        
        Returns:
            Path to the latest run directory, or None if no runs exist
        """
        runs = self.get_available_runs()
        if not runs:
            return None
        return runs[0]["path"]
    
    def read_jsonl_gz_file(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """
        Read records from a single JSONL.gz file.
        
        Args:
            file_path: Path to the .jsonl.gz file
            
        Yields:
            Dictionary records from the file
        """
        logger.debug(f"Reading file: {file_path}")
        
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
    
    def read_run_directory(self, run_dir: Path) -> List[Dict[str, Any]]:
        """
        Read all records from a run directory.
        
        Args:
            run_dir: Path to the run directory
            
        Returns:
            List of all records from all page files
        """
        all_records = []
        
        page_files = sorted(run_dir.glob("page=*.jsonl.gz"))
        logger.info(f"Found {len(page_files)} page files in {run_dir}")
        
        for page_file in page_files:
            records = list(self.read_jsonl_gz_file(page_file))
            all_records.extend(records)
            logger.debug(f"Read {len(records)} records from {page_file.name}")
        
        logger.info(f"Total records read: {len(all_records)}")
        return all_records
    
    def read_latest_run(self) -> pd.DataFrame:
        """
        Read the latest run as a pandas DataFrame.
        
        Returns:
            DataFrame with all records from the latest run
            
        Raises:
            FileNotFoundError: If no bronze data exists
        """
        latest_path = self.get_latest_run_path()
        
        if latest_path is None:
            raise FileNotFoundError(f"No bronze data found in {self.base_dir}")
        
        logger.info(f"Reading latest run from: {latest_path}")
        records = self.read_run_directory(latest_path)
        
        return pd.DataFrame(records)
    
    def read_run_as_dataframe(
        self, 
        ingestion_date: str, 
        run_id: str
    ) -> pd.DataFrame:
        """
        Read a specific run as a pandas DataFrame.
        
        Args:
            ingestion_date: Date in YYYY-MM-DD format
            run_id: Run identifier in YYYYMMDD_HHMMSS format
            
        Returns:
            DataFrame with all records from the specified run
        """
        run_dir = self.base_dir / f"ingestion_date={ingestion_date}" / f"run_id={run_id}"
        
        if not run_dir.exists():
            raise FileNotFoundError(f"Run not found: {run_dir}")
        
        records = self.read_run_directory(run_dir)
        return pd.DataFrame(records)
    
    def read_manifest(self, run_dir: Path) -> Optional[Dict[str, Any]]:
        """
        Read the manifest file from a run directory.
        
        Args:
            run_dir: Path to the run directory
            
        Returns:
            Manifest dictionary, or None if not found
        """
        manifest_path = run_dir / "_manifest.json"
        
        if not manifest_path.exists():
            return None
        
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)