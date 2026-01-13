"""
Bronze Layer Reader.

This module handles reading raw data from the Bronze layer
(gzipped JSONL files) for transformation into Silver layer.

NO PANDAS DEPENDENCY - Returns list of dicts or PyArrow Table.
"""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


class BronzeReader:
    """
    Reader for Bronze layer data (JSONL.gz files).
    
    NO PANDAS - Returns list of dicts or PyArrow Table.
    """
    
    def __init__(self, base_dir: str | Path = "data/bronze/breweries"):
        self.base_dir = Path(base_dir)
    
    def get_available_runs(self) -> List[Dict[str, Any]]:
        """Get all available ingestion runs."""
        runs = []
        
        if not self.base_dir.exists():
            logger.warning(f"Bronze directory does not exist: {self.base_dir}")
            return runs
        
        for date_dir in self.base_dir.glob("ingestion_date=*"):
            ingestion_date = date_dir.name.split("=")[1]
            
            for run_dir in date_dir.glob("run_id=*"):
                run_id = run_dir.name.split("=")[1]
                page_files = list(run_dir.glob("page=*.jsonl.gz"))
                manifest_path = run_dir / "_manifest.json"
                
                runs.append({
                    "ingestion_date": ingestion_date,
                    "run_id": run_id,
                    "path": run_dir,
                    "page_count": len(page_files),
                    "has_manifest": manifest_path.exists()
                })
        
        runs.sort(key=lambda x: (x["ingestion_date"], x["run_id"]), reverse=True)
        return runs
    
    def get_latest_run_path(self) -> Optional[Path]:
        """Get the path to the most recent run."""
        runs = self.get_available_runs()
        if not runs:
            return None
        return runs[0]["path"]
    
    def read_jsonl_gz_file(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Read records from a single JSONL.gz file."""
        logger.debug(f"Reading file: {file_path}")
        
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
    
    def read_run_directory(self, run_dir: Path) -> List[Dict[str, Any]]:
        """Read all records from a run directory."""
        all_records = []
        
        page_files = sorted(run_dir.glob("page=*.jsonl.gz"))
        logger.info(f"Found {len(page_files)} page files in {run_dir}")
        
        for page_file in page_files:
            records = list(self.read_jsonl_gz_file(page_file))
            all_records.extend(records)
        
        logger.info(f"Total records read: {len(all_records)}")
        return all_records
    
    def read_latest_run_as_list(self) -> List[Dict[str, Any]]:
        """Read the latest run as a list of dictionaries."""
        latest_path = self.get_latest_run_path()
        
        if latest_path is None:
            raise FileNotFoundError(f"No bronze data found in {self.base_dir}")
        
        logger.info(f"Reading latest run from: {latest_path}")
        return self.read_run_directory(latest_path)
    
    def read_latest_run_as_arrow(self) -> pa.Table:
        """Read the latest run as a PyArrow Table."""
        records = self.read_latest_run_as_list()
        return pa.Table.from_pylist(records)
    
    def read_run_as_list(self, ingestion_date: str, run_id: str) -> List[Dict[str, Any]]:
        """Read a specific run as a list of dictionaries."""
        run_dir = self.base_dir / f"ingestion_date={ingestion_date}" / f"run_id={run_id}"
        
        if not run_dir.exists():
            raise FileNotFoundError(f"Run not found: {run_dir}")
        
        return self.read_run_directory(run_dir)
    
    def read_run_as_arrow(self, ingestion_date: str, run_id: str) -> pa.Table:
        """Read a specific run as a PyArrow Table."""
        records = self.read_run_as_list(ingestion_date, run_id)
        return pa.Table.from_pylist(records)
    
    def read_manifest(self, run_dir: Path) -> Optional[Dict[str, Any]]:
        """Read the manifest file from a run directory."""
        manifest_path = run_dir / "_manifest.json"
        
        if not manifest_path.exists():
            return None
        
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)