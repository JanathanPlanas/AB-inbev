"""
Raw Data Writer for Bronze Layer.

This module handles writing raw API responses to the bronze layer
in JSONL (gzipped) format with proper partitioning.
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class RawJsonlGzWriter:
    """
    Writer for raw JSON data in gzipped JSONL format.
    
    Organizes data in a partition structure:
        base_dir/ingestion_date=YYYY-MM-DD/run_id=YYYYMMDD_HHMMSS/
    
    Attributes:
        base_dir: Base directory for raw data storage
        ingestion_date: Date of ingestion (YYYY-MM-DD format)
        run_id: Unique identifier for this run
        
    Example:
        >>> writer = RawJsonlGzWriter(base_dir="data/bronze/breweries")
        >>> writer.write_page(1, [{"id": "1", "name": "Brewery A"}])
        >>> writer.write_manifest({"total_records": 100, "pages": 5})
    """
    
    base_dir: Path | str = "data/bronze/breweries"
    ingestion_date: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    
    # Track written pages for manifest
    _written_pages: List[Dict[str, Any]] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        """Ensure base_dir is a Path object."""
        self.base_dir = Path(self.base_dir)
        self._written_pages = []

    @property
    def run_dir(self) -> Path:
        """Get the directory for this run, creating it if necessary."""
        out_dir = self.base_dir / f"ingestion_date={self.ingestion_date}" / f"run_id={self.run_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def write_page(
        self, 
        page: int, 
        records: Iterable[Dict[str, Any]],
        add_metadata: bool = True
    ) -> Path:
        """
        Write a page of records to a gzipped JSONL file.
        
        Args:
            page: Page number (used in filename)
            records: Iterable of dictionaries to write
            add_metadata: If True, adds ingestion metadata to each record
            
        Returns:
            Path to the written file
        """
        out_path = self.run_dir / f"page={page:04d}.jsonl.gz"
        record_count = 0
        
        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            for rec in records:
                if add_metadata:
                    rec = self._add_metadata(rec)
                f.write(json.dumps(rec, ensure_ascii=False))
                f.write("\n")
                record_count += 1
        
        # Track for manifest
        self._written_pages.append({
            "page": page,
            "file": out_path.name,
            "record_count": record_count
        })
        
        logger.info(f"Written page {page} with {record_count} records to {out_path}")
        return out_path

    def _add_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add ingestion metadata to a record."""
        return {
            **record,
            "_ingestion_date": self.ingestion_date,
            "_run_id": self.run_id,
            "_ingested_at": datetime.now(timezone.utc).isoformat()
        }

    def write_manifest(self, extra_metadata: Optional[Dict[str, Any]] = None) -> Path:
        """
        Write a manifest file with ingestion metadata.
        
        Args:
            extra_metadata: Additional metadata to include in manifest
            
        Returns:
            Path to the manifest file
        """
        manifest = {
            "ingestion_date": self.ingestion_date,
            "run_id": self.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "pages": self._written_pages,
            "total_pages": len(self._written_pages),
            "total_records": sum(p["record_count"] for p in self._written_pages),
            **(extra_metadata or {})
        }
        
        out_path = self.run_dir / "_manifest.json"
        out_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), 
            encoding="utf-8"
        )
        
        logger.info(f"Written manifest to {out_path}")
        return out_path

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of written data."""
        return {
            "run_dir": str(self.run_dir),
            "total_pages": len(self._written_pages),
            "total_records": sum(p["record_count"] for p in self._written_pages)
        }