"""
Unit tests for RawJsonlGzWriter.
"""

import gzip
import json
import pytest
from pathlib import Path
from unittest.mock import patch

from src.io.raw_writer import RawJsonlGzWriter


class TestRawJsonlGzWriter:
    """Test suite for RawJsonlGzWriter."""
    
    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for tests."""
        return tmp_path / "bronze"
    
    @pytest.fixture
    def writer(self, temp_dir):
        """Create a writer instance with fixed dates for testing."""
        return RawJsonlGzWriter(
            base_dir=temp_dir,
            ingestion_date="2025-01-08",
            run_id="20250108_120000"
        )
    
    @pytest.fixture
    def sample_records(self):
        """Sample brewery records for testing."""
        return [
            {"id": "1", "name": "Brewery A", "city": "Portland"},
            {"id": "2", "name": "Brewery B", "city": "Seattle"},
            {"id": "3", "name": "Brewery C", "city": "Denver"}
        ]
    
    def test_init_with_string_path(self, temp_dir):
        """Test that string paths are converted to Path objects."""
        writer = RawJsonlGzWriter(base_dir=str(temp_dir))
        assert isinstance(writer.base_dir, Path)
    
    def test_init_with_path_object(self, temp_dir):
        """Test initialization with Path object."""
        writer = RawJsonlGzWriter(base_dir=temp_dir)
        assert writer.base_dir == temp_dir
    
    def test_run_dir_creates_directory(self, writer):
        """Test that run_dir creates the directory structure."""
        run_dir = writer.run_dir
        
        assert run_dir.exists()
        assert "ingestion_date=2025-01-08" in str(run_dir)
        assert "run_id=20250108_120000" in str(run_dir)
    
    def test_write_page_creates_gzipped_file(self, writer, sample_records):
        """Test that write_page creates a gzipped JSONL file."""
        output_path = writer.write_page(page=1, records=sample_records)
        
        assert output_path.exists()
        assert output_path.name == "page=0001.jsonl.gz"
        assert output_path.suffix == ".gz"
    
    def test_write_page_content_is_valid_jsonl(self, writer, sample_records):
        """Test that written content is valid JSONL."""
        output_path = writer.write_page(page=1, records=sample_records, add_metadata=False)
        
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            lines = f.readlines()
        
        assert len(lines) == 3
        
        for line in lines:
            record = json.loads(line.strip())
            assert "id" in record
            assert "name" in record
    
    def test_write_page_adds_metadata(self, writer, sample_records):
        """Test that ingestion metadata is added to records."""
        output_path = writer.write_page(page=1, records=sample_records, add_metadata=True)
        
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            first_line = f.readline()
        
        record = json.loads(first_line)
        
        assert "_ingestion_date" in record
        assert "_run_id" in record
        assert "_ingested_at" in record
        assert record["_ingestion_date"] == "2025-01-08"
        assert record["_run_id"] == "20250108_120000"
    
    def test_write_page_without_metadata(self, writer, sample_records):
        """Test writing without adding metadata."""
        output_path = writer.write_page(page=1, records=sample_records, add_metadata=False)
        
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            first_line = f.readline()
        
        record = json.loads(first_line)
        
        assert "_ingestion_date" not in record
        assert "_run_id" not in record
    
    def test_write_page_tracks_written_pages(self, writer, sample_records):
        """Test that written pages are tracked internally."""
        writer.write_page(page=1, records=sample_records)
        writer.write_page(page=2, records=sample_records[:2])
        
        assert len(writer._written_pages) == 2
        assert writer._written_pages[0]["page"] == 1
        assert writer._written_pages[0]["record_count"] == 3
        assert writer._written_pages[1]["page"] == 2
        assert writer._written_pages[1]["record_count"] == 2
    
    def test_write_page_numbering_format(self, writer, sample_records):
        """Test that page numbers are zero-padded."""
        writer.write_page(page=1, records=sample_records)
        writer.write_page(page=99, records=sample_records)
        writer.write_page(page=100, records=sample_records)
        
        files = list(writer.run_dir.glob("page=*.jsonl.gz"))
        file_names = [f.name for f in files]
        
        assert "page=0001.jsonl.gz" in file_names
        assert "page=0099.jsonl.gz" in file_names
        assert "page=0100.jsonl.gz" in file_names
    
    def test_write_manifest_creates_file(self, writer, sample_records):
        """Test that manifest file is created."""
        writer.write_page(page=1, records=sample_records)
        manifest_path = writer.write_manifest()
        
        assert manifest_path.exists()
        assert manifest_path.name == "_manifest.json"
    
    def test_write_manifest_content(self, writer, sample_records):
        """Test manifest content structure."""
        writer.write_page(page=1, records=sample_records)
        writer.write_page(page=2, records=sample_records[:1])
        manifest_path = writer.write_manifest()
        
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        
        assert manifest["ingestion_date"] == "2025-01-08"
        assert manifest["run_id"] == "20250108_120000"
        assert manifest["total_pages"] == 2
        assert manifest["total_records"] == 4  # 3 + 1
        assert "created_at" in manifest
        assert len(manifest["pages"]) == 2
    
    def test_write_manifest_with_extra_metadata(self, writer, sample_records):
        """Test that extra metadata is included in manifest."""
        writer.write_page(page=1, records=sample_records)
        
        extra = {"source": "test_api", "version": "1.0"}
        manifest_path = writer.write_manifest(extra_metadata=extra)
        
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        
        assert manifest["source"] == "test_api"
        assert manifest["version"] == "1.0"
    
    def test_get_summary(self, writer, sample_records):
        """Test get_summary returns correct information."""
        writer.write_page(page=1, records=sample_records)
        writer.write_page(page=2, records=sample_records[:2])
        
        summary = writer.get_summary()
        
        assert summary["total_pages"] == 2
        assert summary["total_records"] == 5  # 3 + 2
        assert "run_dir" in summary
    
    def test_empty_records(self, writer):
        """Test writing empty records."""
        output_path = writer.write_page(page=1, records=[])
        
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            content = f.read()
        
        assert content == ""
        assert writer._written_pages[0]["record_count"] == 0
    
    def test_unicode_content(self, writer):
        """Test handling of unicode content."""
        records = [
            {"id": "1", "name": "Cervejaria São Paulo", "city": "São Paulo"},
            {"id": "2", "name": "ブルワリー東京", "city": "Tokyo"}
        ]
        
        output_path = writer.write_page(page=1, records=records, add_metadata=False)
        
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            lines = f.readlines()
        
        record1 = json.loads(lines[0])
        record2 = json.loads(lines[1])
        
        assert record1["name"] == "Cervejaria São Paulo"
        assert record2["name"] == "ブルワリー東京"


class TestRawJsonlGzWriterDefaults:
    """Test default values for RawJsonlGzWriter."""
    
    def test_default_ingestion_date_format(self, tmp_path):
        """Test that default ingestion_date is in correct format."""
        writer = RawJsonlGzWriter(base_dir=tmp_path)
        
        # Should be YYYY-MM-DD format
        parts = writer.ingestion_date.split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # Year
        assert len(parts[1]) == 2  # Month
        assert len(parts[2]) == 2  # Day
    
    def test_default_run_id_format(self, tmp_path):
        """Test that default run_id is in correct format."""
        writer = RawJsonlGzWriter(base_dir=tmp_path)
        
        # Should be YYYYMMDD_HHMMSS format
        parts = writer.run_id.split("_")
        assert len(parts) == 2
        assert len(parts[0]) == 8  # YYYYMMDD
        assert len(parts[1]) == 6  # HHMMSS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])