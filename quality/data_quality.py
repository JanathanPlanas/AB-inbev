"""
Data Quality Validation Module.

Provides validation functions for each layer of the Medallion Architecture.
Used by both the DAG and standalone validation scripts.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import duckdb
import pyarrow as pa
from deltalake import DeltaTable

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""
    name: str
    passed: bool
    message: str
    details: Optional[dict] = None


@dataclass
class LayerValidationReport:
    """Complete validation report for a layer."""
    layer: str
    passed: bool
    checks: List[ValidationResult]
    record_count: int
    
    def to_dict(self) -> dict:
        return {
            "layer": self.layer,
            "passed": self.passed,
            "record_count": self.record_count,
            "checks": [
                {"name": c.name, "passed": c.passed, "message": c.message}
                for c in self.checks
            ],
            "failed_checks": [c.name for c in self.checks if not c.passed],
        }


class DataQualityValidator:
    """
    Data quality validator for Medallion Architecture layers.
    
    Example:
        >>> validator = DataQualityValidator()
        >>> report = validator.validate_bronze("data/bronze/breweries")
        >>> if not report.passed:
        ...     raise Exception(f"Validation failed: {report.to_dict()}")
    """
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
    
    def validate_bronze(self, bronze_path: str) -> LayerValidationReport:
        """
        Validate Bronze layer data quality.
        
        Checks:
        - Directory exists
        - Has at least one run
        - Manifest exists and is valid
        - Has records
        """
        checks = []
        path = Path(bronze_path)
        record_count = 0
        
        # Check 1: Directory exists
        checks.append(ValidationResult(
            name="directory_exists",
            passed=path.exists(),
            message=f"Bronze directory {'exists' if path.exists() else 'not found'}: {path}"
        ))
        
        if not path.exists():
            return LayerValidationReport("bronze", False, checks, 0)
        
        # Check 2: Has runs
        runs = sorted(path.glob("ingestion_date=*/run_id=*"), reverse=True)
        checks.append(ValidationResult(
            name="has_runs",
            passed=len(runs) > 0,
            message=f"Found {len(runs)} run(s)"
        ))
        
        if not runs:
            return LayerValidationReport("bronze", False, checks, 0)
        
        latest_run = runs[0]
        
        # Check 3: Manifest exists
        manifest_path = latest_run / "_manifest.json"
        checks.append(ValidationResult(
            name="manifest_exists",
            passed=manifest_path.exists(),
            message=f"Manifest {'found' if manifest_path.exists() else 'missing'}"
        ))
        
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
            
            record_count = manifest.get("total_records", 0)
            
            # Check 4: Has records
            checks.append(ValidationResult(
                name="has_records",
                passed=record_count > 0,
                message=f"Total records: {record_count}"
            ))
            
            
            # Check 5: Ingestion completed successfully (derived status)
            expected_total = manifest.get("expected_total", record_count)
            end_time = manifest.get("end_time")

            ingestion_completed = (
                record_count > 0
                and record_count == expected_total
                and end_time is not None
            )

            checks.append(ValidationResult(
                name="ingestion_completed",
                passed=ingestion_completed,
                message=(
                    f"Records: {record_count}, "
                    f"Expected: {expected_total}, "
                    f"End time present: {end_time is not None}"
                )
            ))

        
        passed = all(c.passed for c in checks)
        return LayerValidationReport("bronze", passed, checks, record_count)
    
    def validate_silver(
        self, 
        silver_path: str,
        expected_min_records: int = 0
    ) -> LayerValidationReport:
        """
        Validate Silver layer data quality.
        
        Checks:
        - Delta table exists
        - Has records
        - Required columns present
        - No null IDs
        - Coordinates in valid range
        """
        checks = []
        path = Path(silver_path)
        record_count = 0
        
        # Check 1: Path exists
        checks.append(ValidationResult(
            name="directory_exists",
            passed=path.exists(),
            message=f"Silver directory {'exists' if path.exists() else 'not found'}"
        ))
        
        if not path.exists():
            return LayerValidationReport("silver", False, checks, 0)
        
        # Check 2: Delta table exists
        delta_log = path / "_delta_log"
        checks.append(ValidationResult(
            name="delta_table_exists",
            passed=delta_log.exists(),
            message=f"Delta log {'found' if delta_log.exists() else 'missing'}"
        ))
        
        if not delta_log.exists():
            return LayerValidationReport("silver", False, checks, 0)
        
        # Load table
        dt = DeltaTable(str(path))
        table = dt.to_pyarrow_table()
        record_count = table.num_rows
        
        # Check 3: Has records
        checks.append(ValidationResult(
            name="has_records",
            passed=record_count > 0,
            message=f"Total records: {record_count}"
        ))
        
        # Check 4: Minimum records (if expected)
        if expected_min_records > 0:
            min_threshold = expected_min_records * 0.9  # 10% tolerance
            checks.append(ValidationResult(
                name="minimum_records",
                passed=record_count >= min_threshold,
                message=f"Expected >= {min_threshold}, got {record_count}"
            ))
        
        # Check 5: Required columns
        required_cols = ["id", "name", "brewery_type", "country", "state_province"]
        missing_cols = [c for c in required_cols if c not in table.column_names]
        checks.append(ValidationResult(
            name="required_columns",
            passed=len(missing_cols) == 0,
            message=f"Missing columns: {missing_cols}" if missing_cols else "All required columns present"
        ))
        
        if record_count > 0:
            self.conn.register("silver", table)
            
            # Check 6: No null IDs
            null_ids = self.conn.execute(
                "SELECT COUNT(*) FROM silver WHERE id IS NULL"
            ).fetchone()[0]
            checks.append(ValidationResult(
                name="no_null_ids",
                passed=null_ids == 0,
                message=f"Null IDs: {null_ids}"
            ))
            
            # Check 7: Valid coordinates
            invalid_coords = self.conn.execute("""
                SELECT COUNT(*) FROM silver 
                WHERE (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
                   OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))
            """).fetchone()[0]
            checks.append(ValidationResult(
                name="valid_coordinates",
                passed=invalid_coords == 0,
                message=f"Invalid coordinates: {invalid_coords}"
            ))
            
            # Check 8: No duplicate IDs
            duplicate_ids = self.conn.execute("""
                SELECT COUNT(*) - COUNT(DISTINCT id) as duplicates FROM silver
            """).fetchone()[0]
            checks.append(ValidationResult(
                name="no_duplicate_ids",
                passed=duplicate_ids == 0,
                message=f"Duplicate IDs: {duplicate_ids}"
            ))
        
        passed = all(c.passed for c in checks)
        return LayerValidationReport("silver", passed, checks, record_count)
    
    def validate_gold(
        self,
        gold_path: str,
        expected_total: int = 0
    ) -> LayerValidationReport:
        """
        Validate Gold layer data quality.
        
        Checks:
        - Main aggregation table exists
        - Has aggregation rows
        - Sum matches expected total
        - Summary file exists
        """
        checks = []
        path = Path(gold_path)
        record_count = 0
        
        # Check 1: Path exists
        checks.append(ValidationResult(
            name="directory_exists",
            passed=path.exists(),
            message=f"Gold directory {'exists' if path.exists() else 'not found'}"
        ))
        
        if not path.exists():
            return LayerValidationReport("gold", False, checks, 0)
        
        # Check 2: Main table exists
        main_table_path = path / "breweries_by_type_and_location"
        checks.append(ValidationResult(
            name="main_table_exists",
            passed=main_table_path.exists(),
            message=f"Main table {'found' if main_table_path.exists() else 'missing'}"
        ))
        
        if not main_table_path.exists():
            return LayerValidationReport("gold", False, checks, 0)
        
        # Load table
        dt = DeltaTable(str(main_table_path))
        table = dt.to_pyarrow_table()
        record_count = table.num_rows
        
        # Check 3: Has aggregations
        checks.append(ValidationResult(
            name="has_aggregations",
            passed=record_count > 0,
            message=f"Aggregation rows: {record_count}"
        ))
        
        if record_count > 0:
            self.conn.register("gold", table)
            
            # Check 4: Sum validation
            total_breweries = self.conn.execute(
                "SELECT SUM(brewery_count) FROM gold"
            ).fetchone()[0]
            
            if expected_total > 0:
                tolerance = 5  # Allow small difference
                checks.append(ValidationResult(
                    name="total_matches",
                    passed=abs(total_breweries - expected_total) <= tolerance,
                    message=f"Expected ~{expected_total}, got {total_breweries}"
                ))
            
            # Check 5: No zero counts
            zero_counts = self.conn.execute(
                "SELECT COUNT(*) FROM gold WHERE brewery_count <= 0"
            ).fetchone()[0]
            checks.append(ValidationResult(
                name="no_zero_counts",
                passed=zero_counts == 0,
                message=f"Zero/negative counts: {zero_counts}"
            ))
        
        # Check 6: Summary exists
        summary_path = path / "_summary.json"
        checks.append(ValidationResult(
            name="summary_exists",
            passed=summary_path.exists(),
            message=f"Summary {'found' if summary_path.exists() else 'missing'}"
        ))
        
        passed = all(c.passed for c in checks)
        return LayerValidationReport("gold", passed, checks, record_count)
    
    def validate_all(
        self,
        bronze_path: str = "data/bronze/breweries",
        silver_path: str = "data/silver/breweries",
        gold_path: str = "data/gold/breweries"
    ) -> dict:
        """
        Validate all layers and return comprehensive report.
        """
        bronze_report = self.validate_bronze(bronze_path)
        silver_report = self.validate_silver(silver_path, bronze_report.record_count)
        gold_report = self.validate_gold(gold_path, silver_report.record_count)
        
        all_passed = bronze_report.passed and silver_report.passed and gold_report.passed
        
        return {
            "passed": all_passed,
            "bronze": bronze_report.to_dict(),
            "silver": silver_report.to_dict(),
            "gold": gold_report.to_dict(),
        }
    
    def close(self):
        self.conn.close()


# Convenience function
def validate_pipeline(
    bronze_path: str = "data/bronze/breweries",
    silver_path: str = "data/silver/breweries",
    gold_path: str = "data/gold/breweries"
) -> dict:
    """Run full pipeline validation."""
    validator = DataQualityValidator()
    try:
        return validator.validate_all(bronze_path, silver_path, gold_path)
    finally:
        validator.close()


if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print("=" * 60)
    
    result = validate_pipeline()
    
    for layer in ["bronze", "silver", "gold"]:
        report = result[layer]
        status = "✅ PASSED" if report["passed"] else "❌ FAILED"
        print(f"\n{layer.upper()}: {status}")
        print(f"  Records: {report['record_count']}")
        if report["failed_checks"]:
            print(f"  Failed: {report['failed_checks']}")
    
    print("\n" + "=" * 60)
    print(f"OVERALL: {'✅ ALL PASSED' if result['passed'] else '❌ VALIDATION FAILED'}")
    print("=" * 60)
    
    sys.exit(0 if result["passed"] else 1)