"""
Silver Layer Transformations using DuckDB + PyArrow.
NO PANDAS DEPENDENCY.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Any

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

SILVER_COLUMNS = [
    "id", "name", "brewery_type", "address_1", "address_2", "address_3",
    "city", "state_province", "postal_code", "country",
    "longitude", "latitude", "phone", "website_url",
]

VALID_BREWERY_TYPES = [
    "micro", "nano", "regional", "brewpub", "large",
    "planning", "bar", "contract", "proprietor", "closed",
]

SILVER_SCHEMA = pa.schema([
    ("id", pa.string()), ("name", pa.string()), ("brewery_type", pa.string()),
    ("address_1", pa.string()), ("address_2", pa.string()), ("address_3", pa.string()),
    ("city", pa.string()), ("state_province", pa.string()), ("postal_code", pa.string()),
    ("country", pa.string()), ("longitude", pa.float64()), ("latitude", pa.float64()),
    ("phone", pa.string()), ("website_url", pa.string()),
])


class DuckDBTransformer:
    """DuckDB-based transformer for Silver layer."""
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
    
    def transform_bronze_to_silver(self, bronze_data: pa.Table | list[dict]) -> pa.Table:
        """Transform Bronze data to Silver."""
        # Convert input to list if PyArrow Table
        if isinstance(bronze_data, pa.Table):
            data_list = bronze_data.to_pylist()
        else:
            data_list = bronze_data
        
        if not data_list:
            # Return empty table with schema
            return pa.Table.from_pylist([], schema=SILVER_SCHEMA)
        
        record_count = len(data_list)
        logger.info(f"Starting DuckDB transformation with {record_count} records")
        
        # Normalize data - ensure all expected columns exist
        normalized = []
        for row in data_list:
            norm_row = {}
            for col in SILVER_COLUMNS:
                norm_row[col] = row.get(col)
            normalized.append(norm_row)
        
        bronze_table = pa.Table.from_pylist(normalized)
        self.conn.register("bronze", bronze_table)
        
        transform_sql = """
        WITH cleaned AS (
            SELECT
                id,
                TRIM(name) as name,
                TRIM(LOWER(brewery_type)) as brewery_type,
                TRIM(address_1) as address_1,
                TRIM(address_2) as address_2,
                TRIM(address_3) as address_3,
                TRIM(city) as city,
                TRIM(state_province) as state_province,
                TRIM(postal_code) as postal_code,
                TRIM(country) as country,
                TRY_CAST(longitude AS DOUBLE) as longitude,
                TRY_CAST(latitude AS DOUBLE) as latitude,
                TRIM(phone) as phone,
                TRIM(website_url) as website_url
            FROM bronze
            WHERE id IS NOT NULL
        ),
        validated AS (
            SELECT
                id, name, brewery_type, address_1, address_2, address_3,
                city, state_province, postal_code, country,
                CASE WHEN longitude < -180 OR longitude > 180 THEN NULL ELSE longitude END as longitude,
                CASE WHEN latitude < -90 OR latitude > 90 THEN NULL ELSE latitude END as latitude,
                phone, website_url
            FROM cleaned
        ),
        deduplicated AS (
            SELECT DISTINCT ON (id)
                id, name, brewery_type, address_1, address_2, address_3, city,
                COALESCE(NULLIF(TRIM(state_province), ''), 'Unknown') as state_province,
                postal_code,
                COALESCE(NULLIF(TRIM(country), ''), 'Unknown') as country,
                longitude, latitude, phone, website_url
            FROM validated
            ORDER BY id
        )
        SELECT * FROM deduplicated
        """
        
        result = self.conn.execute(transform_sql).fetch_arrow_table()
        logger.info(f"DuckDB transformation complete: {result.num_rows} records")
        return result
    
    def get_transformation_summary(self, bronze_table: pa.Table, silver_table: pa.Table) -> dict:
        """Generate transformation summary."""
        if silver_table.num_rows == 0:
            return {
                "bronze_record_count": bronze_table.num_rows,
                "silver_record_count": 0,
                "records_removed": bronze_table.num_rows,
                "null_counts": {},
                "unique_countries": 0,
                "unique_states": 0,
                "unique_types": 0,
            }
        
        self.conn.register("silver_summary", silver_table)
        stats = self.conn.execute("""
            SELECT 
                COUNT(DISTINCT country) as unique_countries,
                COUNT(DISTINCT state_province) as unique_states,
                COUNT(DISTINCT brewery_type) as unique_types
            FROM silver_summary
        """).fetchone()
        
        return {
            "bronze_record_count": bronze_table.num_rows,
            "silver_record_count": silver_table.num_rows,
            "records_removed": bronze_table.num_rows - silver_table.num_rows,
            "null_counts": {},
            "unique_countries": stats[0],
            "unique_states": stats[1],
            "unique_types": stats[2],
        }
    
    def close(self):
        self.conn.close()


# Convenience functions
def transform_bronze_to_silver(data: pa.Table | list[dict]) -> pa.Table:
    transformer = DuckDBTransformer()
    try:
        return transformer.transform_bronze_to_silver(data)
    finally:
        transformer.close()


def get_transformation_summary(bronze_table: pa.Table, silver_table: pa.Table) -> dict:
    transformer = DuckDBTransformer()
    try:
        return transformer.get_transformation_summary(bronze_table, silver_table)
    finally:
        transformer.close()


# Helper functions
def arrow_table_from_pylist(data: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(data)

def arrow_table_to_pylist(table: pa.Table) -> list[dict]:
    return table.to_pylist()

def get_column_as_list(table: pa.Table, column: str) -> list:
    return table.column(column).to_pylist()
