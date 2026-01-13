"""
Gold Layer Transformations using DuckDB + PyArrow.
NO PANDAS DEPENDENCY.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)


class DuckDBAggregator:
    """DuckDB-based aggregator for Gold layer. No Pandas."""
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
    
    def aggregate_by_type_and_location(
        self,
        table: pa.Table,
        group_cols: Optional[List[str]] = None
    ) -> pa.Table:
        """Aggregate breweries by type and location."""
        group_cols = group_cols or ["country", "state_province", "brewery_type"]
        
        if table.num_rows == 0:
            schema = pa.schema([(col, pa.string()) for col in group_cols] + [("brewery_count", pa.int64())])
            return pa.Table.from_pylist([], schema=schema)
        
        logger.info(f"Aggregating by: {group_cols}")
        self.conn.register("silver", table)
        
        group_cols_sql = ", ".join(f'"{col}"' for col in group_cols)
        order_cols = []
        if "country" in group_cols:
            order_cols.append('"country" ASC')
        if "state_province" in group_cols:
            order_cols.append('"state_province" ASC')
        order_cols.append("brewery_count DESC")
        
        sql = f"""
            SELECT {group_cols_sql}, COUNT(*)::BIGINT as brewery_count
            FROM silver
            GROUP BY {group_cols_sql}
            ORDER BY {", ".join(order_cols)}
        """
        
        result = self.conn.execute(sql).fetch_arrow_table()
        logger.info(f"Created {result.num_rows} aggregated rows")
        return result
    
    def aggregate_by_type(self, table: pa.Table) -> pa.Table:
        """Aggregate breweries by type."""
        if table.num_rows == 0:
            return pa.Table.from_pylist([], schema=pa.schema([("brewery_type", pa.string()), ("brewery_count", pa.int64())]))
        
        self.conn.register("silver_type", table)
        return self.conn.execute("""
            SELECT brewery_type, COUNT(*)::BIGINT as brewery_count
            FROM silver_type GROUP BY brewery_type ORDER BY brewery_count DESC
        """).fetch_arrow_table()
    
    def aggregate_by_country(self, table: pa.Table) -> pa.Table:
        """Aggregate breweries by country."""
        if table.num_rows == 0:
            return pa.Table.from_pylist([], schema=pa.schema([("country", pa.string()), ("brewery_count", pa.int64())]))
        
        self.conn.register("silver_country", table)
        return self.conn.execute("""
            SELECT country, COUNT(*)::BIGINT as brewery_count
            FROM silver_country GROUP BY country ORDER BY brewery_count DESC
        """).fetch_arrow_table()
    
    def aggregate_by_state(self, table: pa.Table, country: Optional[str] = None) -> pa.Table:
        """Aggregate breweries by state."""
        if table.num_rows == 0:
            return pa.Table.from_pylist([], schema=pa.schema([
                ("country", pa.string()), ("state_province", pa.string()), ("brewery_count", pa.int64())
            ]))
        
        self.conn.register("silver_state", table)
        if country:
            sql = f"""
                SELECT country, state_province, COUNT(*)::BIGINT as brewery_count
                FROM silver_state WHERE country = '{country}'
                GROUP BY country, state_province ORDER BY brewery_count DESC
            """
        else:
            sql = """
                SELECT country, state_province, COUNT(*)::BIGINT as brewery_count
                FROM silver_state GROUP BY country, state_province ORDER BY brewery_count DESC
            """
        return self.conn.execute(sql).fetch_arrow_table()
    
    def create_gold_summary(self, table: pa.Table) -> dict:
        """Create comprehensive summary."""
        if table.num_rows == 0:
            return {"total_breweries": 0, "total_countries": 0, "total_states": 0, "total_types": 0,
                    "by_type": [], "by_country": [], "top_states": []}
        
        self.conn.register("silver_summary", table)
        totals = self.conn.execute("""
            SELECT COUNT(*), COUNT(DISTINCT country), COUNT(DISTINCT state_province), COUNT(DISTINCT brewery_type)
            FROM silver_summary
        """).fetchone()
        
        return {
            "total_breweries": totals[0],
            "total_countries": totals[1],
            "total_states": totals[2],
            "total_types": totals[3],
            "by_type": self.aggregate_by_type(table).to_pylist(),
            "by_country": self.aggregate_by_country(table).to_pylist(),
            "top_states": self.aggregate_by_state(table).to_pylist()[:10],
        }
    
    def get_aggregation_stats(self, gold_table: pa.Table) -> dict:
        """Get aggregation statistics."""
        if gold_table.num_rows == 0:
            return {"total_rows": 0, "total_breweries": 0, "unique_countries": 0, "unique_states": 0,
                    "unique_types": 0, "avg_breweries_per_group": 0.0, "max_breweries_in_group": 0, "min_breweries_in_group": 0}
        
        self.conn.register("gold_stats", gold_table)
        stats = self.conn.execute("""
            SELECT COUNT(*), SUM(brewery_count), COUNT(DISTINCT country), COUNT(DISTINCT state_province),
                   COUNT(DISTINCT brewery_type), ROUND(AVG(brewery_count), 2), MAX(brewery_count), MIN(brewery_count)
            FROM gold_stats
        """).fetchone()
        
        return {
            "total_rows": int(stats[0]),
            "total_breweries": int(stats[1]) if stats[1] else 0,
            "unique_countries": int(stats[2]) if stats[2] else 0,
            "unique_states": int(stats[3]) if stats[3] else 0,
            "unique_types": int(stats[4]) if stats[4] else 0,
            "avg_breweries_per_group": float(stats[5]) if stats[5] else 0.0,
            "max_breweries_in_group": int(stats[6]) if stats[6] else 0,
            "min_breweries_in_group": int(stats[7]) if stats[7] else 0,
        }
    
    def close(self):
        self.conn.close()


# Convenience functions
def aggregate_by_type_and_location(table: pa.Table, group_cols: Optional[List[str]] = None) -> pa.Table:
    agg = DuckDBAggregator()
    try:
        return agg.aggregate_by_type_and_location(table, group_cols)
    finally:
        agg.close()

def aggregate_by_type(table: pa.Table) -> pa.Table:
    agg = DuckDBAggregator()
    try:
        return agg.aggregate_by_type(table)
    finally:
        agg.close()

def aggregate_by_country(table: pa.Table) -> pa.Table:
    agg = DuckDBAggregator()
    try:
        return agg.aggregate_by_country(table)
    finally:
        agg.close()

def aggregate_by_state(table: pa.Table, country: Optional[str] = None) -> pa.Table:
    agg = DuckDBAggregator()
    try:
        return agg.aggregate_by_state(table, country)
    finally:
        agg.close()

def create_gold_summary(table: pa.Table) -> dict:
    agg = DuckDBAggregator()
    try:
        return agg.create_gold_summary(table)
    finally:
        agg.close()

def get_aggregation_stats(gold_table: pa.Table) -> dict:
    agg = DuckDBAggregator()
    try:
        return agg.get_aggregation_stats(gold_table)
    finally:
        agg.close()
