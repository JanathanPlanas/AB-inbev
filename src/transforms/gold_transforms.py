"""
Gold Layer Transformations.

This module contains aggregation functions for creating
analytical views from the Silver layer data.

Main aggregation:
- Quantity of breweries per type and location (country/state)
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def aggregate_by_type_and_location(
    df: pd.DataFrame,
    group_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Aggregate breweries by type and location.
    
    Creates a summary table with the count of breweries
    for each combination of brewery_type, country, and state_province.
    
    Args:
        df: Silver layer DataFrame
        group_cols: Columns to group by (default: country, state_province, brewery_type)
        
    Returns:
        Aggregated DataFrame with brewery counts
        
    Example:
        >>> gold_df = aggregate_by_type_and_location(silver_df)
        >>> print(gold_df.head())
        
        | country       | state_province | brewery_type | brewery_count |
        |---------------|----------------|--------------|---------------|
        | United States | California     | micro        | 523           |
        | United States | California     | brewpub      | 187           |
    """
    group_cols = group_cols or ["country", "state_province", "brewery_type"]
    
    logger.info(f"Aggregating by: {group_cols}")
    
    # Perform aggregation
    aggregated = (
        df.groupby(group_cols, as_index=False)
        .agg(brewery_count=("id", "count"))
    )
    
    # Sort for consistent output (only use columns that exist)
    sort_cols = [col for col in ["country", "state_province", "brewery_count"] if col in aggregated.columns]
    sort_ascending = [True if col != "brewery_count" else False for col in sort_cols]
    
    aggregated = aggregated.sort_values(
        by=sort_cols,
        ascending=sort_ascending
    ).reset_index(drop=True)
    
    logger.info(f"Created {len(aggregated)} aggregated rows")
    return aggregated


def aggregate_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate breweries by type only (global summary).
    
    Args:
        df: Silver layer DataFrame
        
    Returns:
        DataFrame with brewery counts per type
    """
    aggregated = (
        df.groupby("brewery_type", as_index=False)
        .agg(brewery_count=("id", "count"))
        .sort_values("brewery_count", ascending=False)
        .reset_index(drop=True)
    )
    
    logger.info(f"Global type distribution: {len(aggregated)} types")
    return aggregated


def aggregate_by_country(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate breweries by country only.
    
    Args:
        df: Silver layer DataFrame
        
    Returns:
        DataFrame with brewery counts per country
    """
    aggregated = (
        df.groupby("country", as_index=False)
        .agg(brewery_count=("id", "count"))
        .sort_values("brewery_count", ascending=False)
        .reset_index(drop=True)
    )
    
    logger.info(f"Country distribution: {len(aggregated)} countries")
    return aggregated


def aggregate_by_state(df: pd.DataFrame, country: Optional[str] = None) -> pd.DataFrame:
    """
    Aggregate breweries by state/province.
    
    Args:
        df: Silver layer DataFrame
        country: Optional filter by country
        
    Returns:
        DataFrame with brewery counts per state
    """
    if country:
        df = df[df["country"] == country]
    
    aggregated = (
        df.groupby(["country", "state_province"], as_index=False)
        .agg(brewery_count=("id", "count"))
        .sort_values("brewery_count", ascending=False)
        .reset_index(drop=True)
    )
    
    logger.info(f"State distribution: {len(aggregated)} states")
    return aggregated


def create_gold_summary(df: pd.DataFrame) -> dict:
    """
    Create a comprehensive summary for the Gold layer.
    
    Args:
        df: Silver layer DataFrame
        
    Returns:
        Dictionary with multiple aggregation summaries
    """
    return {
        "total_breweries": len(df),
        "total_countries": df["country"].nunique(),
        "total_states": df["state_province"].nunique(),
        "total_types": df["brewery_type"].nunique(),
        "by_type": aggregate_by_type(df).to_dict(orient="records"),
        "by_country": aggregate_by_country(df).to_dict(orient="records"),
        "top_states": aggregate_by_state(df).head(10).to_dict(orient="records"),
    }


def get_aggregation_stats(gold_df: pd.DataFrame) -> dict:
    """
    Get statistics about the Gold layer aggregation.
    
    Args:
        gold_df: Gold layer aggregated DataFrame
        
    Returns:
        Dictionary with aggregation statistics
    """
    return {
        "total_rows": len(gold_df),
        "total_breweries": gold_df["brewery_count"].sum(),
        "unique_countries": gold_df["country"].nunique(),
        "unique_states": gold_df["state_province"].nunique(),
        "unique_types": gold_df["brewery_type"].nunique(),
        "avg_breweries_per_group": round(gold_df["brewery_count"].mean(), 2),
        "max_breweries_in_group": gold_df["brewery_count"].max(),
        "min_breweries_in_group": gold_df["brewery_count"].min(),
    }
