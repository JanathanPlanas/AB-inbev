"""
Silver Layer Transformations.

This module contains transformation functions for converting
Bronze layer data into clean, standardized Silver layer data.

Transformations applied:
- Data type standardization
- Null handling
- Column selection and ordering
- Deduplication
- Coordinate validation
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# Define the expected schema for Silver layer
SILVER_SCHEMA = {
    "id": "string",
    "name": "string",
    "brewery_type": "string",
    "address_1": "string",
    "address_2": "string",
    "address_3": "string",
    "city": "string",
    "state_province": "string",
    "postal_code": "string",
    "country": "string",
    "longitude": "float64",
    "latitude": "float64",
    "phone": "string",
    "website_url": "string",
}

# Columns to keep in Silver layer (excluding deprecated and metadata columns)
SILVER_COLUMNS = [
    "id",
    "name", 
    "brewery_type",
    "address_1",
    "address_2",
    "address_3",
    "city",
    "state_province",
    "postal_code",
    "country",
    "longitude",
    "latitude",
    "phone",
    "website_url",
]

# Valid brewery types according to API documentation
VALID_BREWERY_TYPES = [
    "micro",
    "nano", 
    "regional",
    "brewpub",
    "large",
    "planning",
    "bar",
    "contract",
    "proprietor",
    "closed",
]


def select_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Select and order columns for Silver layer.
    
    Args:
        df: Input DataFrame
        columns: List of columns to select (defaults to SILVER_COLUMNS)
        
    Returns:
        DataFrame with selected columns
    """
    columns = columns or SILVER_COLUMNS
    
    # Only select columns that exist in the DataFrame
    available_columns = [col for col in columns if col in df.columns]
    missing_columns = [col for col in columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns in source data: {missing_columns}")
    
    logger.info(f"Selecting {len(available_columns)} columns")
    return df[available_columns].copy()


def standardize_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column data types.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized types
    """
    df = df.copy()
    
    # String columns
    string_columns = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "phone", "website_url"
    ]
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype("string")
    
    # Numeric columns (coordinates)
    numeric_columns = ["longitude", "latitude"]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    logger.info("Standardized column data types")
    return df


def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle null values appropriately.
    
    - String columns: Replace None with empty string or keep as NA
    - Numeric columns: Keep as NaN for missing coordinates
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with handled nulls
    """
    df = df.copy()
    
    # For string columns, convert None to pandas NA
    string_columns = df.select_dtypes(include=["string", "object"]).columns
    for col in string_columns:
        df[col] = df[col].replace({None: pd.NA, "": pd.NA, "None": pd.NA})
    
    # Log null counts
    null_counts = df.isnull().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    
    if len(columns_with_nulls) > 0:
        logger.info(f"Columns with null values: {dict(columns_with_nulls)}")
    
    return df


def validate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean coordinate values.
    
    Valid ranges:
    - Latitude: -90 to 90
    - Longitude: -180 to 180
    
    Invalid coordinates are set to NaN.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with validated coordinates
    """
    df = df.copy()
    
    if "latitude" in df.columns:
        invalid_lat = (df["latitude"] < -90) | (df["latitude"] > 90)
        invalid_lat_count = invalid_lat.sum()
        if invalid_lat_count > 0:
            logger.warning(f"Found {invalid_lat_count} invalid latitude values, setting to NaN")
            df.loc[invalid_lat, "latitude"] = np.nan
    
    if "longitude" in df.columns:
        invalid_lon = (df["longitude"] < -180) | (df["longitude"] > 180)
        invalid_lon_count = invalid_lon.sum()
        if invalid_lon_count > 0:
            logger.warning(f"Found {invalid_lon_count} invalid longitude values, setting to NaN")
            df.loc[invalid_lon, "longitude"] = np.nan
    
    return df


def validate_brewery_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate brewery_type values against known types.
    
    Unknown types are logged but kept (API might have new types).
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame (unchanged, but with logging)
    """
    if "brewery_type" not in df.columns:
        return df
    
    unique_types = df["brewery_type"].dropna().unique()
    unknown_types = [t for t in unique_types if t not in VALID_BREWERY_TYPES]
    
    if unknown_types:
        logger.warning(f"Found unknown brewery types: {unknown_types}")
    
    type_counts = df["brewery_type"].value_counts()
    logger.info(f"Brewery type distribution:\n{type_counts.to_string()}")
    
    return df


def deduplicate(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Remove duplicate records.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for deduplication (defaults to 'id')
        
    Returns:
        DataFrame with duplicates removed
    """
    subset = subset or ["id"]
    
    initial_count = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    final_count = len(df)
    
    removed = initial_count - final_count
    if removed > 0:
        logger.info(f"Removed {removed} duplicate records")
    else:
        logger.info("No duplicate records found")
    
    return df


def clean_string_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean string values by trimming whitespace.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with cleaned strings
    """
    df = df.copy()
    
    string_columns = df.select_dtypes(include=["string", "object"]).columns
    
    for col in string_columns:
        # Strip whitespace
        df[col] = df[col].str.strip()
        # Replace empty strings with NA
        df[col] = df[col].replace({"": pd.NA})
    
    logger.info(f"Cleaned string values in {len(string_columns)} columns")
    return df


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure partition columns are properly formatted.
    
    Creates clean versions of country and state_province for partitioning.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with partition-ready columns
    """
    df = df.copy()
    
    # Clean country for partitioning (replace spaces, handle nulls)
    if "country" in df.columns:
        df["country"] = df["country"].fillna("Unknown")
        df["country"] = df["country"].str.strip()
        df["country"] = df["country"].replace({"": "Unknown"})
    
    # Clean state_province for partitioning
    if "state_province" in df.columns:
        df["state_province"] = df["state_province"].fillna("Unknown")
        df["state_province"] = df["state_province"].str.strip()
        df["state_province"] = df["state_province"].replace({"": "Unknown"})
    
    logger.info("Prepared partition columns")
    return df


def transform_bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all Silver layer transformations.
    
    This is the main transformation function that chains all
    individual transformations in the correct order.
    
    Args:
        df: Bronze layer DataFrame
        
    Returns:
        Transformed Silver layer DataFrame
    """
    logger.info(f"Starting Silver transformation with {len(df)} records")
    
    # Apply transformations in order
    df = select_columns(df)
    df = standardize_types(df)
    df = handle_nulls(df)
    df = clean_string_values(df)
    df = validate_coordinates(df)
    df = validate_brewery_type(df)
    df = deduplicate(df)
    df = add_partition_columns(df)
    
    logger.info(f"Silver transformation complete: {len(df)} records")
    return df


def get_transformation_summary(
    bronze_df: pd.DataFrame, 
    silver_df: pd.DataFrame
) -> dict:
    """
    Generate a summary of the transformation.
    
    Args:
        bronze_df: Original Bronze DataFrame
        silver_df: Transformed Silver DataFrame
        
    Returns:
        Dictionary with transformation statistics
    """
    return {
        "bronze_record_count": len(bronze_df),
        "silver_record_count": len(silver_df),
        "records_removed": len(bronze_df) - len(silver_df),
        "bronze_columns": list(bronze_df.columns),
        "silver_columns": list(silver_df.columns),
        "columns_removed": len(bronze_df.columns) - len(silver_df.columns),
        "null_counts": dict(silver_df.isnull().sum()),
        "brewery_types": dict(silver_df["brewery_type"].value_counts()) if "brewery_type" in silver_df.columns else {},
        "countries": list(silver_df["country"].unique()) if "country" in silver_df.columns else [],
    }
