"""
Silver Layer Reader.

This module handles reading Parquet data from the Silver layer
for transformation into the Gold layer.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SilverReader:
    """
    Reader for Silver layer data (Parquet files).
    
    Supports reading partitioned Parquet datasets.
    
    Example:
        >>> reader = SilverReader(base_dir="data/silver/breweries")
        >>> df = reader.read_all()
        >>> print(f"Read {len(df)} records")
    """
    
    def __init__(self, base_dir: str | Path = "data/silver/breweries"):
        """
        Initialize the Silver reader.
        
        Args:
            base_dir: Base directory for silver layer data
        """
        self.base_dir = Path(base_dir)
    
    def read_all(self) -> pd.DataFrame:
        """
        Read all data from the Silver layer.
        
        Returns:
            DataFrame with all Silver layer records
            
        Raises:
            FileNotFoundError: If Silver layer data doesn't exist
        """
        if not self.base_dir.exists():
            raise FileNotFoundError(f"Silver layer not found: {self.base_dir}")
        
        logger.info(f"Reading Silver layer from: {self.base_dir}")
        
        df = pd.read_parquet(self.base_dir)
        
        logger.info(f"Read {len(df)} records from Silver layer")
        return df
    
    def read_by_country(self, country: str) -> pd.DataFrame:
        """
        Read data for a specific country.
        
        Args:
            country: Country name to filter
            
        Returns:
            DataFrame with records for the specified country
        """
        country_path = self.base_dir / f"country={country}"
        
        if not country_path.exists():
            logger.warning(f"Country not found: {country}")
            return pd.DataFrame()
        
        df = pd.read_parquet(country_path)
        df["country"] = country  # Add back partition column
        
        logger.info(f"Read {len(df)} records for country: {country}")
        return df
    
    def read_by_state(self, country: str, state: str) -> pd.DataFrame:
        """
        Read data for a specific country and state.
        
        Args:
            country: Country name
            state: State/province name
            
        Returns:
            DataFrame with records for the specified location
        """
        state_path = self.base_dir / f"country={country}" / f"state_province={state}"
        
        if not state_path.exists():
            logger.warning(f"State not found: {country}/{state}")
            return pd.DataFrame()
        
        df = pd.read_parquet(state_path)
        df["country"] = country
        df["state_province"] = state
        
        logger.info(f"Read {len(df)} records for: {country}/{state}")
        return df
    
    def get_available_countries(self) -> List[str]:
        """
        Get list of available countries in the Silver layer.
        
        Returns:
            List of country names
        """
        countries = []
        
        for path in self.base_dir.glob("country=*"):
            if path.is_dir():
                country = path.name.replace("country=", "")
                countries.append(country)
        
        return sorted(countries)
    
    def get_available_states(self, country: str) -> List[str]:
        """
        Get list of available states for a country.
        
        Args:
            country: Country name
            
        Returns:
            List of state names
        """
        states = []
        country_path = self.base_dir / f"country={country}"
        
        if not country_path.exists():
            return states
        
        for path in country_path.glob("state_province=*"):
            if path.is_dir():
                state = path.name.replace("state_province=", "")
                states.append(state)
        
        return sorted(states)
    
    def is_ready(self) -> bool:
        """
        Check if Silver layer data is ready to read.
        
        Returns:
            True if _SUCCESS marker exists
        """
        success_marker = self.base_dir / "_SUCCESS"
        return success_marker.exists()
