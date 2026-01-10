"""
Pipelines module.

Contains data pipeline implementations for each layer:
- bronze_layer: Raw data ingestion
- silver_layer: Data transformation and partitioning  
- gold_layer: Aggregations and analytics
"""

from .bronze_layer import BronzeLayerPipeline, run_bronze_pipeline

__all__ = [
    "BronzeLayerPipeline",
    "run_bronze_pipeline",
]