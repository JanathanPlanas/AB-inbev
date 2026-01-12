"""
Airflow DAG - Breweries Data Pipeline

This DAG orchestrates the complete data pipeline:
Bronze (API extraction) → Silver (transformation) → Gold (aggregation)

Features:
- Daily scheduling
- Retry logic with exponential backoff
- Error handling and alerting
- Sequential task dependencies

Author: Janathan Junior
Date: January 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Import pipeline functions
import sys
sys.path.insert(0, '/opt/airflow/app')

from src.pipelines.bronze_layer import run_bronze_pipeline
from src.pipelines.silver_layer import run_silver_pipeline
from src.pipelines.gold_layer import run_gold_pipeline


# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=1),
}

dag_description = """
## Breweries Data Pipeline

This pipeline fetches brewery data from the Open Brewery DB API and processes it 
through the medallion architecture (Bronze → Silver → Gold).

### Layers:
- **Bronze**: Raw API data in JSONL.gz format
- **Silver**: Cleaned Parquet files partitioned by country/state
- **Gold**: Aggregated analytics (breweries per type and location)

### Schedule: Daily at 00:00 UTC
"""


# =============================================================================
# Task Functions
# =============================================================================

def extract_to_bronze(**context):
    """
    Extract data from API and persist to Bronze layer.
    
    This task:
    - Fetches all breweries from Open Brewery DB API
    - Handles pagination automatically
    - Persists raw data as JSONL.gz
    - Creates manifest with metadata
    """
    print("=" * 60)
    print("Starting Bronze Layer Extraction")
    print("=" * 60)
    
    result = run_bronze_pipeline(base_dir="/opt/airflow/data/bronze/breweries")
    
    # Push metrics to XCom for downstream tasks
    context['ti'].xcom_push(key='bronze_record_count', value=result.get('total_records', 0))
    context['ti'].xcom_push(key='bronze_run_dir', value=result.get('run_dir', ''))
    
    print(f"Bronze layer complete: {result.get('total_records', 0)} records")
    return result


def transform_to_silver(**context):
    """
    Transform Bronze data to Silver layer.
    
    This task:
    - Reads raw JSONL.gz from Bronze
    - Applies data cleaning and standardization
    - Converts to Parquet format
    - Partitions by country and state_province
    """
    print("=" * 60)
    print("Starting Silver Layer Transformation")
    print("=" * 60)
    
    result = run_silver_pipeline(
        bronze_dir="/opt/airflow/data/bronze/breweries",
        silver_dir="/opt/airflow/data/silver/breweries"
    )
    
    # Push metrics to XCom
    context['ti'].xcom_push(key='silver_record_count', value=result.get('silver_record_count', 0))
    context['ti'].xcom_push(key='records_removed', value=result.get('records_removed', 0))
    
    print(f"Silver layer complete: {result.get('silver_record_count', 0)} records")
    return result


def aggregate_to_gold(**context):
    """
    Create Gold layer aggregations.
    
    This task:
    - Reads Parquet from Silver layer
    - Creates aggregated views (breweries per type and location)
    - Writes analytical Parquet files
    """
    print("=" * 60)
    print("Starting Gold Layer Aggregation")
    print("=" * 60)
    
    result = run_gold_pipeline(
        silver_dir="/opt/airflow/data/silver/breweries",
        gold_dir="/opt/airflow/data/gold/breweries"
    )
    
    # Push metrics to XCom
    context['ti'].xcom_push(key='gold_total_rows', value=result.get('total_rows', 0))
    context['ti'].xcom_push(key='gold_total_breweries', value=result.get('total_breweries', 0))
    
    print(f"Gold layer complete: {result.get('total_rows', 0)} aggregation rows")
    return result


def validate_pipeline(**context):
    """
    Validate pipeline execution and data quality.
    
    This task:
    - Checks record counts across layers
    - Validates data consistency
    - Logs final metrics
    """
    ti = context['ti']
    
    # Pull metrics from previous tasks
    bronze_count = ti.xcom_pull(task_ids='extract_bronze', key='bronze_record_count') or 0
    silver_count = ti.xcom_pull(task_ids='transform_silver', key='silver_record_count') or 0
    gold_breweries = ti.xcom_pull(task_ids='aggregate_gold', key='gold_total_breweries') or 0
    
    print("=" * 60)
    print("Pipeline Validation Summary")
    print("=" * 60)
    print(f"Bronze records: {bronze_count}")
    print(f"Silver records: {silver_count}")
    print(f"Gold total breweries: {gold_breweries}")
    
    # Validate counts
    if silver_count == 0:
        raise ValueError("Silver layer has 0 records - pipeline may have failed")
    
    if gold_breweries != silver_count:
        print(f"WARNING: Gold brewery count ({gold_breweries}) differs from Silver ({silver_count})")
    
    # Calculate data loss
    if bronze_count > 0:
        retention_rate = (silver_count / bronze_count) * 100
        print(f"Data retention rate: {retention_rate:.2f}%")
        
        if retention_rate < 90:
            print(f"WARNING: High data loss detected ({100 - retention_rate:.2f}%)")
    
    print("=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)
    
    return {
        'bronze_count': bronze_count,
        'silver_count': silver_count,
        'gold_breweries': gold_breweries,
        'status': 'success'
    }


def on_failure_callback(context):
    """
    Callback function executed when a task fails.
    
    In production, this would:
    - Send Slack/Teams notification
    - Create PagerDuty incident
    - Log to monitoring system
    """
    task_instance = context['task_instance']
    exception = context.get('exception', 'Unknown error')
    
    error_message = f"""
    🚨 PIPELINE FAILURE ALERT 🚨
    
    DAG: {context['dag'].dag_id}
    Task: {task_instance.task_id}
    Execution Date: {context['execution_date']}
    Error: {exception}
    
    Log URL: {task_instance.log_url}
    """
    
    print(error_message)
    # In production: send to Slack, PagerDuty, etc.


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='breweries_pipeline',
    default_args=default_args,
    description='ETL pipeline for Open Brewery DB data',
    schedule_interval='@daily',  # Run daily at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['breweries', 'etl', 'medallion', 'data-engineering'],
    doc_md=dag_description,
    on_failure_callback=on_failure_callback,
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )
    
    # Bronze Layer - Extract from API
    extract_bronze = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_to_bronze,
        doc_md="""
        ## Extract to Bronze Layer
        
        Fetches all breweries from Open Brewery DB API with:
        - Automatic pagination
        - Retry on failure
        - Raw data persistence in JSONL.gz format
        """,
    )
    
    # Silver Layer - Transform
    transform_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_to_silver,
        doc_md="""
        ## Transform to Silver Layer
        
        Transforms raw data with:
        - Data type standardization
        - Null handling
        - Coordinate validation
        - Deduplication
        - Parquet output partitioned by location
        """,
    )
    
    # Gold Layer - Aggregate
    aggregate_gold = PythonOperator(
        task_id='aggregate_gold',
        python_callable=aggregate_to_gold,
        doc_md="""
        ## Aggregate to Gold Layer
        
        Creates analytical views:
        - Breweries per type and location
        - Breweries per type (global)
        - Breweries per country
        """,
    )
    
    # Validation task
    validate = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline,
        doc_md="""
        ## Validate Pipeline
        
        Checks data quality:
        - Record count validation
        - Cross-layer consistency
        - Data retention metrics
        """,
    )
    
    # End task
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc='Pipeline end marker'
    )
    
    # Define task dependencies (Medallion flow)
    start >> extract_bronze >> transform_silver >> aggregate_gold >> validate >> end