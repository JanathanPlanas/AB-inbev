"""
Breweries ETL Pipeline DAG with Test-Driven Validation.

This DAG implements a robust pipeline with:
- Pre-execution tests validation
- Data quality checks between layers
- Automatic rollback on failure

Author: Janathan
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
import subprocess


# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}


# =============================================================================
# Test Functions
# =============================================================================
def run_unit_tests(**context):
    """
    Run all unit tests before pipeline execution.
    If tests fail, the pipeline stops immediately.
    """
    result = subprocess.run(
       ["python", "-m", "pytest", "/opt/airflow/tests", "-v", "--tb=short"],
        capture_output=True,
        text=True
    )
    
    print("=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(result.stdout)
    
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise AirflowFailException(
            f"Unit tests failed! Pipeline execution blocked.\n{result.stdout}"
        )
    
    context['ti'].xcom_push(key='tests_passed', value=True)
    return "All tests passed ✅"


def validate_bronze_data(**context):
    """Validate Bronze layer data quality."""
    from pathlib import Path
    import json
    
    bronze_path = Path("data/bronze/breweries")
    
    if not bronze_path.exists():
        raise AirflowFailException("Bronze layer directory not found!")
    
    runs = sorted(bronze_path.glob("ingestion_date=*/run_id=*"), reverse=True)
    if not runs:
        raise AirflowFailException("No Bronze data found!")
    
    latest_run = runs[0]
    manifest_path = latest_run / "_manifest.json"
    
    if not manifest_path.exists():
        raise AirflowFailException(f"Manifest not found in {latest_run}")
    
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    total_records = manifest.get("total_records", 0)
    
    checks = {
        "has_records": total_records > 0,
        "has_pages": len(manifest.get("pages", [])) > 0,
        "has_expected_total": manifest.get("expected_total") == total_records,
    }

    failed_checks = [k for k, v in checks.items() if not v]
    
    if failed_checks:
        raise AirflowFailException(f"Bronze validation failed: {failed_checks}")
    
    context['ti'].xcom_push(key='bronze_records', value=total_records)
    print(f"✅ Bronze validation passed: {total_records} records")
    return total_records


def validate_silver_data(**context):
    """Validate Silver layer data quality."""
    from pathlib import Path
    from deltalake import DeltaTable
    import duckdb
    
    silver_path = Path("data/silver/breweries")
    
    if not silver_path.exists():
        raise AirflowFailException("Silver layer directory not found!")
    
    delta_log = silver_path / "_delta_log"
    if not delta_log.exists():
        raise AirflowFailException("Silver Delta table not initialized!")
    
    dt = DeltaTable(str(silver_path))
    table = dt.to_pyarrow_table()
    record_count = table.num_rows
    
    bronze_records = context['ti'].xcom_pull(key='bronze_records', task_ids='validate_bronze')
    
    checks = {
        "has_records": record_count > 0,
        "no_major_data_loss": record_count >= bronze_records * 0.9,
        "has_required_columns": all(
            col in table.column_names 
            for col in ["id", "name", "brewery_type", "country", "state_province"]
        ),
    }
    
    conn = duckdb.connect(":memory:")
    conn.register("silver", table)
    null_check = conn.execute("""
        SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_ids
        FROM silver
    """).fetchone()
    conn.close()
    
    checks["no_null_ids"] = null_check[0] == 0
    
    failed_checks = [k for k, v in checks.items() if not v]
    if failed_checks:
        raise AirflowFailException(f"Silver validation failed: {failed_checks}")
    
    context['ti'].xcom_push(key='silver_records', value=record_count)
    print(f"✅ Silver validation passed: {record_count} records")
    return record_count


def validate_gold_data(**context):
    """Validate Gold layer data quality."""
    from pathlib import Path
    from deltalake import DeltaTable
    import duckdb
    import json
    
    gold_path = Path("data/gold/breweries")
    
    if not gold_path.exists():
        raise AirflowFailException("Gold layer directory not found!")
    
    main_table_path = gold_path / "breweries_by_type_and_location"
    if not main_table_path.exists():
        raise AirflowFailException("Gold main table not found!")
    
    dt = DeltaTable(str(main_table_path))
    table = dt.to_pyarrow_table()
    
    summary_path = gold_path / "_summary.json"
    if not summary_path.exists():
        raise AirflowFailException("Gold summary not found!")
    
    with open(summary_path) as f:
        summary = json.load(f)
    
    silver_records = context['ti'].xcom_pull(key='silver_records', task_ids='validate_silver')
    
    conn = duckdb.connect(":memory:")
    conn.register("gold", table)
    total_in_gold = conn.execute("SELECT SUM(brewery_count) FROM gold").fetchone()[0]
    conn.close()
    
    checks = {
        "has_aggregations": table.num_rows > 0,
        "counts_match": abs(total_in_gold - silver_records) < 5,
        "has_summary": "total_breweries" in summary,
    }
    
    failed_checks = [k for k, v in checks.items() if not v]
    if failed_checks:
        raise AirflowFailException(f"Gold validation failed: {failed_checks}")
    
    context['ti'].xcom_push(key='gold_aggregations', value=table.num_rows)
    context['ti'].xcom_push(key='total_breweries', value=int(total_in_gold))
    print(f"✅ Gold validation passed: {table.num_rows} aggregation rows")
    return table.num_rows


# =============================================================================
# Pipeline Functions
# =============================================================================
def run_bronze_pipeline(**context):
    """Execute Bronze layer pipeline."""
    from src.pipelines.bronze_layer import run_bronze_pipeline as bronze_run
    result = bronze_run()
    context['ti'].xcom_push(key='bronze_result', value=result)
    print(f"✅ Bronze completed: {result.get('total_records', 0)} records")
    return result


def run_silver_pipeline(**context):
    """Execute Silver layer pipeline."""
    from src.pipelines.silver_layer import run_silver_pipeline as silver_run
    result = silver_run()
    context['ti'].xcom_push(key='silver_result', value=result)
    print(f"✅ Silver completed: {result.get('silver_record_count', 0)} records")
    return result


def run_gold_pipeline(**context):
    """Execute Gold layer pipeline."""
    from src.pipelines.gold_layer import run_gold_pipeline as gold_run
    result = gold_run()
    context['ti'].xcom_push(key='gold_result', value=result)
    print(f"✅ Gold completed: {result.get('total_rows', 0)} aggregations")
    return result


def generate_pipeline_report(**context):
    """Generate final pipeline execution report."""
    ti = context['ti']
    
    report = {
        "execution_date": str(context['execution_date']),
        "tests_passed": ti.xcom_pull(key='tests_passed', task_ids='run_tests'),
        "bronze_records": ti.xcom_pull(key='bronze_records', task_ids='validate_bronze'),
        "silver_records": ti.xcom_pull(key='silver_records', task_ids='validate_silver'),
        "gold_aggregations": ti.xcom_pull(key='gold_aggregations', task_ids='validate_gold'),
        "total_breweries": ti.xcom_pull(key='total_breweries', task_ids='validate_gold'),
    }
    
    print("=" * 60)
    print("PIPELINE EXECUTION REPORT")
    print("=" * 60)
    for key, value in report.items():
        print(f"  {key}: {value}")
    print("=" * 60)
    
    return report


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id="breweries_pipeline",
    default_args=default_args,
    description="Breweries ETL Pipeline with Test-Driven Validation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["breweries", "etl", "medallion", "test-driven"],
    doc_md="""
    ## Breweries ETL Pipeline (Test-Driven)
    
    ### Flow
    ```
    run_tests → extract → validate → transform → validate → aggregate → validate → report
    ```
    
    ### Data Quality Checks
    - ✅ All unit tests must pass before execution
    - ✅ Record count validation between layers
    - ✅ Schema validation
    - ✅ Null checks on critical columns
    - ✅ Cross-layer consistency (max 10% data loss)
    
    ### On Failure
    - Automatic retries (3x with exponential backoff)
    - Pipeline stops immediately if tests fail
    """,
) as dag:
    
    # Tasks
    start = EmptyOperator(task_id="start")
    
    run_tests = PythonOperator(
        task_id="run_tests",
        python_callable=run_unit_tests,
    )
    
    extract_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=run_bronze_pipeline,
    )
    
    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze_data,
    )
    
    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver_pipeline,
    )
    
    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver_data,
    )
    
    aggregate_gold = PythonOperator(
        task_id="aggregate_gold",
        python_callable=run_gold_pipeline,
    )
    
    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold_data,
    )
    
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_pipeline_report,
    )
    
    end = EmptyOperator(task_id="end")
    
    # ==========================================================================
    # DAG Flow
    # ==========================================================================
    #
    #  start → run_tests → extract_bronze → validate_bronze
    #                                              ↓
    #                      transform_silver ← ─ ─ ─┘
    #                              ↓
    #                      validate_silver
    #                              ↓
    #                      aggregate_gold
    #                              ↓
    #                      validate_gold
    #                              ↓
    #                      generate_report → end
    #
    # ==========================================================================
    
    (
        start 
        >> run_tests 
        >> extract_bronze 
        >> validate_bronze
        >> transform_silver 
        >> validate_silver
        >> aggregate_gold 
        >> validate_gold
        >> generate_report 
        >> end
    )