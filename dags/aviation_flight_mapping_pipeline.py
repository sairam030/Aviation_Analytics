"""
Aviation Flight Mapping Pipeline
Creates callsign -> route mapping table from FlightsV5 data
Writes directly to MinIO via S3A
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import functions from src/batch
from src.batch.mapping import check_flights_data, create_mapping, upload_india_csv
from src.batch.minio_utils import create_bucket
import src.batch.config as config

# Import metrics utilities
from src.utils.metrics_utils import show_pipeline_metrics


default_args = {
    'owner': 'aviation-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='aviation_flight_mapping_pipeline',
    default_args=default_args,
    description='Create flight callsign to route mapping table from FlightsV5 data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aviation', 'mapping', 'spark'],
    max_active_runs=1,
) as dag:

    # Task 1: Check if FlightsV5 data exists
    check_data = PythonOperator(
        task_id='check_flights_data',
        python_callable=check_flights_data,
        doc_md="""
        ### Check FlightsV5 Data
        - Verifies FlightsV5 data is available
        - Counts CSV files in the directory
        """
    )

    # Task 2: Create MinIO bucket
    create_minio_bucket = PythonOperator(
        task_id='create_minio_bucket',
        python_callable=lambda: create_bucket(config.MAPPING_BUCKET),
        doc_md="""
        ### Create MinIO Bucket
        - Creates aviation-mapping bucket
        """
    )

    # Task 3: Create mapping (writes directly to MinIO)
    create_flight_mapping = PythonOperator(
        task_id='create_flight_mapping',
        python_callable=create_mapping,
        doc_md="""
        ### Create Flight Mapping Table
        - Reads FlightsV5 CSV files
        - Creates callsign -> route mapping
        - Saves Parquet directly to MinIO (s3a://)
        - Exports India routes CSV locally
        """
    )

    # Task 4: Upload India CSV to MinIO
    upload_csv = PythonOperator(
        task_id='upload_india_csv',
        python_callable=upload_india_csv,
        doc_md="""
        ### Upload India CSV
        - Uploads india_routes_merged.csv to MinIO
        """
    )

    # Task 5: Comprehensive Metrics
    def print_comprehensive_metrics(**context):
        """Show comprehensive pipeline metrics"""
        return show_pipeline_metrics(
            pipeline_name="AVIATION FLIGHT MAPPING PIPELINE",
            task_ids=[
                'check_flights_data',
                'create_minio_bucket',
                'create_flight_mapping',
                'upload_india_csv'
            ],
            s3_buckets=[
                ("🗺️  Flight Mapping Data", "aviation-mapping"),
            ],
            **context
        )

    summary = PythonOperator(
        task_id='show_pipeline_metrics',
        python_callable=print_comprehensive_metrics,
        trigger_rule='all_done',
        doc_md="""
        ### Pipeline Metrics & Summary
        - Shows comprehensive execution metrics
        - Mapping data statistics
        - Performance summary
        - Next steps: Run aviation_states_pipeline
        """
    )

    # Dependencies
    check_data >> create_minio_bucket >> create_flight_mapping >> upload_csv >> summary
