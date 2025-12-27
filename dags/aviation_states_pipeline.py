"""
Aviation States Pipeline
Bronze -> Silver architecture for India flight states data
Writes directly to MinIO via S3A
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import functions from src/batch
from src.batch.bronze import extract_states
from src.batch.silver import check_mapping_exists, enrich_states
from src.batch.minio_utils import create_all_buckets


default_args = {
    'owner': 'aviation-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),
}

with DAG(
    dag_id='aviation_states_pipeline',
    default_args=default_args,
    description='Extract India states (Bronze) and enrich with routes (Silver)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aviation', 'states', 'bronze', 'silver', 'spark'],
    max_active_runs=1,
) as dag:

    # ==========================================================================
    # SETUP TASK
    # ==========================================================================

    create_buckets = PythonOperator(
        task_id='create_minio_buckets',
        python_callable=create_all_buckets,
        doc_md="""
        ### Create MinIO Buckets
        - aviation-bronze
        - aviation-silver
        - aviation-gold
        - aviation-mapping
        """
    )

    # ==========================================================================
    # BRONZE LAYER
    # ==========================================================================

    extract_bronze = PythonOperator(
        task_id='extract_bronze_states',
        python_callable=extract_states,
        doc_md="""
        ### Extract Bronze States
        - Reads raw tar files from states directory
        - Filters for India region + Indian airlines
        - Saves partitioned Parquet directly to MinIO (s3a://)
        """
    )

    # ==========================================================================
    # SILVER LAYER
    # ==========================================================================

    check_mapping = PythonOperator(
        task_id='check_mapping_exists',
        python_callable=check_mapping_exists,
        doc_md="""
        ### Check Mapping Table
        - Verifies flight mapping exists in MinIO
        - Required for Silver enrichment
        """
    )

    enrich_silver = PythonOperator(
        task_id='enrich_silver_states',
        python_callable=enrich_states,
        doc_md="""
        ### Enrich Silver States
        - Reads Bronze from MinIO
        - Joins with flight mapping
        - Adds route info (departure/arrival airports)
        - Saves to MinIO Silver bucket
        """
    )

    # ==========================================================================
    # SUMMARY
    # ==========================================================================

    def print_summary():
        print("\n" + "="*60)
        print("✅ AVIATION STATES PIPELINE COMPLETE")
        print("="*60)
        print("📦 BRONZE: s3a://aviation-bronze/states")
        print("🥈 SILVER: s3a://aviation-silver/enriched_states")

    summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
        trigger_rule='all_done',
        doc_md="""
        ### Pipeline Summary
        - Data written directly to MinIO via S3A
        """
    )

    # ==========================================================================
    # DEPENDENCIES
    # ==========================================================================

    create_buckets >> extract_bronze >> check_mapping >> enrich_silver >> summary
