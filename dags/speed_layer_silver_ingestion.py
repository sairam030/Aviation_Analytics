"""
Speed Layer Silver Ingestion Pipeline
Hourly ingestion of enriched flight data from flight-tracker to Silver layer
Writes directly to MinIO via S3A
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import functions from src/speed
from src.speed.hourly_silver_ingestion import (
    check_flight_tracker_health,
    fetch_accumulated_data,
    persist_to_silver,
    clear_accumulator,
    clear_kafka_topics,
    print_summary,
)

# Import metrics utilities
from src.utils.metrics_utils import show_pipeline_metrics


default_args = {
    'owner': 'aviation-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='speed_layer_silver_ingestion',
    default_args=default_args,
    description='Hourly ingestion of enriched flight data to Silver layer',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['aviation', 'speed-layer', 'silver', 'hourly', 'enriched'],
    max_active_runs=1,
) as dag:

    # ==========================================================================
    # HEALTH CHECK
    # ==========================================================================

    check_health = PythonOperator(
        task_id='check_flight_tracker_health',
        python_callable=check_flight_tracker_health,
        doc_md="""
        ### Check Flight Tracker Health
        - Verifies flight-tracker service is running
        - Reports accumulated flight count
        - Reports WebSocket connections
        """
    )

    # ==========================================================================
    # FETCH ACCUMULATED DATA
    # ==========================================================================

    fetch_data = PythonOperator(
        task_id='fetch_accumulated_data',
        python_callable=fetch_accumulated_data,
        doc_md="""
        ### Fetch Accumulated Data
        - Retrieves enriched flight data from flight-tracker
        - Data includes route enrichment (origin/destination)
        - Pushes data to XCom for persistence
        """
    )

    # ==========================================================================
    # PERSIST TO SILVER
    # ==========================================================================

    persist_silver = PythonOperator(
        task_id='persist_to_silver',
        python_callable=persist_to_silver,
        doc_md="""
        ### Persist to Silver Layer
        - Creates Spark DataFrame with full schema
        - Writes to MinIO: s3a://aviation-silver/speed_layer/
        - Partitioned by year/month/day/hour
        """
    )

    # ==========================================================================
    # CLEAR ACCUMULATOR
    # ==========================================================================

    clear_state = PythonOperator(
        task_id='clear_accumulator',
        python_callable=clear_accumulator,
        doc_md="""
        ### Clear Accumulator
        - Clears flight-tracker accumulator after persistence
        - Prepares for next hour's data collection
        """
    )

    # ==========================================================================
    # CLEAR KAFKA TOPICS
    # ==========================================================================

    clear_kafka = PythonOperator(
        task_id='clear_kafka_topics',
        python_callable=clear_kafka_topics,
        doc_md="""
        ### Clear Kafka Topics
        - Deletes and recreates aviation-india-states (raw)
        - Deletes and recreates aviation-enriched-states (enriched)
        - Ensures clean slate for next hour
        - Producer and Spark will reconnect automatically
        """
    )

    # ==========================================================================
    # METRICS & SUMMARY
    # ==========================================================================

    def print_comprehensive_metrics(**context):
        """Show comprehensive pipeline metrics"""
        result = show_pipeline_metrics(
            pipeline_name="SPEED LAYER SILVER INGESTION",
            task_ids=[
                'check_flight_tracker_health',
                'fetch_accumulated_data',
                'persist_to_silver',
                'clear_accumulator',
                'clear_kafka_topics'
            ],
            s3_buckets=[
                ("🥈 Silver Layer (Speed Data)", "aviation-silver"),
            ],
            **context
        )
        
        # Also call the original summary for backward compatibility
        print_summary(**context)
        
        return result

    metrics = PythonOperator(
        task_id='show_pipeline_metrics',
        python_callable=print_comprehensive_metrics,
        trigger_rule='all_done',
        doc_md="""
        ### Pipeline Metrics & Summary
        - Shows comprehensive execution metrics
        - Data ingestion statistics
        - Kafka topic management status
        - System health checks
        """
    )

    # ==========================================================================
    # DEPENDENCIES
    # ==========================================================================

    check_health >> fetch_data >> persist_silver >> clear_state >> clear_kafka >> metrics
