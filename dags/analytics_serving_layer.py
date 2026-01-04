"""
Analytics Serving Layer DAG
Loads data from Silver buckets to PostgreSQL analytics tables
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param

from src.serving.analytics_loader import (
    create_analytics_database,
    merge_to_gold,
    create_fact_flight_history,
    create_dim_route_analytics,
    create_dim_aircraft_utilization,
    create_dim_airspace_heatmap,
    create_raw_telemetry_table
)

# Import metrics utilities
from src.utils.metrics_utils import show_pipeline_metrics


default_args = {
    'owner': 'aviation-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}


with DAG(
    dag_id='analytics_serving_layer',
    default_args=default_args,
    description='Load data from Silver buckets to PostgreSQL analytics tables',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aviation', 'serving', 'analytics', 'postgres'],
    max_active_runs=1,
    params={
        "load_batch_data": Param(
            default=False,
            type="boolean",
            description="Load batch layer data? Set True for first run or full reload, False for incremental (speed only)"
        )
    }
) as dag:

    # ==========================================================================
    # SETUP
    # ==========================================================================

    setup_db = PythonOperator(
        task_id='create_analytics_database',
        python_callable=create_analytics_database,
        doc_md="""
        ### Create Analytics Database
        - Creates 'analytics' database in PostgreSQL if not exists
        """
    )

    # ==========================================================================
    # GOLD LAYER - Master Copy
    # ==========================================================================

    def merge_based_on_param(**context):
        """Merge to gold: controlled by DAG param 'load_batch_data'."""
        load_batch = context['params'].get('load_batch_data', False)
        return merge_to_gold(load_batch_data=load_batch)

    merge_gold = PythonOperator(
        task_id='merge_to_gold_bucket',
        python_callable=merge_based_on_param,
        doc_md="""
        ### Merge to Gold Bucket
        - Param load_batch_data=True: Load batch + speed data (first run / full reload)
        - Param load_batch_data=False: Append only speed layer data (incremental)
        - Saves master copy to s3a://aviation-gold/master_flights
        """
    )

    # ==========================================================================
    # ANALYTICS TABLES
    # ==========================================================================

    create_fact = PythonOperator(
        task_id='create_fact_flight_history',
        python_callable=create_fact_flight_history,
        doc_md="""
        ### FACT_FLIGHT_HISTORY
        - Grain: One row per unique flight (Airline + Flight Number + Date)
        - Aggregates: max_velocity, avg_cruise_altitude, duration_minutes
        - Source: Gold master bucket
        """
    )

    create_route = PythonOperator(
        task_id='create_dim_route_analytics',
        python_callable=create_dim_route_analytics,
        doc_md="""
        ### DIM_ROUTE_ANALYTICS
        - Grain: One row per Route (Origin-Destination)
        - Metrics: avg_flight_time, total_flights, unique_airlines
        - Source: FACT_FLIGHT_HISTORY table
        """
    )

    create_aircraft = PythonOperator(
        task_id='create_dim_aircraft_utilization',
        python_callable=create_dim_aircraft_utilization,
        doc_md="""
        ### DIM_AIRCRAFT_UTILIZATION
        - Grain: One row per Aircraft (icao24) per Day
        - Metrics: flight_hours, number_of_flights, max_vertical_rate
        - Source: Gold master bucket
        """
    )

    create_heatmap = PythonOperator(
        task_id='create_dim_airspace_heatmap',
        python_callable=create_dim_airspace_heatmap,
        doc_md="""
        ### DIM_AIRSPACE_HEATMAP
        - Grain: Spatial Grid (0.5° bins) + Hour
        - Metrics: traffic_density, avg_altitude, velocity_variance
        - Source: Gold master bucket
        """
    )

    create_raw_telemetry = PythonOperator(
        task_id='create_raw_telemetry',
        python_callable=create_raw_telemetry_table,
        doc_md="""
        ### RAW_TELEMETRY
        - Grain: Individual data points (sample for PostgreSQL)
        - Contains: ALL fields from raw data
        - Source: Gold master bucket
        - Note: Full data remains in Gold bucket
        """
    )

    # ==========================================================================
    # COMPREHENSIVE METRICS
    # ==========================================================================

    def print_comprehensive_metrics(**context):
        """Show comprehensive pipeline metrics"""
        load_batch = context['params'].get('load_batch_data', False)
        mode = "FULL LOAD (Batch + Speed)" if load_batch else "INCREMENTAL (Speed Only)"
        
        result = show_pipeline_metrics(
            pipeline_name=f"ANALYTICS SERVING LAYER - {mode}",
            task_ids=[
                'create_analytics_database',
                'merge_to_gold_bucket',
                'create_raw_telemetry',
                'create_fact_flight_history',
                'create_dim_route_analytics',
                'create_dim_aircraft_utilization',
                'create_dim_airspace_heatmap'
            ],
            s3_buckets=[
                ("🥈 Silver Layer (Source)", "aviation-silver"),
                ("🥇 Gold Layer (Master)", "aviation-gold"),
            ],
            **context
        )
        
        # Additional Analytics-specific summary
        print("\n" + "=" * 80)
        print("📊 ANALYTICS TABLES SUMMARY")
        print("=" * 80)
        print("\n  PostgreSQL Database: analytics")
        print("\n  Tables Created:")
        print("    1. ✅ raw_telemetry         - Sampled raw data points")
        print("    2. ✅ fact_flight_history    - Flight master table")
        print("    3. ✅ dim_route_analytics    - Route performance metrics")
        print("    4. ✅ dim_aircraft_utilization - Aircraft tracking & utilization")
        print("    5. ✅ dim_airspace_heatmap   - Spatial & temporal traffic analysis")
        print("\n  💾 Full raw data available in: s3a://aviation-gold/master_flights")
        print("\n  🔄 Next scheduled run: In 6 hours")
        print("=" * 80 + "\n")
        
        return result

    metrics = PythonOperator(
        task_id='show_pipeline_metrics',
        python_callable=print_comprehensive_metrics,
        trigger_rule='all_done',
        doc_md="""
        ### Comprehensive Pipeline Metrics
        - Shows execution metrics for all tasks
        - Data layer statistics (Silver/Gold)
        - PostgreSQL analytics tables summary
        - Performance and health checks
        """
    )

    # ==========================================================================
    # DEPENDENCIES
    # ==========================================================================

    setup_db >> merge_gold >> create_raw_telemetry >> create_fact
    create_fact >> [create_route, create_aircraft, create_heatmap] >> metrics
