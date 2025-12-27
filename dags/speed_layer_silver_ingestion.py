"""
Airflow DAG - Hourly Silver Ingestion from Flight Tracker
Fetches enriched & accumulated flight data from flight-tracker service
Persists to MinIO Silver layer with route mapping and metadata
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
import requests


# =============================================================================
# CONFIGURATION
# =============================================================================

FLIGHT_TRACKER_HOST = "flight-tracker"
FLIGHT_TRACKER_PORT = 8050
FLIGHT_TRACKER_URL = f"http://{FLIGHT_TRACKER_HOST}:{FLIGHT_TRACKER_PORT}"
PERSISTENCE_API_KEY = "aviation-secret-key"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
# Use silver bucket directly (matches batch layer convention)
SILVER_BUCKET = "aviation-silver"


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'aviation',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'speed_layer_silver_ingestion',
    default_args=default_args,
    description='Hourly ingestion of enriched flight data to Silver layer',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['aviation', 'speed-layer', 'silver', 'hourly', 'enriched'],
)


# =============================================================================
# TASKS
# =============================================================================

def check_flight_tracker_health(**context):
    """Check if flight-tracker service is healthy."""
    try:
        response = requests.get(f"{FLIGHT_TRACKER_URL}/health", timeout=10)
        response.raise_for_status()
        health = response.json()
        print(f"✓ Flight tracker healthy: {health['flights']} flights accumulated")
        return True
    except Exception as e:
        print(f"❌ Flight tracker not healthy: {e}")
        raise


def fetch_accumulated_data(**context):
    """Fetch accumulated flight data from flight-tracker."""
    try:
        response = requests.get(
            f"{FLIGHT_TRACKER_URL}/api/accumulator/data",
            params={"api_key": PERSISTENCE_API_KEY},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        flights = data.get('flights', [])
        stats = data.get('stats', {})
        
        print(f"✓ Fetched {len(flights)} flights from accumulator")
        print(f"  Stats: {stats}")
        
        # Push to XCom for next task
        context['ti'].xcom_push(key='flight_data', value=flights)
        context['ti'].xcom_push(key='flight_count', value=len(flights))
        
        return len(flights)
        
    except Exception as e:
        print(f"❌ Failed to fetch accumulated data: {e}")
        raise


def persist_to_bronze(**context):
    """Persist flight data to MinIO Bronze layer using Spark."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        BooleanType, LongType, IntegerType
    )
    
    # Get data from XCom
    ti = context['ti']
    flights = ti.xcom_pull(key='flight_data', task_ids='fetch_accumulated_data')
    
    if not flights:
        print("⚠️ No flights to persist")
        return 0
    
    # Create Spark session
    spark = (SparkSession.builder
        .appName("SpeedLayerBronzeIngestion")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())
    
    try:
        # Schema for flight data
        schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", LongType(), True),
            StructField("last_contact", LongType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", IntegerType(), True),
            StructField("ingestion_time", StringType(), True),
            # Enriched fields
            StructField("airline_name", StringType(), True),
            StructField("airline_iata", StringType(), True),
            StructField("airline_icao", StringType(), True),
            StructField("airline_prefix", StringType(), True),
            StructField("flight_number", IntegerType(), True),
            StructField("origin_code", StringType(), True),
            StructField("origin_city", StringType(), True),
            StructField("origin_airport", StringType(), True),
            StructField("origin_lat", DoubleType(), True),
            StructField("origin_lon", DoubleType(), True),
            StructField("destination_code", StringType(), True),
            StructField("destination_city", StringType(), True),
            StructField("destination_airport", StringType(), True),
            StructField("destination_lat", DoubleType(), True),
            StructField("destination_lon", DoubleType(), True),
            # Accumulator metadata
            StructField("first_seen", StringType(), True),
            StructField("last_seen", StringType(), True),
            StructField("position_count", IntegerType(), True),
        ])
        
        # Clean data - remove fields not in schema and convert types
        clean_flights = []
        schema_fields = {f.name: f.dataType for f in schema.fields}
        
        for f in flights:
            clean = {}
            for k, v in f.items():
                if k in schema_fields:
                    # Convert int to float for DoubleType fields
                    if isinstance(schema_fields[k], DoubleType) and isinstance(v, int):
                        clean[k] = float(v)
                    elif v is not None:
                        clean[k] = v
                    else:
                        clean[k] = None
            clean_flights.append(clean)
        
        # Create DataFrame
        df = spark.createDataFrame(clean_flights, schema=schema)
        
        # Partition by date/hour
        execution_date = context['execution_date']
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")
        day = execution_date.strftime("%d")
        hour = execution_date.strftime("%H")
        
        output_path = f"s3a://{SILVER_BUCKET}/speed_layer/enriched_flight_states/year={year}/month={month}/day={day}/hour={hour}"
        
        # Write to MinIO Silver layer (data is already enriched)
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"✓ Persisted {len(flights)} enriched flights to Silver: {output_path}")
        
        # Push output path to XCom
        ti.xcom_push(key='silver_path', value=output_path)
        
        return len(flights)
        
    finally:
        spark.stop()


def clear_accumulator(**context):
    """Clear the flight-tracker accumulator after successful persistence."""
    ti = context['ti']
    flight_count = ti.xcom_pull(key='flight_count', task_ids='fetch_accumulated_data')
    
    if not flight_count or flight_count == 0:
        print("⚠️ No flights were persisted, skipping clear")
        return
    
    try:
        response = requests.post(
            f"{FLIGHT_TRACKER_URL}/api/accumulator/clear",
            params={"api_key": PERSISTENCE_API_KEY},
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        
        print(f"✓ Accumulator cleared: {result['cleared_count']} flights")
        print("✓ Ready for next hour's accumulation")
        
        return result['cleared_count']
        
    except Exception as e:
        print(f"❌ Failed to clear accumulator: {e}")
        raise


def log_summary(**context):
    """Log summary of the ingestion."""
    ti = context['ti']
    flight_count = ti.xcom_pull(key='flight_count', task_ids='fetch_accumulated_data')
    silver_path = ti.xcom_pull(key='silver_path', task_ids='persist_to_silver')
    
    print("="*60)
    print("SPEED LAYER SILVER INGESTION COMPLETE")
    print("="*60)
    print(f"  Enriched flights: {flight_count}")
    print(f"  Silver path: {silver_path}")
    print(f"  Execution time: {context['execution_date']}")
    print("="*60)


# =============================================================================
# TASK DEFINITIONS
# =============================================================================

check_health = PythonOperator(
    task_id='check_flight_tracker_health',
    python_callable=check_flight_tracker_health,
    dag=dag,
)

fetch_data = PythonOperator(
    task_id='fetch_accumulated_data',
    python_callable=fetch_accumulated_data,
    dag=dag,
)

persist_silver = PythonOperator(
    task_id='persist_to_silver',
    python_callable=persist_to_bronze,
    dag=dag,
)

clear_state = PythonOperator(
    task_id='clear_accumulator',
    python_callable=clear_accumulator,
    dag=dag,
)

log_result = PythonOperator(
    task_id='log_summary',
    python_callable=log_summary,
    dag=dag,
)


# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

check_health >> fetch_data >> persist_silver >> clear_state >> log_result
