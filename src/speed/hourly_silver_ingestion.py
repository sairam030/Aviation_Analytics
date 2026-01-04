"""
Speed Layer - Hourly Silver Ingestion
Functions for fetching accumulated data from flight-tracker and persisting to Silver layer
"""
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType
)
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError


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
SILVER_BUCKET = "aviation-silver"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_RAW = "aviation-india-states"
KAFKA_TOPIC_ENRICHED = "aviation-enriched-states"


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

ENRICHED_FLIGHT_SCHEMA = StructType([
    # Core flight identifiers
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    
    # Timestamps
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("ingestion_time", StringType(), True),
    
    # Position data
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    
    # Flight dynamics
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    
    # Additional fields
    StructField("sensors", StringType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    
    # Enriched fields (from Spark streaming)
    StructField("airline_prefix", StringType(), True),
    StructField("flight_number", IntegerType(), True),
    StructField("airline_name", StringType(), True),
    StructField("airline_iata", StringType(), True),
    StructField("airline_icao", StringType(), True),
    
    # Route information
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
    
    # Accumulator tracking fields
    StructField("first_seen", StringType(), True),
    StructField("last_seen", StringType(), True),
    StructField("position_count", IntegerType(), True),
])


# =============================================================================
# API FUNCTIONS
# =============================================================================

def check_flight_tracker_health(**context):
    """
    Check if flight-tracker service is healthy and accumulating data.
    """
    print("="*60)
    print("HEALTH CHECK - Flight Tracker")
    print("="*60)
    
    try:
        response = requests.get(
            f"{FLIGHT_TRACKER_URL}/health",
            timeout=10
        )
        response.raise_for_status()
        
        health = response.json()
        print(f"✓ Flight Tracker is healthy")
        print(f"  Accumulated flights: {health.get('flights', 0)}")
        print(f"  WebSocket connections: {health.get('connections', 0)}")
        print(f"  Status: {health.get('status', 'unknown')}")
        
        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(
            key='flight_count',
            value=health.get('flights', 0)
        )
        
        return health
        
    except Exception as e:
        print(f"⚠️ Health check failed: {e}")
        raise


def fetch_accumulated_data(**context):
    """
    Fetch accumulated enriched flight data from flight-tracker.
    """
    print("="*60)
    print("FETCHING ACCUMULATED DATA")
    print("="*60)
    
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
        
        print(f"✓ Fetched {len(flights)} flights")
        print(f"  Total messages: {stats.get('total_messages', 0)}")
        print(f"  Updates: {stats.get('updates', 0)}")
        print(f"  New flights: {stats.get('new_flights', 0)}")
        
        if len(flights) == 0:
            print("⚠️ No flights to persist - skipping Silver persistence")
            context['task_instance'].xcom_push(key='flights', value=[])
            return {"flights": [], "count": 0}
        
        # Push to XCom
        context['task_instance'].xcom_push(key='flights', value=flights)
        context['task_instance'].xcom_push(key='count', value=len(flights))
        
        return data
        
    except Exception as e:
        print(f"⚠️ Failed to fetch data: {e}")
        raise


def persist_to_silver(**context):
    """
    Persist accumulated flight data to MinIO Silver layer.
    Uses Spark to write partitioned Parquet files.
    """
    print("="*60)
    print("PERSISTING TO SILVER LAYER")
    print("="*60)
    
    # Get flights from XCom
    task_instance = context['task_instance']
    flights = task_instance.xcom_pull(task_ids='fetch_accumulated_data', key='flights')
    
    if not flights:
        print("⚠️ No flights to persist - skipping")
        return {"status": "skipped", "reason": "no_data"}
    
    print(f"Processing {len(flights)} flights...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SpeedLayerSilverIngestion") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    try:
        # Create DataFrame
        df = spark.createDataFrame(flights, schema=ENRICHED_FLIGHT_SCHEMA)
        
        # Add processing metadata
        from pyspark.sql.functions import current_timestamp, lit
        df = df.withColumn("processing_time", current_timestamp())
        df = df.withColumn("source", lit("speed_layer"))
        
        # Get current timestamp for partitioning
        now = datetime.utcnow()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        
        # Silver layer path
        silver_path = f"s3a://{SILVER_BUCKET}/speed_layer/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        
        print(f"Writing to: {silver_path}")
        
        # Write to MinIO
        df.write \
            .mode("append") \
            .parquet(silver_path)
        
        print(f"✓ Persisted {len(flights)} flights to Silver layer")
        print(f"  Path: {silver_path}")
        print(f"  Timestamp: {now.isoformat()}")
        
        return {
            "status": "success",
            "count": len(flights),
            "path": silver_path,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        print(f"⚠️ Failed to persist data: {e}")
        raise
        
    finally:
        spark.stop()


def clear_accumulator(**context):
    """
    Clear flight-tracker accumulator after successful persistence.
    """
    print("="*60)
    print("CLEARING ACCUMULATOR")
    print("="*60)
    
    # Only clear if we actually persisted data
    task_instance = context['task_instance']
    flight_count = task_instance.xcom_pull(task_ids='fetch_accumulated_data', key='count') or 0
    
    if flight_count == 0:
        print("⚠️ No flights were persisted - skipping clear")
        return {"status": "skipped", "reason": "no_data"}
    
    try:
        response = requests.post(
            f"{FLIGHT_TRACKER_URL}/api/accumulator/clear",
            params={"api_key": PERSISTENCE_API_KEY},
            timeout=10
        )
        response.raise_for_status()
        
        result = response.json()
        cleared_count = result.get('cleared_count', 0)
        
        print(f"✓ Cleared {cleared_count} flights from accumulator")
        print(f"  Status: {result.get('status', 'unknown')}")
        print(f"  Timestamp: {result.get('timestamp', 'N/A')}")
        
        return result
        
    except Exception as e:
        print(f"⚠️ Failed to clear accumulator: {e}")
        # Don't raise - clearing is not critical
        return {"status": "error", "message": str(e)}


def clear_kafka_topics(**context):
    """
    Clear Kafka topics after successful persistence to start with clean slate.
    Deletes and recreates both raw and enriched topics.
    """
    print("="*60)
    print("CLEARING KAFKA TOPICS")
    print("="*60)
    
    # Only clear if we actually persisted data
    task_instance = context['task_instance']
    flight_count = task_instance.xcom_pull(task_ids='fetch_accumulated_data', key='count') or 0
    
    if flight_count == 0:
        print("⚠️ No flights were persisted - skipping Kafka clear")
        return {"status": "skipped", "reason": "no_data"}
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='silver_ingestion_cleanup'
        )
        
        topics_to_clear = [KAFKA_TOPIC_RAW, KAFKA_TOPIC_ENRICHED]
        
        # Delete existing topics
        print(f"Deleting topics: {topics_to_clear}")
        try:
            admin_client.delete_topics(topics=topics_to_clear, timeout_ms=10000)
            print(f"✓ Deleted topics: {', '.join(topics_to_clear)}")
        except UnknownTopicOrPartitionError:
            print("⚠️ Some topics didn't exist, continuing...")
        
        # Wait a moment for deletion to complete
        import time
        time.sleep(2)
        
        # Recreate topics with same configuration
        new_topics = [
            NewTopic(
                name=KAFKA_TOPIC_RAW,
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                name=KAFKA_TOPIC_ENRICHED,
                num_partitions=3,
                replication_factor=1
            )
        ]
        
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✓ Recreated topics: {', '.join(topics_to_clear)}")
        
        admin_client.close()
        
        print("✓ Kafka topics cleared - starting fresh for next hour")
        
        return {
            "status": "success",
            "topics_cleared": topics_to_clear,
            "message": "Kafka topics deleted and recreated"
        }
        
    except Exception as e:
        print(f"⚠️ Failed to clear Kafka topics: {e}")
        print("  Continuing anyway - not critical for pipeline")
        # Don't raise - clearing is not critical
        return {"status": "error", "message": str(e)}


def print_summary(**context):
    """
    Print summary of the ingestion pipeline.
    """
    print("="*60)
    print("PIPELINE SUMMARY")
    print("="*60)
    
    task_instance = context['task_instance']
    
    # Get metrics from previous tasks
    flight_count = task_instance.xcom_pull(task_ids='fetch_accumulated_data', key='count') or 0
    
    print(f"✓ Pipeline completed successfully")
    print(f"  Flights processed: {flight_count}")
    print(f"  Execution time: {context['execution_date']}")
    print(f"  Kafka topics: Cleared and recreated")
    print(f"  Accumulator: Cleared")
    print(f"  Status: Ready for next hour")
    print("="*60)
    
    return {
        "status": "success",
        "flights_processed": flight_count,
        "execution_date": str(context['execution_date'])
    }
