"""
Stateful Flight Accumulator
- Consumes flight data from Kafka
- Maintains current state of all flights (update existing, add new)
- Hourly persists accumulated data to MinIO Bronze layer
"""
import json
import time
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType, ArrayType, TimestampType
)

from src.speed.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INDIA,
)
from src.speed.route_mapping import enrich_flight_data


# =============================================================================
# CONFIGURATION
# =============================================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "aviation-data"

# How often to persist to Bronze (seconds)
PERSIST_INTERVAL = 3600  # 1 hour

# Flight state storage
flight_state: Dict[str, Dict[str, Any]] = {}  # icao24 -> flight data
flight_history: Dict[str, list] = defaultdict(list)  # icao24 -> list of positions

# Locks for thread safety
state_lock = threading.Lock()
history_lock = threading.Lock()

# Stats
stats = {
    "total_messages": 0,
    "unique_flights": 0,
    "updates": 0,
    "new_flights": 0,
    "last_persist": None,
    "persists_count": 0,
}


# =============================================================================
# SPARK SESSION
# =============================================================================

_spark = None

def get_spark() -> SparkSession:
    """Get or create Spark session."""
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder
            .appName("FlightAccumulator")
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
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


# Schema for Bronze layer
FLIGHT_SCHEMA = StructType([
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
    StructField("flight_number", IntegerType(), True),
    StructField("origin_code", StringType(), True),
    StructField("origin_city", StringType(), True),
    StructField("destination_code", StringType(), True),
    StructField("destination_city", StringType(), True),
    # Accumulator metadata
    StructField("first_seen", StringType(), True),
    StructField("last_seen", StringType(), True),
    StructField("position_count", IntegerType(), True),
])


# =============================================================================
# FLIGHT STATE MANAGEMENT
# =============================================================================

def update_flight_state(flight: dict) -> str:
    """
    Update flight state - add new or update existing.
    Returns: 'new' or 'updated'
    """
    icao24 = flight.get('icao24')
    if not icao24:
        return None
    
    now = datetime.utcnow().isoformat()
    
    # Enrich with route data
    enriched = enrich_flight_data(flight.copy())
    
    with state_lock:
        if icao24 in flight_state:
            # Update existing flight
            existing = flight_state[icao24]
            
            # Keep first_seen, update last_seen
            enriched['first_seen'] = existing.get('first_seen', now)
            enriched['last_seen'] = now
            enriched['position_count'] = existing.get('position_count', 0) + 1
            
            flight_state[icao24] = enriched
            stats['updates'] += 1
            return 'updated'
        else:
            # New flight
            enriched['first_seen'] = now
            enriched['last_seen'] = now
            enriched['position_count'] = 1
            
            flight_state[icao24] = enriched
            stats['new_flights'] += 1
            stats['unique_flights'] = len(flight_state)
            return 'new'


def add_to_history(flight: dict):
    """Add flight position to history for trajectory tracking."""
    icao24 = flight.get('icao24')
    if not icao24:
        return
    
    # Store minimal position data
    position = {
        'timestamp': flight.get('ingestion_time'),
        'latitude': flight.get('latitude'),
        'longitude': flight.get('longitude'),
        'altitude': flight.get('baro_altitude'),
        'velocity': flight.get('velocity'),
        'heading': flight.get('true_track'),
    }
    
    with history_lock:
        flight_history[icao24].append(position)
        # Keep only last 100 positions per flight
        if len(flight_history[icao24]) > 100:
            flight_history[icao24] = flight_history[icao24][-100:]


def get_current_state() -> list:
    """Get current state of all flights."""
    with state_lock:
        return list(flight_state.values())


def clear_state():
    """Clear accumulated state after persistence."""
    global flight_state, flight_history
    with state_lock:
        flight_state = {}
    with history_lock:
        flight_history = defaultdict(list)
    stats['unique_flights'] = 0


# =============================================================================
# BRONZE LAYER PERSISTENCE
# =============================================================================

def persist_to_bronze():
    """Persist accumulated flight data to MinIO Bronze layer."""
    flights = get_current_state()
    
    if not flights:
        print("⚠️ No flights to persist")
        return
    
    try:
        spark = get_spark()
        
        # Create timestamp for partitioning
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        
        # Clean data for Spark (remove None sensors field, etc.)
        clean_flights = []
        for f in flights:
            clean = {k: v for k, v in f.items() if k != 'sensors'}
            clean_flights.append(clean)
        
        # Create DataFrame
        df = spark.createDataFrame(clean_flights, schema=FLIGHT_SCHEMA)
        
        # Output path with partitioning
        output_path = f"s3a://{MINIO_BUCKET}/bronze/flight_states/year={year}/month={month}/day={day}/hour={hour}"
        
        # Write as Parquet
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        stats['last_persist'] = now.isoformat()
        stats['persists_count'] += 1
        
        print(f"\n{'='*60}")
        print(f"✓ PERSISTED TO BRONZE LAYER")
        print(f"  Path: {output_path}")
        print(f"  Flights: {len(flights)}")
        print(f"  Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        
        # Don't clear state - keep tracking flights
        # Only reset position counts
        with state_lock:
            for icao24 in flight_state:
                flight_state[icao24]['position_count'] = 0
        
    except Exception as e:
        print(f"❌ Error persisting to Bronze: {e}")
        import traceback
        traceback.print_exc()


def persist_flight_history():
    """Persist flight trajectory history to Bronze layer."""
    with history_lock:
        if not flight_history:
            return
        
        history_data = []
        for icao24, positions in flight_history.items():
            for pos in positions:
                history_data.append({
                    'icao24': icao24,
                    **pos
                })
    
    if not history_data:
        return
    
    try:
        spark = get_spark()
        
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        
        df = spark.createDataFrame(history_data)
        
        output_path = f"s3a://{MINIO_BUCKET}/bronze/flight_trajectories/year={year}/month={month}/day={day}/hour={hour}"
        
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        print(f"✓ Saved {len(history_data)} trajectory points to {output_path}")
        
        # Clear history after saving
        with history_lock:
            flight_history.clear()
            
    except Exception as e:
        print(f"❌ Error persisting trajectories: {e}")


# =============================================================================
# SCHEDULED PERSISTENCE
# =============================================================================

def persistence_scheduler():
    """Background thread that persists data hourly."""
    print(f"📅 Persistence scheduler started (interval: {PERSIST_INTERVAL}s)")
    
    # Calculate time until next hour
    now = datetime.utcnow()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    initial_delay = (next_hour - now).total_seconds()
    
    print(f"   First persistence at: {next_hour.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"   (in {int(initial_delay)} seconds)")
    
    # Wait until next hour boundary
    time.sleep(initial_delay)
    
    while True:
        print(f"\n⏰ Hourly persistence triggered at {datetime.utcnow().isoformat()}")
        
        # Persist flight states
        persist_to_bronze()
        
        # Persist trajectories
        persist_flight_history()
        
        # Sleep until next hour
        time.sleep(PERSIST_INTERVAL)


# =============================================================================
# KAFKA CONSUMER
# =============================================================================

def consume_and_accumulate():
    """Consume from Kafka and accumulate flight state."""
    print("="*60)
    print("STATEFUL FLIGHT ACCUMULATOR")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_INDIA}")
    print(f"Persist Interval: {PERSIST_INTERVAL}s (hourly)")
    print("="*60)
    
    # Start persistence scheduler in background
    scheduler_thread = threading.Thread(target=persistence_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC_INDIA,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000,
    )
    
    print(f"✓ Connected to Kafka: {KAFKA_TOPIC_INDIA}")
    print("✓ Accumulating flight data...")
    print("-"*60)
    
    last_status = time.time()
    
    while True:
        for message in consumer:
            flight = message.value
            stats['total_messages'] += 1
            
            # Update state
            action = update_flight_state(flight)
            
            # Add to history for trajectory
            add_to_history(flight)
            
            # Print status every 30 seconds
            if time.time() - last_status > 30:
                print(f"📊 Status: {stats['unique_flights']} unique flights | "
                      f"{stats['total_messages']} messages | "
                      f"{stats['updates']} updates | "
                      f"{stats['new_flights']} new")
                last_status = time.time()


# =============================================================================
# MAIN
# =============================================================================

def run_accumulator():
    """Main entry point."""
    try:
        consume_and_accumulate()
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
        print("💾 Final persistence before exit...")
        persist_to_bronze()
        persist_flight_history()
        print("✓ Done")


if __name__ == "__main__":
    run_accumulator()
