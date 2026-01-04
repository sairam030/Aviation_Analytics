"""
Spark Structured Streaming - Flight Data Enrichment (SQL-Based)
Uses Spark SQL joins instead of UDFs to avoid serialization issues
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, lit,
    struct, to_json, current_timestamp,
    regexp_extract, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType
)


# =============================================================================
# ROUTE MAPPING DATA (Embedded)
# =============================================================================

# Indian Airlines
INDIAN_AIRLINES = {
    "IGO": {"name": "IndiGo", "iata": "6E", "icao": "IGO"},
    "AIC": {"name": "Air India", "iata": "AI", "icao": "AIC"},
    "VTI": {"name": "Vistara", "iata": "UK", "icao": "VTI"},
    "SEJ": {"name": "SpiceJet", "iata": "SG", "icao": "SEJ"},
    "AKJ": {"name": "Akasa Air", "iata": "QP", "icao": "AKJ"},
}

# Indian Airports
INDIAN_AIRPORTS = {
    "DEL": {"name": "Indira Gandhi International", "city": "Delhi", "lat": 28.5562, "lon": 77.1000},
    "BOM": {"name": "Chhatrapati Shivaji Maharaj", "city": "Mumbai", "lat": 19.0896, "lon": 72.8656},
    "BLR": {"name": "Kempegowda International", "city": "Bangalore", "lat": 13.1986, "lon": 77.7066},
    "MAA": {"name": "Chennai International", "city": "Chennai", "lat": 12.9941, "lon": 80.1709},
    "CCU": {"name": "Netaji Subhas Chandra Bose", "city": "Kolkata", "lat": 22.6547, "lon": 88.4467},
    "HYD": {"name": "Rajiv Gandhi International", "city": "Hyderabad", "lat": 17.2403, "lon": 78.4294},
    "COK": {"name": "Cochin International", "city": "Kochi", "lat": 10.1520, "lon": 76.4019},
    "GOI": {"name": "Goa International", "city": "Goa", "lat": 15.3808, "lon": 73.8314},
    "AMD": {"name": "Sardar Vallabhbhai Patel", "city": "Ahmedabad", "lat": 23.0772, "lon": 72.6347},
    "PNQ": {"name": "Pune Airport", "city": "Pune", "lat": 18.5822, "lon": 73.9197},
}

# Route Patterns
ROUTE_PATTERNS = {
    "IGO": [
        (100, 199, "DEL", "BOM"), (200, 299, "DEL", "BLR"), (300, 399, "DEL", "CCU"),
        (400, 499, "BOM", "BLR"), (500, 599, "BOM", "DEL"), (600, 699, "BLR", "DEL"),
        (700, 799, "DEL", "MAA"), (800, 899, "BOM", "CCU"), (900, 999, "DEL", "HYD"),
        (1000, 1099, "BLR", "BOM"), (1100, 1199, "HYD", "DEL"), (1200, 1299, "MAA", "DEL"),
        (1300, 1399, "CCU", "DEL"), (1400, 1499, "DEL", "COK"), (1500, 1599, "BOM", "HYD"),
        (2000, 2999, "DEL", "GOI"), (5000, 5999, "BOM", "MAA"), (6000, 6999, "BLR", "CCU"),
    ],
    "AIC": [
        (100, 199, "DEL", "BOM"), (200, 299, "DEL", "BLR"), (300, 399, "DEL", "MAA"),
        (400, 499, "DEL", "CCU"), (500, 599, "BOM", "DEL"), (600, 699, "BOM", "BLR"),
        (800, 899, "DEL", "HYD"), (900, 999, "BLR", "DEL"),
    ],
    "VTI": [
        (800, 899, "DEL", "BOM"), (810, 819, "DEL", "BLR"), (820, 829, "DEL", "HYD"),
        (830, 839, "DEL", "MAA"), (840, 849, "DEL", "CCU"), (900, 999, "BOM", "DEL"),
    ],
    "SEJ": [
        (100, 199, "DEL", "BOM"), (200, 299, "DEL", "GOI"), (300, 399, "DEL", "HYD"),
        (400, 499, "BOM", "DEL"), (500, 599, "BOM", "BLR"),
    ],
    "AKJ": [
        (100, 199, "BOM", "BLR"), (200, 299, "DEL", "BOM"),
        (300, 399, "BOM", "DEL"), (400, 499, "BLR", "DEL"),
    ],
}

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_RAW = "aviation-india-states"
KAFKA_TOPIC_ENRICHED = "aviation-enriched-states"

# =============================================================================
# SCHEMAS
# =============================================================================

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
    StructField("sensors", StringType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("ingestion_time", StringType(), True),
])


def create_spark_session():
    """Create Spark session with Kafka support."""
    return (SparkSession.builder
        .appName("FlightStreamEnrichment")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())


def create_route_mapping_df(spark):
    """Create route mapping DataFrame from embedded data."""
    rows = []
    for prefix, routes in ROUTE_PATTERNS.items():
        airline = INDIAN_AIRLINES.get(prefix, {})
        for min_num, max_num, origin, dest in routes:
            origin_info = INDIAN_AIRPORTS.get(origin, {})
            dest_info = INDIAN_AIRPORTS.get(dest, {})
            rows.append({
                "airline_prefix": prefix,
                "airline_name": airline.get("name", ""),
                "airline_iata": airline.get("iata", ""),
                "airline_icao": airline.get("icao", ""),
                "min_flight_num": min_num,
                "max_flight_num": max_num,
                "origin_code": origin,
                "origin_city": origin_info.get("city", ""),
                "origin_airport": origin_info.get("name", ""),
                "origin_lat": origin_info.get("lat"),
                "origin_lon": origin_info.get("lon"),
                "destination_code": dest,
                "destination_city": dest_info.get("city", ""),
                "destination_airport": dest_info.get("name", ""),
                "destination_lat": dest_info.get("lat"),
                "destination_lon": dest_info.get("lon"),
            })
    
    return spark.createDataFrame(rows)


def run_enrichment_stream():
    """Main streaming enrichment job."""
    print("=" * 60)
    print("Starting Spark Streaming Enrichment")
    print("=" * 60)
    
    spark = create_spark_session()
    
    print(f"✓ Spark session created")
    print(f"✓ Reading from: {KAFKA_TOPIC_RAW}")
    print(f"✓ Writing to: {KAFKA_TOPIC_ENRICHED}")
    
    print("[DEBUG] Creating route mapping table...")
    # Create route mapping lookup table
    route_mapping_df = create_route_mapping_df(spark)
    route_mapping_df.cache()  # Cache for performance
    print(f"✓ Route mapping table created ({route_mapping_df.count()} routes)")
    
    print("[DEBUG] Setting up Kafka input stream...")
    # Read from Kafka
    raw_stream = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
    
    print("✓ Connected to Kafka input stream")
    
    print("[DEBUG] Parsing JSON and extracting fields...")
    # Parse JSON
    flights = (raw_stream
        .select(from_json(col("value").cast("string"), FLIGHT_SCHEMA).alias("flight"))
        .select("flight.*"))
    
    # Extract airline prefix and flight number from callsign
    flights_with_extracted = flights.withColumn(
        "airline_prefix",
        when(col("callsign").rlike("^IGO"), lit("IGO"))
        .when(col("callsign").rlike("^AIC"), lit("AIC"))
        .when(col("callsign").rlike("^VTI"), lit("VTI"))
        .when(col("callsign").rlike("^SEJ"), lit("SEJ"))
        .when(col("callsign").rlike("^AKJ"), lit("AKJ"))
        .otherwise(lit(None))
    ).withColumn(
        "flight_number",
        regexp_extract(col("callsign"), r"(\d+)", 1).cast("int")
    )
    
    print("[DEBUG] Joining with route mapping...")
    # Join with route mapping
    enriched = flights_with_extracted.join(
        route_mapping_df,
        (flights_with_extracted.airline_prefix == route_mapping_df.airline_prefix) &
        (flights_with_extracted.flight_number >= route_mapping_df.min_flight_num) &
        (flights_with_extracted.flight_number <= route_mapping_df.max_flight_num),
        "left"
    ).select(
        # Original fields
        flights_with_extracted["*"],
        # Enriched fields (from route_mapping_df)
        route_mapping_df.airline_name,
        route_mapping_df.airline_iata,
        route_mapping_df.airline_icao,
        route_mapping_df.origin_code,
        route_mapping_df.origin_city,
        route_mapping_df.origin_airport,
        route_mapping_df.origin_lat,
        route_mapping_df.origin_lon,
        route_mapping_df.destination_code,
        route_mapping_df.destination_city,
        route_mapping_df.destination_airport,
        route_mapping_df.destination_lat,
        route_mapping_df.destination_lon
    )
    
    print("[DEBUG] Setting up output stream to Kafka...")
    # Convert to JSON and write to Kafka
    output_stream = (enriched
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_ENRICHED)
        .option("checkpointLocation", "/tmp/spark-checkpoint-enrichment")
        .outputMode("append")
        .start())
    
    print("=" * 60)
    print("✓ Streaming query started successfully!")
    print("✓ Enriching flights with route information...")
    print(f"✓ Query ID: {output_stream.id}")
    print("=" * 60)
    
    # Wait for termination
    print("[DEBUG] Waiting for stream termination...")
    output_stream.awaitTermination()


if __name__ == "__main__":
    try:
        run_enrichment_stream()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
