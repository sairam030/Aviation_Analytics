"""
Analytics Serving Layer - Load data from Silver buckets to PostgreSQL
Creates dimensional and fact tables for analytics
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, concat_ws, coalesce, trim, upper,
    max as spark_max, min as spark_min, avg, sum as spark_sum, count as spark_count, countDistinct,
    unix_timestamp, round as spark_round, when, abs as spark_abs,
    first, last, stddev, hour, date_format
)
from pyspark.sql.window import Window
from datetime import datetime
import src.batch.config as config


def log_progress(message: str, level: str = "INFO"):
    """Log progress with timestamp"""
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "PROCESSING": "⚙️"}
    icon = icons.get(level, "•")
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"  [{timestamp}] {icon} {message}")


def get_spark_with_postgres(app_name: str = "AnalyticsLoader") -> SparkSession:
    """Create Spark session with S3A and PostgreSQL support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "50")
        # PostgreSQL JDBC driver
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.1.jar")
        # S3A/MinIO
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def check_is_first_run() -> bool:
    """Removed - use DAG param 'load_batch_data' instead."""
    pass


def load_batch_silver() -> dict:
    """Load batch layer silver data (enriched states from batch processing)."""
    spark = get_spark_with_postgres("LoadBatchSilver")
    try:
        df = spark.read.parquet(config.SILVER_PATH)
        count = df.count()
        spark.stop()
        print(f"✓ Batch silver: {count:,} records")
        return {"source": "batch", "records": count, "df": df}
    except Exception as e:
        spark.stop()
        print(f"⚠ No batch silver data: {e}")
        return {"source": "batch", "records": 0, "df": None}


def load_speed_silver() -> dict:
    """Load speed layer silver data (real-time enriched states from Kafka)."""
    spark = get_spark_with_postgres("LoadSpeedSilver")
    try:
        # Speed layer writes to different bucket path
        speed_path = "s3a://aviation-silver"
        df = spark.read.parquet(speed_path)
        count = df.count()
        spark.stop()
        print(f"✓ Speed silver: {count:,} records")
        return {"source": "speed", "records": count, "df": df}
    except Exception as e:
        spark.stop()
        print(f"⚠ No speed silver data: {e}")
        return {"source": "speed", "records": 0, "df": None}


def merge_to_gold(load_batch_data: bool = True) -> dict:
    """
    Merge batch and speed data to gold bucket.
    
    Args:
        load_batch_data: If True, load both batch + speed. If False, only speed.
    """
    print("\n" + "="*80)
    print("📦 MERGING TO GOLD BUCKET")
    print("="*80)
    log_progress(f"Mode: {'FULL LOAD (Batch + Speed)' if load_batch_data else 'INCREMENTAL (Speed Only)'}", "INFO")
    
    spark = get_spark_with_postgres("MergeToGold")
    
    try:
        gold_master_path = f"{config.GOLD_PATH}/master_flights"
        log_progress(f"Target path: {gold_master_path}", "INFO")
        
        if load_batch_data:
            # Load both batch and speed
            log_progress("Loading batch layer data...", "PROCESSING")
            df_batch = spark.read.parquet(config.SILVER_PATH)
            batch_count = df_batch.count()
            log_progress(f"Batch: {batch_count:,} records", "SUCCESS")
            
            try:
                log_progress("Loading speed layer data...", "PROCESSING")
                df_speed = spark.read.parquet("s3a://aviation-silver/speed_enriched")
                speed_count = df_speed.count()
                log_progress(f"Speed: {speed_count:,} records", "SUCCESS")
                df_combined = df_batch.unionByName(df_speed, allowMissingColumns=True)
            except Exception as e:
                log_progress(f"Speed layer not found: {str(e)}", "WARNING")
                log_progress("Using batch data only", "INFO")
                df_combined = df_batch
                speed_count = 0
            
            # Save to gold
            log_progress("Writing to Gold bucket...", "PROCESSING")
            df_combined.write.mode("overwrite").partitionBy("year", "month").parquet(gold_master_path)
            total = batch_count + speed_count
            log_progress(f"Gold master created: {total:,} records", "SUCCESS")
            
        else:
            # Append only speed layer
            try:
                log_progress("Loading speed layer data...", "PROCESSING")
                df_speed = spark.read.parquet("s3a://aviation-silver/speed_enriched")
                speed_count = df_speed.count()
                log_progress(f"Speed: {speed_count:,} records", "SUCCESS")
                
                log_progress("Appending to Gold bucket...", "PROCESSING")
                df_speed.write.mode("append").partitionBy("year", "month").parquet(gold_master_path)
                log_progress(f"Gold master updated: +{speed_count:,} records", "SUCCESS")
                total = speed_count
            except Exception as e:
                log_progress(f"No new speed data: {str(e)}", "WARNING")
                total = 0
        
        print("="*80 + "\n")
        return {"gold_records": total}
        
    finally:
        spark.stop()


def create_fact_flight_history() -> dict:
    """
    Create FACT_FLIGHT_HISTORY table.
    Grain: One row per unique flight (Airline + Flight Number + Date)
    """
    print("\n" + "="*80)
    print("📊 CREATING FACT_FLIGHT_HISTORY TABLE")
    print("="*80)
    
    spark = get_spark_with_postgres("FactFlightHistory")
    
    try:
        log_progress("Reading from Gold bucket...", "PROCESSING")
        # Read gold master
        df = spark.read.parquet(f"{config.GOLD_PATH}/master_flights")
        
        # Extract flight info
        df = df.withColumn("flight_date", to_date(col("time").cast("timestamp")))
        df = df.withColumn("callsign_clean", trim(upper(col("callsign"))))
        
        # Create flight_id
        df = df.withColumn("flight_id", concat_ws("_", col("flight_date"), col("callsign_clean")))
        
        # Aggregate per flight (using only columns that exist)
        fact_table = df.groupBy(
            "flight_id", "flight_date", "callsign_clean", "icao24",
            "departure_airport", "arrival_airport", "route"
        ).agg(
            # Time metrics
            spark_min("time").alias("first_seen"),
            spark_max("time").alias("last_seen"),
            spark_min("lastposupdate").alias("first_position_update"),
            spark_max("lastposupdate").alias("last_position_update"),
            spark_min("lastcontact").alias("first_contact"),
            spark_max("lastcontact").alias("last_contact"),
            
            # Velocity metrics
            spark_max("velocity").alias("max_velocity"),
            spark_min("velocity").alias("min_velocity"),
            avg("velocity").alias("avg_velocity"),
            stddev("velocity").alias("stddev_velocity"),
            
            # Altitude metrics
            spark_max("baroaltitude").alias("max_baro_altitude"),
            spark_min("baroaltitude").alias("min_baro_altitude"),
            avg("baroaltitude").alias("avg_baro_altitude"),
            spark_max("geoaltitude").alias("max_geo_altitude"),
            spark_min("geoaltitude").alias("min_geo_altitude"),
            avg("geoaltitude").alias("avg_geo_altitude"),
            avg(when(spark_abs(col("vertrate")) < 1, col("baroaltitude"))).alias("avg_cruise_altitude"),
            
            # Vertical rate metrics
            spark_max("vertrate").alias("max_vertical_rate"),
            spark_min("vertrate").alias("min_vertical_rate"),
            avg("vertrate").alias("avg_vertical_rate"),
            
            # Heading metrics
            avg("heading").alias("avg_heading"),
            
            # Position metrics
            spark_min("lat").alias("min_latitude"),
            spark_max("lat").alias("max_latitude"),
            avg("lat").alias("avg_latitude"),
            spark_min("lon").alias("min_longitude"),
            spark_max("lon").alias("max_longitude"),
            avg("lon").alias("avg_longitude"),
            
            # Status flags
            spark_sum(when(col("onground"), 1).otherwise(0)).alias("onground_count"),
            spark_sum(when(col("alert"), 1).otherwise(0)).alias("alert_count"),
            spark_sum(when(col("spi"), 1).otherwise(0)).alias("spi_count"),
            
            # Aircraft info
            first("aircraft_model").alias("aircraft_model"),
            first("aircraft_type").alias("aircraft_type"),
            
            # Data quality
            spark_count("*").alias("total_data_points"),
            countDistinct("squawk").alias("unique_squawk_codes")
        )
        
        # Calculate duration
        fact_table = fact_table.withColumn(
            "duration_seconds",
            col("last_seen") - col("first_seen")
        ).withColumn(
            "duration_minutes",
            spark_round(col("duration_seconds") / 60, 2)
        ).withColumn(
            "duration_hours",
            spark_round(col("duration_seconds") / 3600, 2)
        ).withColumn(
            "airborne_percentage",
            spark_round((1 - col("onground_count") / col("total_data_points")) * 100, 2)
        )
        
        # Write to PostgreSQL
        fact_table.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "fact_flight_history") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        count = fact_table.count()
        print(f"✅ FACT_FLIGHT_HISTORY: {count:,} records")
        
        return {"table": "fact_flight_history", "records": count}
        
    finally:
        spark.stop()


def create_dim_route_analytics() -> dict:
    """
    Create DIM_ROUTE_ANALYTICS table.
    Grain: One row per Route (Origin-Destination pair)
    """
    print("\n" + "="*80)
    print("📈 CREATING DIM_ROUTE_ANALYTICS TABLE")
    print("="*80)
    log_progress("Reading from FACT_FLIGHT_HISTORY table...", "PROCESSING")
    
    spark = get_spark_with_postgres("DimRouteAnalytics")
    
    try:
        # Read from fact table (already aggregated)
        fact_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "fact_flight_history") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # Create route_id
        fact_df = fact_df.withColumn(
            "route_id",
            coalesce(col("route"), concat_ws("-", col("departure_airport"), col("arrival_airport")))
        )
        
        # Aggregate by route (using only columns that exist)
        route_table = fact_df.groupBy(
            "route_id", "departure_airport", "arrival_airport"
        ).agg(
            avg("duration_minutes").alias("avg_flight_time_minutes"),
            avg("max_velocity").alias("avg_max_velocity"),
            avg("avg_cruise_altitude").alias("avg_cruise_altitude"),
            spark_count("*").alias("total_flights"),
            countDistinct("callsign_clean").alias("unique_airlines")
        )
        
        # Write to PostgreSQL
        route_table.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "dim_route_analytics") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        count = route_table.count()
        print(f"✅ DIM_ROUTE_ANALYTICS: {count:,} records")
        
        return {"table": "dim_route_analytics", "records": count}
        
    finally:
        spark.stop()


def create_dim_aircraft_utilization() -> dict:
    """
    Create DIM_AIRCRAFT_UTILIZATION table.
    Grain: One row per Aircraft (icao24) per Day
    """
    print("\n" + "="*80)
    print("✈️  CREATING DIM_AIRCRAFT_UTILIZATION TABLE")
    print("="*80)
    log_progress("Reading from Gold bucket...", "PROCESSING")
    
    spark = get_spark_with_postgres("DimAircraftUtilization")
    
    try:
        # Read gold master
        df = spark.read.parquet(f"{config.GOLD_PATH}/master_flights")
        
        # Add date column
        df = df.withColumn("flight_date", to_date(col("time").cast("timestamp")))
        
        # Aggregate by aircraft and date
        aircraft_table = df.groupBy("icao24", "flight_date").agg(
            first("callsign").alias("callsign"),
            spark_sum(when(~col("onground"), 1).otherwise(0)).alias("airborne_data_points"),
            countDistinct("callsign").alias("number_of_flights"),
            spark_max("vertrate").alias("max_vertical_rate"),
            spark_min("vertrate").alias("min_vertical_rate"),
            last("lat").alias("last_reported_lat"),
            last("lon").alias("last_reported_lon"),
            avg("velocity").alias("avg_velocity"),
            spark_max("baroaltitude").alias("max_altitude")
        )
        
        # Estimate flight hours (airborne points * 10 seconds / 3600)
        aircraft_table = aircraft_table.withColumn(
            "estimated_flight_hours",
            spark_round(col("airborne_data_points") * 10 / 3600, 2)
        )
        
        # Write to PostgreSQL
        aircraft_table.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "dim_aircraft_utilization") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        count = aircraft_table.count()
        print(f"✅ DIM_AIRCRAFT_UTILIZATION: {count:,} records")
        
        return {"table": "dim_aircraft_utilization", "records": count}
        
    finally:
        spark.stop()


def create_dim_airspace_heatmap() -> dict:
    """
    Create DIM_AIRSPACE_HEATMAP table.
    Grain: Spatial Grid (Lat/Lon rounded) + Hour
    """
    print("\n" + "="*80)
    print("🗺️  CREATING DIM_AIRSPACE_HEATMAP TABLE")
    print("="*80)
    log_progress("Reading from Gold bucket...", "PROCESSING")
    
    spark = get_spark_with_postgres("DimAirspaceHeatmap")
    
    try:
        # Read gold master
        df = spark.read.parquet(f"{config.GOLD_PATH}/master_flights")
        
        # Create spatial bins (round to 0.5 degrees ~ 55km)
        df = df.withColumn("lat_bin", spark_round(col("lat") * 2) / 2)
        df = df.withColumn("lon_bin", spark_round(col("lon") * 2) / 2)
        df = df.withColumn("hour_of_day", hour(col("time").cast("timestamp")))
        
        # Create geo_hash
        df = df.withColumn("geo_hash", concat_ws(",", col("lat_bin"), col("lon_bin")))
        
        # Aggregate by spatial grid and hour
        heatmap_table = df.groupBy("geo_hash", "lat_bin", "lon_bin", "hour_of_day").agg(
            countDistinct("icao24").alias("traffic_density"),
            avg("baroaltitude").alias("avg_altitude"),
            stddev("velocity").alias("velocity_variance"),
            avg("velocity").alias("avg_velocity"),
            spark_count("*").alias("data_points")
        )
        
        # Write to PostgreSQL
        heatmap_table.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "dim_airspace_heatmap") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        count = heatmap_table.count()
        print(f"✅ DIM_AIRSPACE_HEATMAP: {count:,} records")
        
        return {"table": "dim_airspace_heatmap", "records": count}
        
    finally:
        spark.stop()


def create_analytics_database() -> dict:
    """Create analytics database if it doesn't exist."""
    import psycopg2
    
    print("Creating analytics database...")
    
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host="postgres",
            database="postgres",
            user="airflow",
            password="airflow"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'analytics'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE analytics")
            print("✓ Created analytics database")
        else:
            print("✓ Analytics database already exists")
        
        cursor.close()
        conn.close()
        
        return {"status": "success"}
        
    except Exception as e:
        print(f"⚠ Error creating database: {e}")
        return {"status": "error", "message": str(e)}


def create_raw_telemetry_table() -> dict:
    """
    Create RAW_TELEMETRY table with ALL individual data points.
    This is the complete raw data with no aggregation.
    Useful for detailed analysis and machine learning.
    """
    print("="*60)
    print("CREATING RAW_TELEMETRY TABLE")
    print("="*60)
    
    spark = get_spark_with_postgres("RawTelemetry")
    
    try:
        # Read gold master - all raw data points
        df = spark.read.parquet(f"{config.GOLD_PATH}/master_flights")
        
        # Add derived columns
        df = df.withColumn("timestamp", col("time").cast("timestamp"))
        df = df.withColumn("flight_date", to_date(col("timestamp")))
        df = df.withColumn("hour_of_day", hour(col("timestamp")))
        df = df.withColumn("callsign_clean", trim(upper(col("callsign"))))
        
        # Select all columns in clean order
        telemetry_table = df.select(
            # Identifiers
            col("icao24"),
            col("callsign"),
            col("callsign_clean"),
            col("squawk"),
            
            # Timestamps
            col("time"),
            col("timestamp"),
            col("flight_date"),
            col("hour_of_day"),
            col("lastposupdate"),
            col("lastcontact"),
            
            # Position
            col("lat"),
            col("lon"),
            col("baroaltitude"),
            col("geoaltitude"),
            
            # Motion
            col("velocity"),
            col("heading"),
            col("vertrate"),
            
            # Status
            col("onground"),
            col("alert"),
            col("spi"),
            
            # Route & Aircraft (from enrichment)
            col("departure_airport"),
            col("arrival_airport"),
            col("route"),
            col("aircraft_model"),
            col("aircraft_type"),
            
            # Partitioning
            col("year"),
            col("month"),
            col("day")
        )
        
        # Sample for PostgreSQL (full data stays in gold bucket)
        # Store last 7 days or max 1M records for quick analysis
        sample_size = min(1000000, telemetry_table.count())
        if sample_size < telemetry_table.count():
            fraction = sample_size / telemetry_table.count()
            telemetry_sample = telemetry_table.sample(fraction=fraction, seed=42)
            print(f"  Sampling {sample_size:,} / {telemetry_table.count():,} records for PostgreSQL")
        else:
            telemetry_sample = telemetry_table
            print(f"  Loading all {sample_size:,} records to PostgreSQL")
        
        # Write to PostgreSQL
        telemetry_sample.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/analytics") \
            .option("dbtable", "raw_telemetry") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        count = telemetry_sample.count()
        print(f"✅ RAW_TELEMETRY: {count:,} records")
        print(f"   Full data available in: {config.GOLD_PATH}/master_flights")
        
        return {"table": "raw_telemetry", "records": count}
        
    finally:
        spark.stop()
