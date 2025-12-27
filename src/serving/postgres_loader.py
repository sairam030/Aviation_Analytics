"""
PostgreSQL Loader - Merge batch and speed layer data into PostgreSQL
With incremental file tracking to only load new data
"""
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, max as spark_max, min as spark_min, count, avg, 
    sum as spark_sum, lit, current_timestamp, date_format,
    hour, to_date, expr, input_file_name
)
from pyspark.sql.types import *
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json

from src.serving.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    SILVER_BUCKET,
    FLIGHT_STATES_TABLE,
    FLIGHT_SUMMARY_TABLE,
    AIRLINE_STATS_TABLE,
    AIRPORT_STATS_TABLE,
    HOURLY_STATS_TABLE,
)

# Table to track processed files
PROCESSED_FILES_TABLE = "processed_files"


def create_spark_session() -> SparkSession:
    """Create Spark session with PostgreSQL and S3A support."""
    return SparkSession.builder \
        .appName("Aviation Serving Layer") \
        .config("spark.jars.packages", 
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def get_postgres_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def create_database_and_tables():
    """Create PostgreSQL database and all tables if they don't exist."""
    try:
        # First connect to postgres database to create aviation_db
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database="postgres",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{POSTGRES_DB}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {POSTGRES_DB}")
            print(f"✓ Created database: {POSTGRES_DB}")
        else:
            print(f"✓ Database exists: {POSTGRES_DB}")
        
        cursor.close()
        conn.close()
        
        # Now connect to aviation_db and create tables
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Create processed files tracking table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {PROCESSED_FILES_TABLE} (
                id SERIAL PRIMARY KEY,
                file_path VARCHAR(500) UNIQUE NOT NULL,
                layer VARCHAR(50) NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                record_count INTEGER,
                file_size_bytes BIGINT
            )
        """)
        
        # Create index on file_path for faster lookups
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_processed_files_path 
            ON {PROCESSED_FILES_TABLE} (file_path)
        """)
        
        # Create unified flight_states table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {FLIGHT_STATES_TABLE} (
                id SERIAL,
                -- Core identifiers
                icao24 VARCHAR(10),
                callsign VARCHAR(20),
                data_source VARCHAR(10),
                
                -- Timestamps
                timestamp BIGINT,
                last_contact BIGINT,
                time_position BIGINT,
                
                -- Position
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                baro_altitude DOUBLE PRECISION,
                geo_altitude DOUBLE PRECISION,
                on_ground BOOLEAN,
                
                -- Flight dynamics
                velocity DOUBLE PRECISION,
                true_track DOUBLE PRECISION,
                vertical_rate DOUBLE PRECISION,
                
                -- Transponder
                squawk VARCHAR(10),
                spi BOOLEAN,
                position_source INTEGER,
                
                -- Route info
                origin_code VARCHAR(10),
                origin_city VARCHAR(100),
                destination_code VARCHAR(10),
                destination_city VARCHAR(100),
                route VARCHAR(500),
                
                -- Aircraft info
                aircraft_model VARCHAR(100),
                aircraft_type VARCHAR(50),
                airline_name VARCHAR(100),
                airline_icao VARCHAR(10),
                
                -- Origin country
                origin_country VARCHAR(100),
                
                -- Time partitions
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                
                -- Metadata
                ingestion_time VARCHAR(50),
                first_seen VARCHAR(50),
                last_seen VARCHAR(50),
                position_count INTEGER,
                
                -- For time-series queries
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for common queries
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_icao24 
            ON {FLIGHT_STATES_TABLE} (icao24)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_callsign 
            ON {FLIGHT_STATES_TABLE} (callsign)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_timestamp 
            ON {FLIGHT_STATES_TABLE} (timestamp)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_source 
            ON {FLIGHT_STATES_TABLE} (data_source)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_date 
            ON {FLIGHT_STATES_TABLE} (year, month, day, hour)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_route 
            ON {FLIGHT_STATES_TABLE} (origin_code, destination_code)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_flight_states_airline 
            ON {FLIGHT_STATES_TABLE} (airline_name)
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✓ Tracking table ready: {PROCESSED_FILES_TABLE}")
        print(f"✓ Flight states table ready: {FLIGHT_STATES_TABLE}")
        
    except Exception as e:
        print(f"⚠️ Database setup error: {e}")
        raise
        
    except Exception as e:
        print(f"⚠️ Database setup error: {e}")
        raise


def get_processed_files(layer: str = None) -> set:
    """Get set of already processed file paths."""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        if layer:
            cursor.execute(
                f"SELECT file_path FROM {PROCESSED_FILES_TABLE} WHERE layer = %s",
                (layer,)
            )
        else:
            cursor.execute(f"SELECT file_path FROM {PROCESSED_FILES_TABLE}")
        
        processed = {row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        
        return processed
    except Exception as e:
        print(f"⚠️ Error getting processed files: {e}")
        return set()


def mark_files_processed(file_paths: list, layer: str, record_counts: dict = None):
    """Mark files as processed in tracking table."""
    if not file_paths:
        return
    
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        for file_path in file_paths:
            count = record_counts.get(file_path, 0) if record_counts else 0
            cursor.execute(f"""
                INSERT INTO {PROCESSED_FILES_TABLE} (file_path, layer, record_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (file_path) DO UPDATE SET
                    processed_at = CURRENT_TIMESTAMP,
                    record_count = EXCLUDED.record_count
            """, (file_path, layer, count))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"  ✓ Marked {len(file_paths)} files as processed")
    except Exception as e:
        print(f"⚠️ Error marking files processed: {e}")


def list_parquet_files(spark: SparkSession, base_path: str) -> list:
    """List all parquet files in a path using Spark."""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
        hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(base_path),
            hadoop_conf
        )
        
        files = []
        
        def list_files_recursive(path):
            try:
                status = fs.listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path))
                for file_status in status:
                    file_path = file_status.getPath().toString()
                    if file_status.isDirectory():
                        list_files_recursive(file_path)
                    elif file_path.endswith('.parquet'):
                        files.append(file_path)
            except Exception:
                pass
        
        list_files_recursive(base_path)
        return files
    except Exception as e:
        print(f"  ⚠️ Error listing files: {e}")
        return []


def read_new_files_only(spark: SparkSession, base_path: str, layer: str, processed_files: set) -> tuple:
    """Read only new (unprocessed) parquet files."""
    all_files = list_parquet_files(spark, base_path)
    
    # Filter to only new files
    new_files = [f for f in all_files if f not in processed_files]
    
    if not new_files:
        print(f"  No new files to process in {layer}")
        return None, []
    
    print(f"  Found {len(new_files)} new files (out of {len(all_files)} total)")
    
    # Read only new files
    df = spark.read.parquet(*new_files)
    
    # Add source file column for tracking
    df = df.withColumn("_source_file", input_file_name())
    
    return df, new_files


def read_batch_layer_data(spark: SparkSession, processed_files: set, full_load: bool = False) -> tuple:
    """Read batch layer data from Silver bucket."""
    try:
        batch_path = f"s3a://{SILVER_BUCKET}/enriched_states"
        print(f"  Reading batch data from: {batch_path}")
        
        if full_load:
            # Full load - read everything
            df = spark.read.parquet(batch_path)
            all_files = list_parquet_files(spark, batch_path)
            print(f"  Batch layer (full): {df.count()} records from {len(all_files)} files")
            return df, all_files
        else:
            # Incremental load - only new files
            return read_new_files_only(spark, batch_path, "batch", processed_files)
        
    except Exception as e:
        print(f"  ⚠️ No batch data or error: {e}")
        return None, []


def read_speed_layer_data(spark: SparkSession, processed_files: set, full_load: bool = False) -> tuple:
    """Read speed layer data from Silver bucket."""
    try:
        speed_path = f"s3a://{SILVER_BUCKET}/speed_layer/enriched_flight_states"
        print(f"  Reading speed data from: {speed_path}")
        
        if full_load:
            # Full load - read everything
            df = spark.read.parquet(speed_path)
            all_files = list_parquet_files(spark, speed_path)
            print(f"  Speed layer (full): {df.count()} records from {len(all_files)} files")
            return df, all_files
        else:
            # Incremental load - only new files
            return read_new_files_only(spark, speed_path, "speed", processed_files)
        
    except Exception as e:
        print(f"  ⚠️ No speed data or error: {e}")
        return None, []


def normalize_batch_data(df: DataFrame) -> DataFrame:
    """Normalize batch layer data to unified schema."""
    from pyspark.sql.functions import col, lit, when, to_timestamp
    
    return df.select(
        # Core identifiers
        col("icao24"),
        col("callsign"),
        lit("batch").alias("data_source"),
        
        # Timestamp
        col("time").alias("timestamp"),
        col("lastcontact").cast("long").alias("last_contact"),
        col("lastposupdate").cast("long").alias("time_position"),
        
        # Position
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        col("baroaltitude").alias("baro_altitude"),
        col("geoaltitude").alias("geo_altitude"),
        col("onground").alias("on_ground"),
        
        # Flight dynamics
        col("velocity"),
        col("heading").alias("true_track"),
        col("vertrate").alias("vertical_rate"),
        
        # Transponder
        col("squawk"),
        col("spi"),
        lit(None).cast("int").alias("position_source"),
        
        # Route info from batch
        col("departure_airport").alias("origin_code"),
        lit(None).cast("string").alias("origin_city"),
        col("arrival_airport").alias("destination_code"),
        lit(None).cast("string").alias("destination_city"),
        col("route"),
        
        # Aircraft info
        col("aircraft_model"),
        col("aircraft_type"),
        lit(None).cast("string").alias("airline_name"),
        lit(None).cast("string").alias("airline_icao"),
        
        # Origin country (not in batch, use empty)
        lit(None).cast("string").alias("origin_country"),
        
        # Time partitions
        col("year"),
        col("month"),
        col("day"),
        lit(None).cast("int").alias("hour"),
        
        # Metadata
        lit(None).cast("string").alias("ingestion_time"),
        lit(None).cast("string").alias("first_seen"),
        lit(None).cast("string").alias("last_seen"),
        lit(1).alias("position_count"),
    )


def normalize_speed_data(df: DataFrame) -> DataFrame:
    """Normalize speed layer data to unified schema."""
    from pyspark.sql.functions import col, lit
    
    return df.select(
        # Core identifiers
        col("icao24"),
        col("callsign"),
        lit("speed").alias("data_source"),
        
        # Timestamp
        col("time_position").alias("timestamp"),
        col("last_contact"),
        col("time_position"),
        
        # Position
        col("latitude"),
        col("longitude"),
        col("baro_altitude"),
        col("geo_altitude"),
        col("on_ground"),
        
        # Flight dynamics
        col("velocity"),
        col("true_track"),
        col("vertical_rate"),
        
        # Transponder
        col("squawk"),
        col("spi"),
        col("position_source"),
        
        # Route info
        col("origin_code"),
        col("origin_city"),
        col("destination_code"),
        col("destination_city"),
        lit(None).cast("string").alias("route"),
        
        # Aircraft info
        lit(None).cast("string").alias("aircraft_model"),
        lit(None).cast("string").alias("aircraft_type"),
        col("airline_name"),
        col("airline_icao"),
        
        # Origin country
        col("origin_country"),
        
        # Time partitions
        col("year"),
        col("month"),
        col("day"),
        col("hour"),
        
        # Metadata
        col("ingestion_time"),
        col("first_seen"),
        col("last_seen"),
        col("position_count"),
    )


def merge_dataframes(batch_df: DataFrame, speed_df: DataFrame) -> DataFrame:
    """Merge batch and speed layer data with normalized schema."""
    
    normalized_batch = None
    normalized_speed = None
    
    if batch_df is not None:
        print("  Normalizing batch layer data...")
        normalized_batch = normalize_batch_data(batch_df)
        print(f"    Batch: {normalized_batch.count()} records")
        
    if speed_df is not None:
        print("  Normalizing speed layer data...")
        normalized_speed = normalize_speed_data(speed_df)
        print(f"    Speed: {normalized_speed.count()} records")
    
    if normalized_batch is not None and normalized_speed is not None:
        print("  Merging normalized data...")
        merged = normalized_batch.union(normalized_speed)
        print(f"    Total: {merged.count()} records")
        return merged
    elif normalized_batch is not None:
        return normalized_batch
    elif normalized_speed is not None:
        return normalized_speed
    else:
        print("  ⚠️ No data available")
        return None


def load_to_postgres(df: DataFrame, table_name: str, mode: str = "append"):
    """Load DataFrame to PostgreSQL."""
    if df is None or df.count() == 0:
        print(f"  ⚠️ No data to load to {table_name}")
        return 0
    
    # Remove internal tracking columns before loading
    columns_to_drop = [c for c in df.columns if c.startswith('_')]
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
    
    print(f"  Loading to PostgreSQL table: {table_name} (mode: {mode})")
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    record_count = df.count()
    
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()
    
    print(f"  ✓ Loaded {record_count:,} records to {table_name}")
    return record_count


def create_flight_summary(df: DataFrame) -> DataFrame:
    """Create summary statistics per flight."""
    if df is None:
        return None
    
    # Remove internal columns
    columns_to_drop = [c for c in df.columns if c.startswith('_')]
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
    
    agg_cols = [
        count("*").alias("position_updates"),
    ]
    
    # Add aggregations for columns that exist
    if "first_seen" in df.columns:
        agg_cols.append(spark_min("first_seen").alias("first_seen"))
    if "last_seen" in df.columns:
        agg_cols.append(spark_max("last_seen").alias("last_seen"))
    if "baro_altitude" in df.columns:
        agg_cols.append(avg("baro_altitude").alias("avg_altitude"))
        agg_cols.append(spark_max("baro_altitude").alias("max_altitude"))
    if "velocity" in df.columns:
        agg_cols.append(avg("velocity").alias("avg_velocity"))
        agg_cols.append(spark_max("velocity").alias("max_velocity"))
    if "airline_name" in df.columns:
        agg_cols.append(spark_max("airline_name").alias("airline_name"))
    if "origin_code" in df.columns:
        agg_cols.append(spark_max("origin_code").alias("origin_code"))
    if "destination_code" in df.columns:
        agg_cols.append(spark_max("destination_code").alias("destination_code"))
    
    group_cols = ["icao24"]
    if "callsign" in df.columns:
        group_cols.append("callsign")
    if "origin_country" in df.columns:
        group_cols.append("origin_country")
    
    summary = df.groupBy(*group_cols).agg(*agg_cols)
    
    return summary


def create_airline_stats(df: DataFrame) -> DataFrame:
    """Create airline statistics."""
    if df is None or "airline_name" not in df.columns:
        return None
    
    stats = df.filter(col("airline_name").isNotNull()) \
        .groupBy("airline_name") \
        .agg(
            count("icao24").alias("total_flights"),
            avg("baro_altitude").alias("avg_altitude") if "baro_altitude" in df.columns else lit(None).alias("avg_altitude"),
            avg("velocity").alias("avg_velocity") if "velocity" in df.columns else lit(None).alias("avg_velocity"),
        ) \
        .orderBy(col("total_flights").desc())
    
    return stats


def create_airport_stats(df: DataFrame) -> DataFrame:
    """Create airport statistics."""
    if df is None:
        return None
    
    has_origin = "origin_code" in df.columns
    has_dest = "destination_code" in df.columns
    
    if not has_origin and not has_dest:
        return None
    
    stats_list = []
    
    if has_origin:
        origins = df.filter(col("origin_code").isNotNull()) \
            .groupBy("origin_code") \
            .agg(
                count("icao24").alias("departures"),
                spark_max("origin_city").alias("city") if "origin_city" in df.columns else lit(None).alias("city")
            ) \
            .withColumnRenamed("origin_code", "airport_code") \
            .withColumn("arrivals", lit(0))
        stats_list.append(origins)
    
    if has_dest:
        dests = df.filter(col("destination_code").isNotNull()) \
            .groupBy("destination_code") \
            .agg(
                count("icao24").alias("arrivals"),
                spark_max("destination_city").alias("city") if "destination_city" in df.columns else lit(None).alias("city")
            ) \
            .withColumnRenamed("destination_code", "airport_code") \
            .withColumn("departures", lit(0))
        stats_list.append(dests)
    
    if not stats_list:
        return None
    
    # Combine and aggregate
    combined = stats_list[0]
    for s in stats_list[1:]:
        combined = combined.union(s)
    
    stats = combined.groupBy("airport_code", "city") \
        .agg(
            spark_sum("departures").alias("departures"),
            spark_sum("arrivals").alias("arrivals")
        ) \
        .withColumn("total_flights", col("departures") + col("arrivals")) \
        .orderBy(col("total_flights").desc())
    
    return stats


def create_hourly_stats(df: DataFrame) -> DataFrame:
    """Create hourly statistics."""
    if df is None or "last_contact" not in df.columns:
        return None
    
    hourly = df.withColumn("hour_timestamp", 
                           expr("from_unixtime(last_contact)")) \
        .withColumn("date", to_date(col("hour_timestamp"))) \
        .withColumn("hour", hour(col("hour_timestamp"))) \
        .groupBy("date", "hour") \
        .agg(
            count("icao24").alias("total_flights"),
            avg("baro_altitude").alias("avg_altitude") if "baro_altitude" in df.columns else lit(None).alias("avg_altitude"),
            avg("velocity").alias("avg_velocity") if "velocity" in df.columns else lit(None).alias("avg_velocity"),
        ) \
        .orderBy("date", "hour")
    
    return hourly


def initial_full_load():
    """Perform initial full load of all data to PostgreSQL."""
    print("="*60)
    print("AVIATION SERVING LAYER - INITIAL FULL LOAD")
    print("="*60)
    
    # Setup database and tables
    create_database_and_tables()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read ALL data (full load)
        print("\n1. Reading ALL data from Silver layer...")
        batch_df, batch_files = read_batch_layer_data(spark, set(), full_load=True)
        speed_df, speed_files = read_speed_layer_data(spark, set(), full_load=True)
        
        # Merge data
        print("\n2. Merging data...")
        merged_df = merge_dataframes(batch_df, speed_df)
        
        if merged_df is None:
            print("\n⚠️ No data to load")
            return
        
        total_records = merged_df.count()
        print(f"  Total merged records: {total_records:,}")
        
        # Load main flight states table (OVERWRITE for initial load)
        print(f"\n3. Loading to {FLIGHT_STATES_TABLE} (overwrite)...")
        load_to_postgres(merged_df, FLIGHT_STATES_TABLE, mode="overwrite")
        
        # Create and load summary tables
        print(f"\n4. Creating {FLIGHT_SUMMARY_TABLE}...")
        summary_df = create_flight_summary(merged_df)
        if summary_df:
            load_to_postgres(summary_df, FLIGHT_SUMMARY_TABLE, mode="overwrite")
        
        print(f"\n5. Creating {AIRLINE_STATS_TABLE}...")
        airline_df = create_airline_stats(merged_df)
        if airline_df:
            load_to_postgres(airline_df, AIRLINE_STATS_TABLE, mode="overwrite")
        
        print(f"\n6. Creating {AIRPORT_STATS_TABLE}...")
        airport_df = create_airport_stats(merged_df)
        if airport_df:
            load_to_postgres(airport_df, AIRPORT_STATS_TABLE, mode="overwrite")
        
        print(f"\n7. Creating {HOURLY_STATS_TABLE}...")
        hourly_df = create_hourly_stats(merged_df)
        if hourly_df:
            load_to_postgres(hourly_df, HOURLY_STATS_TABLE, mode="overwrite")
        
        # Mark all files as processed
        print("\n8. Marking files as processed...")
        mark_files_processed(batch_files, "batch")
        mark_files_processed(speed_files, "speed")
        
        print("\n" + "="*60)
        print("INITIAL FULL LOAD COMPLETE")
        print("="*60)
        print(f"  Total Records: {total_records:,}")
        print(f"  Batch Files: {len(batch_files)}")
        print(f"  Speed Files: {len(speed_files)}")
        print("="*60)
        
    finally:
        spark.stop()


def incremental_load():
    """Perform incremental load of only new data to PostgreSQL."""
    print("="*60)
    print("AVIATION SERVING LAYER - INCREMENTAL LOAD")
    print("="*60)
    
    # Setup database and tables
    create_database_and_tables()
    
    # Get already processed files
    processed_files = get_processed_files()
    print(f"  Already processed: {len(processed_files)} files")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read only NEW data (incremental)
        print("\n1. Checking for new data in Silver layer...")
        batch_df, batch_files = read_batch_layer_data(spark, processed_files, full_load=False)
        speed_df, speed_files = read_speed_layer_data(spark, processed_files, full_load=False)
        
        if not batch_files and not speed_files:
            print("\n✓ No new files to process")
            return
        
        # Merge new data
        print("\n2. Merging new data...")
        merged_df = merge_dataframes(batch_df, speed_df)
        
        if merged_df is None:
            print("\n⚠️ No data to load")
            return
        
        new_records = merged_df.count()
        print(f"  New records: {new_records:,}")
        
        # APPEND new data to flight states table
        print(f"\n3. Appending to {FLIGHT_STATES_TABLE}...")
        load_to_postgres(merged_df, FLIGHT_STATES_TABLE, mode="append")
        
        # Rebuild aggregation tables (need full data for correct aggregations)
        print("\n4. Rebuilding aggregation tables...")
        
        # Read all flight states from PostgreSQL
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        full_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", FLIGHT_STATES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print(f"  Total records in PostgreSQL: {full_df.count():,}")
        
        # Rebuild summary tables
        print(f"\n5. Rebuilding {FLIGHT_SUMMARY_TABLE}...")
        summary_df = create_flight_summary(full_df)
        if summary_df:
            load_to_postgres(summary_df, FLIGHT_SUMMARY_TABLE, mode="overwrite")
        
        print(f"\n6. Rebuilding {AIRLINE_STATS_TABLE}...")
        airline_df = create_airline_stats(full_df)
        if airline_df:
            load_to_postgres(airline_df, AIRLINE_STATS_TABLE, mode="overwrite")
        
        print(f"\n7. Rebuilding {AIRPORT_STATS_TABLE}...")
        airport_df = create_airport_stats(full_df)
        if airport_df:
            load_to_postgres(airport_df, AIRPORT_STATS_TABLE, mode="overwrite")
        
        print(f"\n8. Rebuilding {HOURLY_STATS_TABLE}...")
        hourly_df = create_hourly_stats(full_df)
        if hourly_df:
            load_to_postgres(hourly_df, HOURLY_STATS_TABLE, mode="overwrite")
        
        # Mark new files as processed
        print("\n9. Marking new files as processed...")
        mark_files_processed(batch_files, "batch")
        mark_files_processed(speed_files, "speed")
        
        print("\n" + "="*60)
        print("INCREMENTAL LOAD COMPLETE")
        print("="*60)
        print(f"  New Records Added: {new_records:,}")
        print(f"  New Batch Files: {len(batch_files)}")
        print(f"  New Speed Files: {len(speed_files)}")
        print("="*60)
        
    finally:
        spark.stop()


def load_serving_layer(full_load: bool = False):
    """Main entry point for loading serving layer."""
    if full_load:
        initial_full_load()
    else:
        incremental_load()


if __name__ == "__main__":
    import sys
    
    # Check command line args
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        load_serving_layer(full_load=True)
    else:
        load_serving_layer(full_load=False)
