"""Flight mapping functions - creates callsign -> route mapping from FlightsV5 data.
"""
import os
import shutil
from pyspark.sql.functions import col, trim, upper, count, first, concat, lit, coalesce
import src.batch.config as config
from src.batch.spark_utils import get_spark


def find_csv_files() -> list:
    """Find all CSV files in FlightsV5 directory."""
    csv_files = []
    for root, dirs, files in os.walk(config.FLIGHTS_V5_PATH):
        for f in files:
            if f.endswith('.csv') or f.endswith('.csv.gz'):
                csv_files.append(os.path.join(root, f))
    return csv_files


def check_flights_data() -> dict:
    """Check if FlightsV5 data is available."""
    print(f"Checking: {config.FLIGHTS_V5_PATH}")
    
    if not os.path.exists(config.FLIGHTS_V5_PATH):
        raise ValueError(f"FlightsV5 data not found at {config.FLIGHTS_V5_PATH}")
    
    csv_files = find_csv_files()
    if not csv_files:
        raise ValueError("No CSV files found")
    
    print(f"✓ Found {len(csv_files)} CSV files")
    return {"file_count": len(csv_files)}


def create_mapping() -> dict:
    """Create callsign -> route mapping table."""
    print("="*60)
    print("CREATING FLIGHT MAPPING TABLE")
    print(f"Output: {config.MAPPING_PATH}")
    print("="*60)
    
    spark = get_spark("FlightMapping")
    
    try:
        csv_files = find_csv_files()
        print(f"Found {len(csv_files)} CSV files")
        
        # Read data
        print("Reading FlightsV5 data...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_files)
        
        # Fix column names with leading spaces
        for old_col in df.columns:
            new_col = old_col.strip()
            if old_col != new_col:
                df = df.withColumnRenamed(old_col, new_col)
        
        total_records = df.count()
        print(f"  Total records: {total_records:,}")
        
        # Clean and prepare
        df = df.withColumn("callsign_clean", trim(upper(col("callsign"))))
        df = df.withColumn("dep_airport", coalesce(col("estdepartureairport"), col("airportofdeparture")))
        df = df.withColumn("arr_airport", coalesce(col("estarrivalairport"), col("airportofdestination")))
        
        # Filter valid
        df_valid = df.filter(
            (col("callsign_clean").isNotNull()) & 
            (col("callsign_clean") != "") &
            (col("dep_airport").isNotNull()) &
            (col("arr_airport").isNotNull())
        )
        print(f"  Valid records: {df_valid.count():,}")
        
        # Create mapping
        print("Creating callsign -> route mapping...")
        df_mapping = df_valid.groupBy("callsign_clean").agg(
            first("dep_airport").alias("departure_airport"),
            first("arr_airport").alias("arrival_airport"),
            first("model").alias("aircraft_model"),
            first("typecode").alias("aircraft_type"),
            count("*").alias("flight_count")
        )
        
        df_mapping = df_mapping.withColumn(
            "route", concat(col("departure_airport"), lit("->"), col("arrival_airport"))
        ).withColumnRenamed("callsign_clean", "callsign")
        
        # Filter India routes
        df_india = df_mapping.filter(
            (col("departure_airport").startswith("V")) |
            (col("arrival_airport").startswith("V"))
        )
        
        india_count = df_india.count()
        total_mapping = df_mapping.count()
        print(f"  Unique callsigns: {total_mapping:,}")
        print(f"  India routes: {india_count:,}")
        
        # Save parquet to MinIO
        df_mapping.write.mode("overwrite").parquet(config.MAPPING_PATH)
        
        # Export India CSV (temp local, then upload)
        india_csv_path = os.path.join(config.LOCAL_OUTPUT, "mapping", "india_routes_merged.csv")
        os.makedirs(os.path.dirname(india_csv_path), exist_ok=True)
        
        df_india.orderBy(col("flight_count").desc()).coalesce(1).write \
            .mode("overwrite").option("header", "true").csv(india_csv_path + "_tmp")
        
        # Rename part file
        tmp_dir = india_csv_path + "_tmp"
        for f in os.listdir(tmp_dir):
            if f.startswith("part-") and f.endswith(".csv"):
                shutil.move(os.path.join(tmp_dir, f), india_csv_path)
                break
        shutil.rmtree(tmp_dir, ignore_errors=True)
        
        print(f"\n✅ MAPPING CREATED")
        print(f"   Callsigns: {total_mapping:,}")
        print(f"   India routes: {india_count:,}")
        
        return {
            "total_records": total_records,
            "unique_callsigns": total_mapping,
            "india_routes": india_count
        }
        
    finally:
        spark.stop()


def upload_india_csv():
    """Upload India routes CSV to MinIO."""
    from src.batch.minio_utils import upload_file
    
    india_csv = os.path.join(config.LOCAL_OUTPUT, "mapping", "india_routes_merged.csv")
    if os.path.exists(india_csv):
        upload_file(india_csv, config.MAPPING_BUCKET, "india_routes_merged.csv")
        print("✓ Uploaded india_routes_merged.csv")
        return 1
    return 0
