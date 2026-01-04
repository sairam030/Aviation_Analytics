"""
Extract India-related flight data from the raw states data.
"""
import os
import glob
import tarfile
import gzip
import io
from datetime import datetime
from typing import List, Generator, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, lit, 
    year, month, dayofmonth, hour,
    count, sum as spark_sum, avg,
    regexp_extract, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, LongType, TimestampType
)

import src.batch.config as config


def get_directory_size(path: str) -> int:
    """Calculate total size of all files in a directory recursively."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)
    return total_size


# Indian airline callsign prefixes (from config)
INDIAN_AIRLINE_PREFIXES = config.INDIAN_AIRLINE_PREFIXES


def get_states_schema() -> StructType:
    """Define the schema for states data."""
    return StructType([
        StructField("time", LongType(), True),
        StructField("icao24", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("vertrate", DoubleType(), True),
        StructField("callsign", StringType(), True),
        StructField("onground", BooleanType(), True),
        StructField("alert", BooleanType(), True),
        StructField("spi", BooleanType(), True),
        StructField("squawk", StringType(), True),
        StructField("baroaltitude", DoubleType(), True),
        StructField("geoaltitude", DoubleType(), True),
        StructField("lastposupdate", DoubleType(), True),
        StructField("lastcontact", DoubleType(), True),
    ])


def find_all_tar_files(base_path: str) -> List[str]:
    """
    Recursively find all .tar files in the states directory.
    
    Structure: {base_path}/{year}/{month}/{date}/{hour}/states_{date}-{hour}.csv.tar
    """
    pattern = os.path.join(base_path, "**", "*.tar")
    tar_files = glob.glob(pattern, recursive=True)
    return sorted(tar_files)


# Removed: extract_csv_from_tar() - not used anywhere


def extract_tar_to_temp(tar_path: str, temp_dir: str) -> str:
    """
    Extract tar file to temp directory and return path to CSV file.
    More efficient than loading into memory.
    """
    import tempfile
    import shutil
    
    try:
        with tarfile.open(tar_path, 'r') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.csv.gz'):
                    # Extract to temp directory
                    tar.extract(member, temp_dir)
                    return os.path.join(temp_dir, member.name)
    except Exception as e:
        print(f"Error extracting {tar_path}: {e}")
        return None
    return None


def process_tar_files_to_parquet(
    spark: SparkSession,
    tar_files: List[str],
    output_path: str,
    batch_size: int = 5,
    max_batches: int = None
) -> bool:
    """
    Process tar files in batches and save as parquet.
    
    Args:
        spark: SparkSession
        tar_files: List of tar file paths
        output_path: Output path for parquet files
        batch_size: Number of tar files to process per batch
        max_batches: Stop after this many batches (None = process all)
    
    Returns:
        True if max_batches reached, False if all files processed
    """
    import tempfile
    import shutil
    
    schema = get_states_schema()
    total_files = len(tar_files)
    total_batches = (total_files + batch_size - 1) // batch_size
    
    if max_batches:
        print(f"Processing {total_files} tar files... (Max batches: {max_batches})")
    else:
        print(f"Processing {total_files} tar files... (All batches)")
    
    total_records = 0
    
    for i in range(0, total_files, batch_size):
        batch_files = tar_files[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        # Check if we've hit max_batches limit
        if max_batches and batch_num > max_batches:
            print(f"\n✅ MAX BATCHES REACHED: {max_batches} batches")
            print(f"   Total records: {total_records:,}")
            return True
        
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
        
        # Create temp directory for this batch
        temp_dir = tempfile.mkdtemp(prefix="aviation_batch_")
        extracted_files = []
        
        try:
            # Extract all files in batch to temp directory
            for idx, tar_path in enumerate(batch_files):
                print(f"  Extracting file {idx+1}/{len(batch_files)}: {os.path.basename(tar_path)}")
                csv_path = extract_tar_to_temp(tar_path, temp_dir)
                if csv_path:
                    extracted_files.append(csv_path)
            
            if not extracted_files:
                print(f"  Batch {batch_num}: No files extracted, skipping...")
                continue
            
            print(f"  Reading {len(extracted_files)} CSV files with Spark...")
            
            # Let Spark read the gzipped CSV files directly (much more efficient)
            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(extracted_files)
            
            # Apply India filters
            df_filtered = filter_india_data(df)
            
            # Count filtered records
            count = df_filtered.count()
            total_records += count
            print(f"  Batch {batch_num}: {count:,} India-related records (Total: {total_records:,})")
            
            if count > 0:
                # Save to parquet (append mode)
                df_filtered.write \
                    .mode("append") \
                    .partitionBy("year", "month") \
                    .parquet(output_path)
            
        finally:
            # Clean up temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"  Cleaned up temp directory")
    
    print(f"\n✅ ALL FILES PROCESSED: {total_files} files in {total_batches} batches")
    print(f"   Total records: {total_records:,}")
    return False


def filter_india_data(df: DataFrame) -> DataFrame:
    """
    Filter DataFrame to include only India-related flight data.
    
    Filtering criteria:
    1. Flights within India's bounding box
    2. Indian airline callsigns
    """
    # Clean callsign
    df = df.withColumn("callsign_clean", trim(upper(col("callsign"))))
    
    # Build callsign filter for Indian airlines
    callsign_conditions = None
    for prefix in INDIAN_AIRLINE_PREFIXES:
        condition = col("callsign_clean").startswith(prefix)
        if callsign_conditions is None:
            callsign_conditions = condition
        else:
            callsign_conditions = callsign_conditions | condition
    
    # Geographic filter (India bounding box)
    geo_filter = (
        (col("lat") >= config.INDIA_LAT_MIN) & 
        (col("lat") <= config.INDIA_LAT_MAX) &
        (col("lon") >= config.INDIA_LON_MIN) & 
        (col("lon") <= config.INDIA_LON_MAX)
    )
    
    # Combined filter: in India OR Indian airline
    combined_filter = geo_filter | callsign_conditions
    
    # Apply filter
    df_filtered = df.filter(combined_filter)
    
    # Add partitioning columns from timestamp
    df_filtered = df_filtered \
        .withColumn("year", year((col("time")).cast("timestamp"))) \
        .withColumn("month", month((col("time")).cast("timestamp"))) \
        .withColumn("day", dayofmonth((col("time")).cast("timestamp")))
    
    return df_filtered.drop("callsign_clean")


def sample_data_to_target_size(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    target_size_gb: float = 20.0
) -> None:
    """
    Sample data to achieve target size in GB.
    
    This reads the filtered parquet data and samples it down
    to approximately the target size.
    """
    df = spark.read.parquet(input_path)
    
    # Get current size estimate (rough calculation)
    total_count = df.count()
    
    # Estimate current size based on average row size (~200 bytes for states data)
    estimated_bytes_per_row = 200
    current_size_gb = (total_count * estimated_bytes_per_row) / (1024**3)
    
    print(f"Current data: {total_count:,} rows, ~{current_size_gb:.2f} GB")
    
    if current_size_gb <= target_size_gb:
        print(f"Data already under target size ({target_size_gb} GB), no sampling needed")
        df.write.mode("overwrite").parquet(output_path)
        return
    
    # Calculate sampling fraction
    sample_fraction = target_size_gb / current_size_gb
    print(f"Sampling fraction: {sample_fraction:.4f}")
    
    # Sample and save
    df_sampled = df.sample(fraction=sample_fraction, seed=42)
    sampled_count = df_sampled.count()
    sampled_size_gb = (sampled_count * estimated_bytes_per_row) / (1024**3)
    
    print(f"Sampled data: {sampled_count:,} rows, ~{sampled_size_gb:.2f} GB")
    
    df_sampled.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)


# Removed: main() function - not used, DAGs call functions directly
