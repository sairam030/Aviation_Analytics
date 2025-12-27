"""
Hourly Ingestion - Consumes Kafka data and writes to MinIO Bronze layer
This runs hourly to persist streaming data to the lakehouse
"""
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, ArrayType, IntegerType
)

import src.batch.config as batch_config
from src.speed.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_INDIA


# Schema for OpenSky state data
FLIGHT_STATE_SCHEMA = StructType([
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
    StructField("sensors", ArrayType(IntegerType()), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("ingestion_time", StringType(), True),
])

# Output path for streaming bronze data
STREAMING_BRONZE_PATH = f"s3a://{batch_config.BRONZE_BUCKET}/realtime"


def get_spark() -> SparkSession:
    """Create Spark session with Kafka and S3A support."""
    return (
        SparkSession.builder
        .appName("HourlyIngestion")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        # Kafka
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        # S3A/MinIO
        .config("spark.hadoop.fs.s3a.endpoint", batch_config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", batch_config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", batch_config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ingest_hourly() -> dict:
    """
    Ingest last hour of Kafka data to MinIO Bronze layer.
    Called by Airflow DAG every hour.
    """
    now = datetime.utcnow()
    hour_str = now.strftime("%Y-%m-%d-%H")
    
    print("="*60)
    print("HOURLY INGESTION: Kafka → MinIO Bronze")
    print(f"Time: {now.isoformat()}")
    print(f"Output: {STREAMING_BRONZE_PATH}")
    print("="*60)
    
    spark = get_spark()
    
    try:
        # Read from Kafka (batch mode - reads all available)
        df_raw = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC_INDIA)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        
        # Parse JSON from Kafka value
        df_parsed = (
            df_raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), FLIGHT_STATE_SCHEMA).alias("data"))
            .select("data.*")
        )
        
        # Add partitioning columns
        df_with_partitions = (
            df_parsed
            .withColumn("year", lit(now.year))
            .withColumn("month", lit(now.month))
            .withColumn("day", lit(now.day))
            .withColumn("hour", lit(now.hour))
        )
        
        # Count records
        count = df_with_partitions.count()
        print(f"  Records to write: {count:,}")
        
        if count > 0:
            # Write to MinIO
            (
                df_with_partitions.write
                .mode("append")
                .partitionBy("year", "month", "day", "hour")
                .parquet(STREAMING_BRONZE_PATH)
            )
            print(f"  ✓ Written to {STREAMING_BRONZE_PATH}")
        else:
            print("  ⚠️ No records to write")
        
        return {
            "timestamp": now.isoformat(),
            "records": count,
            "output": STREAMING_BRONZE_PATH
        }
        
    finally:
        spark.stop()


def ingest_streaming():
    """
    Continuous streaming ingestion using Spark Structured Streaming.
    Writes micro-batches to MinIO every minute.
    """
    print("="*60)
    print("STREAMING INGESTION: Kafka → MinIO Bronze")
    print(f"Output: {STREAMING_BRONZE_PATH}")
    print("="*60)
    
    spark = get_spark()
    
    try:
        # Read stream from Kafka
        df_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC_INDIA)
            .option("startingOffsets", "latest")
            .load()
        )
        
        # Parse JSON
        df_parsed = (
            df_stream
            .selectExpr("CAST(value AS STRING) as json_str", "timestamp")
            .select(
                from_json(col("json_str"), FLIGHT_STATE_SCHEMA).alias("data"),
                col("timestamp")
            )
            .select("data.*", "timestamp")
        )
        
        # Write stream to MinIO with hourly partitioning
        query = (
            df_parsed.writeStream
            .format("parquet")
            .option("path", STREAMING_BRONZE_PATH)
            .option("checkpointLocation", f"{STREAMING_BRONZE_PATH}/_checkpoint")
            .partitionBy("year", "month", "day", "hour")
            .trigger(processingTime="1 minute")  # Micro-batch every minute
            .start()
        )
        
        print("✓ Streaming started")
        print("Press Ctrl+C to stop...")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping streaming...")
    finally:
        spark.stop()


if __name__ == "__main__":
    # Run hourly batch ingestion
    result = ingest_hourly()
    print(f"\nResult: {result}")
