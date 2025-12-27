"""Spark utilities for aviation data pipeline.
Configures Spark with S3A support for direct MinIO read/write.
"""
from pyspark.sql import SparkSession
import src.batch.config as config


def get_spark(app_name: str = "AviationPipeline") -> SparkSession:
    """
    Create Spark session with S3A/MinIO support.
    Reads/writes directly to MinIO buckets using s3a:// paths.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "50")
        # S3A/MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
