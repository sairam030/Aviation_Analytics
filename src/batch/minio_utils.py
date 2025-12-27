"""
MinIO utility functions for aviation data pipeline.
"""
import os
from minio import Minio
import src.batch.config as config


def get_minio_client():
    """Get MinIO client."""
    return Minio(
        "minio:9000",
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=False
    )


def create_bucket(bucket_name: str) -> bool:
    """Create a MinIO bucket if it doesn't exist."""
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✓ Created bucket: {bucket_name}")
        return True
    print(f"✓ Bucket exists: {bucket_name}")
    return False


def create_all_buckets():
    """Create all lakehouse buckets."""
    buckets = [config.BRONZE_BUCKET, config.SILVER_BUCKET, config.GOLD_BUCKET, config.MAPPING_BUCKET]
    for bucket in buckets:
        create_bucket(bucket)


def upload_directory(local_path: str, bucket: str, remote_prefix: str) -> int:
    """Upload a local directory to MinIO bucket."""
    client = get_minio_client()
    uploaded = 0
    
    if not os.path.exists(local_path):
        print(f"⚠️ Path not found: {local_path}")
        return 0
    
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.startswith('.') or file.startswith('_'):
                continue
            local_file = os.path.join(root, file)
            remote_path = remote_prefix + "/" + os.path.relpath(local_file, local_path)
            client.fput_object(bucket, remote_path, local_file)
            uploaded += 1
    
    print(f"✅ Uploaded {uploaded} files to {bucket}/{remote_prefix}")
    return uploaded


def upload_file(local_path: str, bucket: str, remote_path: str) -> bool:
    """Upload a single file to MinIO."""
    client = get_minio_client()
    if os.path.exists(local_path):
        client.fput_object(bucket, remote_path, local_path)
        print(f"✅ Uploaded {remote_path} to {bucket}")
        return True
    return False
