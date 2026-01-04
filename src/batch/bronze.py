"""Bronze layer functions - Extract India flight states from raw tar files.
"""
import src.batch.config as config
from src.batch.spark_utils import get_spark
from src.batch.extract_india_flights import find_all_tar_files, process_tar_files_to_parquet


def extract_states() -> dict:
    """Extract India flight states from raw tar files to MinIO."""
    print("="*60)
    print("BRONZE LAYER: Extracting India Flight States")
    print(f"Output: {config.BRONZE_PATH}")
    print(f"Max batches: {config.MAX_BATCHES}")
    print("="*60)
    
    tar_files = find_all_tar_files(config.STATES_PATH)
    print(f"Found {len(tar_files)} tar files")
    
    if not tar_files:
        raise ValueError(f"No tar files found in {config.STATES_PATH}")
    
    spark = get_spark("BronzeExtraction")
    
    try:
        process_tar_files_to_parquet(
            spark=spark,
            tar_files=tar_files,
            output_path=config.BRONZE_PATH,
            batch_size=config.BATCH_SIZE,
            max_batches=config.MAX_BATCHES
        )
        
        record_count = spark.read.parquet(config.BRONZE_PATH).count()
        print(f"\n✅ BRONZE COMPLETE: {record_count:,} records")
        
        return {"records": record_count}
        
    finally:
        spark.stop()
