"""Silver layer functions - Enrich bronze states with route information.
"""
from pyspark.sql.functions import col, trim, upper, broadcast
import src.batch.config as config
from src.batch.spark_utils import get_spark


def check_mapping_exists() -> bool:
    """Check if mapping table exists in MinIO."""
    try:
        spark = get_spark("CheckMapping")
        df = spark.read.parquet(config.MAPPING_PATH)
        count = df.count()
        spark.stop()
        print(f"✓ Mapping found: {count} records")
        return count > 0
    except Exception as e:
        print(f"⚠️ Mapping not found: {e}")
        return False


def enrich_states() -> dict:
    """Enrich bronze states with route information from mapping table."""
    print("="*60)
    print("SILVER LAYER: Enriching States with Routes")
    print(f"Bronze: {config.BRONZE_PATH}")
    print(f"Silver: {config.SILVER_PATH}")
    print(f"Mapping: {config.MAPPING_PATH}")
    print("="*60)
    
    spark = get_spark("SilverEnrichment")
    
    try:
        # Read bronze
        df_states = spark.read.parquet(config.BRONZE_PATH)
        bronze_count = df_states.count()
        print(f"  Bronze records: {bronze_count:,}")
        
        # Read mapping
        df_mapping = spark.read.parquet(config.MAPPING_PATH)
        print(f"  Mapping records: {df_mapping.count():,}")
        
        # Prepare join
        df_states = df_states.withColumn("callsign_clean", trim(upper(col("callsign"))))
        
        df_mapping_select = df_mapping.select(
            col("callsign").alias("mapping_callsign"),
            col("departure_airport"),
            col("arrival_airport"),
            col("route"),
            col("aircraft_model"),
            col("aircraft_type")
        )
        
        # Join with broadcast
        df_enriched = df_states.join(
            broadcast(df_mapping_select),
            df_states.callsign_clean == col("mapping_callsign"),
            "left"
        ).drop("callsign_clean", "mapping_callsign")
        
        # Save to MinIO
        df_enriched.write.mode("overwrite").partitionBy("year", "month").parquet(config.SILVER_PATH)
        
        # Stats
        silver_count = df_enriched.count()
        matched = df_enriched.filter(col("route").isNotNull()).count()
        match_rate = (matched / silver_count * 100) if silver_count > 0 else 0
        
        print(f"\n✅ SILVER COMPLETE: {silver_count:,} records")
        print(f"   Matched: {matched:,} ({match_rate:.1f}%)")
        
        return {
            "records": silver_count,
            "matched": matched,
            "match_rate": round(match_rate, 1)
        }
        
    finally:
        spark.stop()
