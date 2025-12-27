"""
Aviation Data Pipeline Configuration
Simple key-value configuration - no classes, no complexity
"""
import os

# =============================================================================
# DATA PATHS
# =============================================================================

# Raw data locations
RAW_DATA_ROOT = "/data/aviation_data"           # Docker mount
LOCAL_DATA_ROOT = "/media/D/data/aviation_data"  # Local fallback

# Auto-detect which path exists
DATA_ROOT = RAW_DATA_ROOT if os.path.exists(RAW_DATA_ROOT) else LOCAL_DATA_ROOT

# Source data paths
STATES_PATH = f"{DATA_ROOT}/states"
FLIGHTS_V5_PATH = f"{DATA_ROOT}/flightsV5"

# =============================================================================
# MINIO / S3 CONFIGURATION  
# =============================================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Lakehouse buckets
BRONZE_BUCKET = "aviation-bronze"
SILVER_BUCKET = "aviation-silver"
GOLD_BUCKET = "aviation-gold"
MAPPING_BUCKET = "aviation-mapping"

# S3A paths for direct MinIO access
BRONZE_PATH = f"s3a://{BRONZE_BUCKET}/states"
SILVER_PATH = f"s3a://{SILVER_BUCKET}/enriched_states"
GOLD_PATH = f"s3a://{GOLD_BUCKET}/analytics"
MAPPING_PATH = f"s3a://{MAPPING_BUCKET}/flight_routes"

# Temp directory for intermediate files (CSV exports, etc.)
LOCAL_OUTPUT = "/tmp/aviation"

# =============================================================================
# INDIA FILTER SETTINGS
# =============================================================================

# India bounding box (approximate)
INDIA_LAT_MIN = 6.0
INDIA_LAT_MAX = 37.0
INDIA_LON_MIN = 68.0
INDIA_LON_MAX = 98.0

# =============================================================================
# PROCESSING LIMITS
# =============================================================================

# Max batches to process (None = process all, set to limit for testing)
MAX_BATCHES = 10  # ~100 tar files, ~500k-1M records

# Indian airline callsign prefixes
INDIAN_AIRLINE_PREFIXES = [
    "AIC", "AI",    # Air India
    "IGO", "6E",    # IndiGo
    "SEJ", "SG",    # SpiceJet
    "VTI", "UK",    # Vistara
    "GAL", "G8",    # GoAir
    "JAI", "9W",    # Jet Airways
    "LLR", "SG",    # SpiceJet (alt)
    "AXB", "IX",    # Air India Express
    "ALW", "I5",    # AirAsia India
    "AAY", "QP",    # Akasa Air
]
