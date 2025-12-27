"""
Serving Layer Configuration
PostgreSQL and data merge settings
"""

# =============================================================================
# POSTGRESQL CONFIGURATION
# =============================================================================

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "aviation_db"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# Connection string
POSTGRES_CONN_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# =============================================================================
# MINIO / S3 CONFIGURATION
# =============================================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Buckets
SILVER_BUCKET = "aviation-silver"
GOLD_BUCKET = "aviation-gold"

# =============================================================================
# SERVING LAYER TABLES
# =============================================================================

# Table names in PostgreSQL
FLIGHT_STATES_TABLE = "flight_states"
FLIGHT_SUMMARY_TABLE = "flight_summary"
AIRLINE_STATS_TABLE = "airline_stats"
AIRPORT_STATS_TABLE = "airport_stats"
HOURLY_STATS_TABLE = "hourly_stats"

# =============================================================================
# DATA MERGE SETTINGS
# =============================================================================

# How many days of batch data to include
BATCH_LOOKBACK_DAYS = 7

# Deduplication strategy: keep latest by last_contact timestamp
DEDUP_KEY = ["icao24", "callsign", "last_contact"]
