"""
Speed Layer Configuration
Kafka and OpenSky API settings
"""

# =============================================================================
# OPENSKY API
# =============================================================================

OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

# India bounding box
INDIA_BBOX = {
    "lamin": 8.0,    # Latitude min
    "lamax": 37.0,   # Latitude max
    "lomin": 68.0,   # Longitude min
    "lomax": 98.0,   # Longitude max
}

# API credentials (OAuth2 - higher rate limits with auth)
OPENSKY_CLIENT_ID = "voidmain-api-client"
OPENSKY_CLIENT_SECRET = "xjzyvZylyH0iFSxCGcssekIavYIF0zf2"

# Legacy basic auth (kept for backward compatibility)
OPENSKY_USERNAME = None
OPENSKY_PASSWORD = None

# Fetch interval in seconds 
# Without auth: OpenSky allows ~1 request per 10 seconds (anonymous)
# With OAuth2 auth: ~1 request per 5 seconds (authenticated users)
FETCH_INTERVAL = 5  # 5 seconds with OAuth2 authentication

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_RAW = "aviation-raw-states"
KAFKA_TOPIC_INDIA = "aviation-india-states"

# Consumer group
KAFKA_CONSUMER_GROUP = "aviation-consumer-group"

# =============================================================================
# DATA SCHEMA (OpenSky API response fields)
# =============================================================================

# OpenSky returns an array with these fields (in order):
OPENSKY_FIELDS = [
    "icao24",           # 0: Unique ICAO 24-bit address (hex)
    "callsign",         # 1: Callsign (8 chars max)
    "origin_country",   # 2: Country of origin
    "time_position",    # 3: Unix timestamp of last position update
    "last_contact",     # 4: Unix timestamp of last contact
    "longitude",        # 5: WGS-84 longitude
    "latitude",         # 6: WGS-84 latitude
    "baro_altitude",    # 7: Barometric altitude (meters)
    "on_ground",        # 8: Boolean - is on ground
    "velocity",         # 9: Ground speed (m/s)
    "true_track",       # 10: Track angle (degrees clockwise from north)
    "vertical_rate",    # 11: Vertical rate (m/s)
    "sensors",          # 12: IDs of sensors that received signal
    "geo_altitude",     # 13: Geometric altitude (meters)
    "squawk",           # 14: Transponder code
    "spi",              # 15: Special purpose indicator
    "position_source",  # 16: Source of position (0=ADS-B, 1=ASTERIX, 2=MLAT)
]
