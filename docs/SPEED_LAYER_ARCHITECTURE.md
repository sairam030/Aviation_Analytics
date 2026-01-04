# Speed Layer Architecture - Separation of Concerns

## Overview
The speed layer has been refactored to follow proper separation of concerns with dedicated services for each responsibility.

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                    │
└─────────────────────────────────────────────────────────────────────┘

1. INGESTION (kafka_producer.py)
   ├─ Fetches: OpenSky API or Mock Data Generator
   ├─ Publishes: Raw flight state data
   └─ Topic: aviation-india-states

2. ENRICHMENT (spark_streaming_enrichment.py)
   ├─ Consumes: aviation-india-states (raw)
   ├─ Enriches: Route mapping (airline, origin, destination)
   ├─ Method: Spark Structured Streaming with UDF
   └─ Publishes: aviation-enriched-states

3. CONSUMPTION (kafka_consumer.py)
   ├─ Consumes: aviation-enriched-states
   └─ Streams: Enriched flights to callback function

4. ACCUMULATION (server.py via consumer_thread.py)
   ├─ Receives: Enriched flights from kafka_consumer
   ├─ Maintains: Stateful accumulator (update/add flights)
   ├─ Serves: WebSocket real-time streaming
   └─ Exposes: REST API for DAG persistence

5. PERSISTENCE (hourly_silver_ingestion.py DAG)
   ├─ Fetches: Accumulated data from server API
   ├─ Persists: Silver layer (Spark + MinIO)
   └─ Clears: Accumulator after persistence
```

## Services

### 1. kafka-producer
**File:** `src/speed/kafka_producer.py`
**Container:** `kafka-producer`
**Purpose:** Fetch and publish raw flight data
**Topic Output:** `aviation-india-states` (raw)

**Environment:**
- `USE_MOCK_DATA=true` - Use mock generator (testing)
- `USE_MOCK_DATA=false` - Use OpenSky API (production)

**Key Functions:**
- `fetch_opensky_data()` - Real API
- `run_producer()` - Main loop

### 2. spark-streaming-enrichment
**File:** `src/speed/spark_streaming_enrichment.py`
**Container:** `spark-streaming-enrichment`
**Purpose:** Real-time enrichment with route mapping
**Topic Input:** `aviation-india-states`
**Topic Output:** `aviation-enriched-states`

**Enrichment Process:**
- Uses Spark Structured Streaming
- UDF calls `route_mapping.enrich_flight_data()`
- Adds: airline info, origin airport, destination airport
- Output: 36 fields (16 raw + 20 enriched)

**Key Functions:**
- `enrich_flight_udf()` - UDF wrapper
- `run_enrichment_stream()` - Main streaming job

### 3. kafka_consumer
**File:** `src/speed/kafka_consumer.py`
**Purpose:** Simple consumer for enriched data
**Topic Input:** `aviation-enriched-states`

**Key Functions:**
- `consume_stream(callback)` - Streams to callback function

### 4. consumer_thread
**File:** `src/speed/consumer_thread.py`
**Purpose:** Bridge between kafka_consumer and server
**Key Functions:**
- `start_consumer_thread(callback)` - Runs consumer in background thread

### 5. flight-tracker (server)
**File:** `src/webapp/server.py`
**Container:** `flight-tracker`
**Port:** `8050`
**Purpose:** Stateful accumulator + WebSocket + API

**Components:**
- `FlightAccumulator` - Maintains flight state (update/add)
- `process_enriched_flights()` - Receives from consumer_thread
- `broadcast_messages()` - WebSocket streaming
- REST API endpoints for DAG

**Key Endpoints:**
- `GET /` - Flight tracker UI
- `WS /ws` - WebSocket for real-time updates
- `GET /api/flights` - Get accumulated flights (DAG)
- `POST /api/clear` - Clear accumulator (DAG)
- `GET /health` - Health check

### 6. hourly_silver_ingestion DAG
**File:** `dags/speed_layer_silver_ingestion.py`
**Business Logic:** `src/speed/hourly_silver_ingestion.py`
**Schedule:** Every hour at minute 0
**Purpose:** Persist accumulated data to Silver layer

**Tasks:**
1. `check_health` - Verify flight-tracker is running
2. `fetch_data` - Get accumulated data via API
3. `persist_silver` - Write to Silver layer (MinIO)
4. `clear_state` - Clear accumulator via API
5. `summary` - Print statistics

## Data Schema

### Raw Data (aviation-india-states)
16 fields from OpenSky API:
- icao24, callsign, origin_country
- time_position, last_contact
- longitude, latitude
- baro_altitude, geo_altitude, on_ground
- velocity, true_track, vertical_rate
- squawk, spi, position_source

### Enriched Data (aviation-enriched-states)
36 fields (raw + enrichment):

**Added by Spark Streaming:**
- `airline_prefix` - "IGO", "AIC", etc.
- `airline_name` - "IndiGo", "Air India", etc.
- `airline_iata` - "6E", "AI", etc.
- `airline_icao` - "IGO", "AIC", etc.
- `flight_number` - Extracted from callsign
- `origin_code` - "DEL", "BOM", etc.
- `origin_city` - "Delhi", "Mumbai", etc.
- `origin_airport` - "Indira Gandhi International"
- `origin_lat`, `origin_lon`
- `destination_code`, `destination_city`, `destination_airport`
- `destination_lat`, `destination_lon`

**Added by Accumulator:**
- `first_seen` - First position timestamp
- `last_seen` - Latest position timestamp
- `position_count` - Number of position updates
- `ingestion_time` - Timestamp when added to accumulator

## Configuration

### Kafka Topics
**File:** `src/speed/config.py`
```python
KAFKA_TOPIC_RAW = "aviation-india-states"
KAFKA_TOPIC_ENRICHED = "aviation-enriched-states"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
```

### Route Mapping
**File:** `src/speed/route_mapping.py`
- 10 Indian airports (DEL, BOM, BLR, MAA, CCU, HYD, AMD, COK, GOI, PNQ)
- 5 airlines (IndiGo, Air India, SpiceJet, Vistara, GoAir)
- Route patterns by airline and flight number range

## Testing

### Start Services
```bash
docker-compose up -d
```

### Verify Kafka Topics
```bash
# Check topics exist
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Should see:
# - aviation-india-states (raw)
# - aviation-enriched-states (enriched)
```

### Monitor Spark Streaming
```bash
# View Spark logs
docker logs -f spark-streaming-enrichment
```

### Test Flight Tracker
```bash
# WebSocket connection
# Open browser: http://localhost:8050

# Check accumulated data
curl http://localhost:8050/api/flights

# Check health
curl http://localhost:8050/health
```

### Verify DAG
```bash
# Airflow UI: http://localhost:8080
# Enable: speed_layer_silver_ingestion
# Trigger manually or wait for hourly schedule
```

## File Structure

```
src/speed/
├── config.py                          # Kafka configuration
├── kafka_producer.py                  # Raw data producer
├── spark_streaming_enrichment.py     # Spark UDF enrichment
├── kafka_consumer.py                  # Simple enriched consumer
├── consumer_thread.py                 # Bridge to server
├── route_mapping.py                   # Enrichment logic
├── mock_data_generator.py             # Test data generator
└── hourly_silver_ingestion.py         # DAG business logic

src/webapp/
├── server.py                          # FastAPI + accumulator
├── static/                            # CSS, JS
└── templates/                         # HTML

dags/
└── speed_layer_silver_ingestion.py    # Airflow DAG

scripts/
└── run_spark_enrichment.sh            # Spark startup script
```

## Deleted Dead Code

The following files were removed during refactoring (755 lines total):
- `src/speed/stateful_accumulator.py` (412 lines) - Redundant, functionality in server.py
- `src/speed/hourly_ingestion.py` (200 lines) - Old approach, replaced by hourly_silver_ingestion.py
- `src/speed/kafka_accumulator.py` (143 lines) - Old Kafka consumer approach

## Benefits of New Architecture

### Separation of Concerns
- **Producer**: Only responsible for data ingestion
- **Spark Streaming**: Only responsible for enrichment
- **Consumer**: Only responsible for reading enriched data
- **Server**: Only responsible for accumulation and serving
- **DAG**: Only responsible for persistence

### Scalability
- Spark Streaming can scale independently
- Multiple consumers can read enriched data
- Stateless enrichment (UDF)

### Maintainability
- Each component has single responsibility
- Easy to test each component independently
- Clear data flow

### Performance
- Spark's distributed processing for enrichment
- No enrichment in server (already done)
- Clean Kafka topic separation

## Monitoring

### Kafka UI
**URL:** http://localhost:8084
- View topics: aviation-india-states, aviation-enriched-states
- Monitor consumer groups
- Check message throughput

### Spark Logs
```bash
docker logs -f spark-streaming-enrichment
```

### Server Logs
```bash
docker logs -f flight-tracker
```

### Airflow Logs
**URL:** http://localhost:8080
- Task logs for each DAG run
- XCom values for data passed between tasks

## Troubleshooting

### No Data in enriched-states Topic
1. Check kafka-producer is running: `docker logs kafka-producer`
2. Check spark-streaming is running: `docker logs spark-streaming-enrichment`
3. Verify raw topic has data: Kafka UI → aviation-india-states

### Server Not Receiving Data
1. Check enriched topic has data: Kafka UI → aviation-enriched-states
2. Check server logs: `docker logs flight-tracker`
3. Verify consumer_thread is running

### DAG Failing
1. Check server health: `curl http://localhost:8050/health`
2. Check accumulated data exists: `curl http://localhost:8050/api/flights`
3. Check Airflow logs for specific task failure

## Future Enhancements

1. **Schema Registry**: Add Confluent Schema Registry for schema evolution
2. **Monitoring**: Add Prometheus + Grafana for metrics
3. **Alerting**: Add alerts for data quality issues
4. **Backpressure**: Handle backpressure in Spark Streaming
5. **Checkpointing**: Add Spark checkpointing for fault tolerance
