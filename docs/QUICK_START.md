# Speed Layer Refactoring - Quick Start Guide

## What Changed?

The speed layer has been refactored to follow proper separation of concerns:

### OLD Architecture
```
kafka_producer → Kafka → server.py (consumer + enrichment + accumulator + WebSocket)
```

### NEW Architecture
```
kafka_producer → aviation-india-states (raw)
                 ↓
spark_streaming_enrichment → aviation-enriched-states (enriched)
                              ↓
kafka_consumer → consumer_thread → server.py (accumulator + WebSocket)
                                   ↓
                            hourly_silver_ingestion DAG → Silver Layer
```

## Files Modified

### Core Changes
1. **config.py**: Added `KAFKA_TOPIC_RAW` and `KAFKA_TOPIC_ENRICHED`
2. **kafka_producer.py**: Updated to publish to raw topic
3. **kafka_consumer.py**: **Completely rewritten** - simple consumer for enriched data
4. **spark_streaming_enrichment.py**: **Completely rewritten** - UDF-based enrichment
5. **consumer_thread.py**: **NEW** - Bridge between consumer and server
6. **server.py**: Updated to receive enriched data (removed enrichment logic)

### Files Deleted (612 lines)
- `stateful_accumulator.py` (412 lines) - Redundant
- `hourly_ingestion.py` (200 lines) - Old approach

### Docker Changes
- **docker-compose.yml**: Added `spark-streaming-enrichment` service

## Quick Start

### 1. Stop Existing Services
```bash
cd /home/ram/aviation_dataPipeline
docker compose down
```

### 2. Start New Architecture
```bash
docker compose up -d
```

### 3. Verify Services

#### Check All Services Running
```bash
docker compose ps
```

Expected services:
- postgres
- minio
- kafka, zookeeper, kafka-ui
- kafka-producer
- **spark-streaming-enrichment** (NEW)
- flight-tracker
- airflow-webserver, airflow-scheduler

#### Check Kafka Topics
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Should see:
# - aviation-india-states (raw)
# - aviation-enriched-states (enriched)
```

#### Monitor Spark Streaming
```bash
# View Spark enrichment logs
docker logs -f spark-streaming-enrichment

# Look for:
# ✓ Spark session created
# ✓ Reading from aviation-india-states
# ✓ Writing to aviation-enriched-states
```

#### Check Producer
```bash
docker logs kafka-producer | tail -20

# Look for:
# ✓ Created/verified topic: aviation-india-states
# ✓ Sent X messages to aviation-india-states
```

#### Check Server
```bash
docker logs flight-tracker | tail -20

# Look for:
# ✈️  FLIGHT TRACKER SERVER STARTED
# ✓ Consuming enriched flights from Kafka
# ✓ Stateful accumulator (update/add flights)
# ✓ WebSocket real-time streaming
```

### 4. Test Data Flow

#### a) Check Raw Topic (Producer Output)
Open Kafka UI: http://localhost:8084
- Navigate to: Topics → aviation-india-states
- View messages - should see raw flight data (16 fields)

#### b) Check Enriched Topic (Spark Output)
- Navigate to: Topics → aviation-enriched-states
- View messages - should see enriched data (36 fields with airline, routes)

#### c) Check Flight Tracker UI
Open browser: http://localhost:8050
- Should see real-time flight map
- Flights should have airline names, origin/destination info

#### d) Check API
```bash
# Get accumulated flights
curl http://localhost:8050/api/flights | jq '.[0]'

# Should see all 36 fields including:
# - airline_name, airline_iata
# - origin_code, origin_city, origin_airport
# - destination_code, destination_city, destination_airport
# - first_seen, last_seen, position_count
```

### 5. Test DAG

#### Enable DAG
1. Open Airflow UI: http://localhost:8080
2. Login: admin / admin
3. Enable DAG: `speed_layer_silver_ingestion`
4. Trigger manually or wait for hourly schedule

#### Monitor DAG Run
Check task logs:
1. `check_health` - Should pass
2. `fetch_data` - Should fetch accumulated flights
3. `persist_silver` - Should write to MinIO Silver bucket
4. `clear_state` - Should clear accumulator
5. `summary` - Should print statistics

## Troubleshooting

### Issue: spark-streaming-enrichment keeps restarting
**Solution:** Check logs for errors
```bash
docker logs spark-streaming-enrichment | grep -i error
```
Common causes:
- Kafka not ready: Service will retry
- Missing packages: Rebuild image with `--no-cache`

### Issue: No data in enriched topic
**Steps:**
1. Check raw topic has data:
   ```bash
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic aviation-india-states \
     --from-beginning --max-messages 1
   ```

2. Check Spark is reading:
   ```bash
   docker logs spark-streaming-enrichment | grep "Batch:"
   ```

3. Check for enrichment errors:
   ```bash
   docker logs spark-streaming-enrichment | grep -i "error\|exception"
   ```

### Issue: Server not receiving data
**Steps:**
1. Verify enriched topic has data (see above)
2. Check server consumer is running:
   ```bash
   docker logs flight-tracker | grep "consumer"
   ```
3. Restart flight-tracker:
   ```bash
   docker compose restart flight-tracker
   ```

### Issue: DAG failing
**Steps:**
1. Check server is running:
   ```bash
   curl http://localhost:8050/health
   ```

2. Check accumulated data exists:
   ```bash
   curl http://localhost:8050/api/flights
   ```

3. Check task logs in Airflow UI for specific error

## Verification Checklist

- [ ] All services running: `docker compose ps`
- [ ] Raw topic has data: Kafka UI → aviation-india-states
- [ ] Enriched topic has data: Kafka UI → aviation-enriched-states
- [ ] Spark streaming running: `docker logs spark-streaming-enrichment`
- [ ] Server receiving data: `curl http://localhost:8050/api/flights`
- [ ] WebSocket working: Open http://localhost:8050
- [ ] DAG enabled: Airflow UI
- [ ] DAG running successfully: Check task logs

## Key Endpoints

- **Flight Tracker UI**: http://localhost:8050
- **Kafka UI**: http://localhost:8084
- **Airflow UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (minioadmin / minioadmin)
- **Metabase**: http://localhost:3000

## Testing with Mock Data

By default, `USE_MOCK_DATA=true` in kafka-producer service.

Mock data characteristics:
- Generates 20-30 flights every 15 seconds
- Flight numbers match route mapping ranges
- Realistic progression (altitude, velocity changes)
- 100% enrichment rate (all flights have routes)

To use real OpenSky API:
1. Edit docker-compose.yml
2. Change `USE_MOCK_DATA: "false"` in kafka-producer
3. Restart: `docker compose restart kafka-producer`

## Next Steps

1. **Monitor for 1 hour**: Ensure DAG runs successfully at hour mark
2. **Check Silver Layer**: Verify data in MinIO `aviation-silver` bucket
3. **Review Logs**: Look for any warnings or errors
4. **Performance**: Monitor Spark memory usage
5. **Scale Testing**: Increase mock data rate if needed

## Rollback (if needed)

If you encounter critical issues:

```bash
# Stop services
docker compose down

# Switch to backup branch (if you created one)
git checkout backup-before-refactor

# Start old services
docker compose up -d
```

## Support

For detailed architecture documentation, see:
- `docs/SPEED_LAYER_ARCHITECTURE.md` - Complete architecture overview
- `src/speed/spark_streaming_enrichment.py` - Spark UDF enrichment details
- `src/webapp/server.py` - Server accumulator details

## Summary

The refactoring provides:
- **Separation of Concerns**: Each service has one responsibility
- **Scalability**: Spark can scale independently
- **Maintainability**: Easier to test and debug
- **Performance**: Optimized enrichment with Spark UDF
- **Clean Data Flow**: Clear separation of raw vs enriched data
