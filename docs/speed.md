cat << 'EOF'
═══════════════════════════════════════════════════════════════════════════
                    SPEED LAYER ARCHITECTURE ANALYSIS
═══════════════════════════════════════════════════════════════════════════

1. CURRENT ARCHITECTURE
────────────────────────────────────────────────────────────────────────────

   [Mock Data/OpenSky API]
            │
            ↓
   ┌──────────────────┐
   │ kafka_producer.py│  → Sends to Kafka topic: aviation-india-states
   └──────────────────┘
            │
            ↓
      [Kafka Topic]
            │
            ├─────────────────────────────────┐
            │                                 │
            ↓                                 ↓
   ┌──────────────────────┐        ┌──────────────────┐
   │  server.py (FastAPI) │        │  Hourly DAG      │
   │  - Kafka Consumer    │        │  (Airflow)       │
   │  - Route Enrichment  │        └──────────────────┘
   │  - Accumulator       │                 │
   │  - WebSocket Server  │                 ↓
   └──────────────────────┘         Calls /api/accumulator/data
            │                                │
            ├────────────────────────────────┘
            ↓                                ↓
    [Real-time Map]                  [Silver Layer]
    http://localhost:8050            MinIO: aviation-silver/


2. FILE-BY-FILE ANALYSIS
────────────────────────────────────────────────────────────────────────────

✅ config.py (67 lines) - CLEAN
   - Single source of truth for configuration
   - No redundancy
   Purpose: Kafka settings, OpenSky API config, field mappings

✅ kafka_producer.py (180 lines) - CLEAN
   - Fetches data (mock or real API)
   - Sends to Kafka
   Purpose: Data ingestion into Kafka

✅ mock_data_generator.py (293 lines) - CLEAN
   - Generates realistic flight data
   - Matches OpenSky API format
   - Stateful flight progression
   Purpose: Testing and development when OpenSky unavailable

✅ route_mapping.py (246 lines) - CLEAN
   - Airlines and airports data
   - Flight number → route patterns
   - Enrichment function
   Purpose: Add origin/destination to raw flight data

⚠️  kafka_consumer.py (143 lines) - UNUSED/REDUNDANT
   - Has consume_realtime() and consume_batch() functions
   - NOT used anywhere in the codebase
   - server.py implements its own consumer
   Status: DEAD CODE - Can be deleted

⚠️  stateful_accumulator.py (412 lines) - UNUSED/REDUNDANT
   - Has full accumulator logic with Spark persistence
   - NOT used anywhere
   - server.py implements its own accumulator
   Status: DEAD CODE - Can be deleted

⚠️  hourly_ingestion.py (200 lines) - UNUSED/REDUNDANT
   - Old approach: Spark reads from Kafka, writes to Bronze
   - NOT used by any DAG
   - Replaced by: server.py accumulator + hourly_silver_ingestion.py
   Status: DEAD CODE - Can be deleted

✅ hourly_silver_ingestion.py (301 lines) - CLEAN & ACTIVE
   - NEW: Clean separation of concerns
   - Fetches from server.py API
   - Persists to Silver with Spark
   Purpose: Hourly DAG logic (used by speed_layer_silver_ingestion DAG)

⚠️  spark_streaming_enrichment.py (362 lines) - UNUSED
   - Spark Structured Streaming approach
   - Reads from Kafka → Enriches → Writes to another Kafka topic
   - NOT used anywhere
   Status: DEAD CODE OR ALTERNATIVE APPROACH - Can be deleted or kept for future

✅ server.py (FastAPI webapp) - ACTIVE & ESSENTIAL
   - Kafka consumer in background thread
   - Route enrichment via route_mapping.py
   - Stateful accumulator (in-memory)
   - WebSocket for real-time map
   - REST API for DAG to fetch/clear data
   Purpose: Core speed layer service


3. REDUNDANCY ISSUES
────────────────────────────────────────────────────────────────────────────

❌ THREE implementations of Kafka consumer:
   1. kafka_consumer.py (unused)
   2. stateful_accumulator.py (unused)
   3. server.py (ACTIVE - this is the one being used)

❌ TWO implementations of stateful accumulator:
   1. stateful_accumulator.py (unused)
   2. server.py FlightAccumulator class (ACTIVE)

❌ TWO implementations of hourly persistence:
   1. hourly_ingestion.py (unused)
   2. hourly_silver_ingestion.py (ACTIVE)

❌ Spark streaming enrichment exists but not used anywhere


4. WHAT'S ACTUALLY RUNNING
────────────────────────────────────────────────────────────────────────────

Active Services:
  1. kafka-producer → Runs: kafka_producer.py
  2. flight-tracker → Runs: server.py (FastAPI)
  3. Airflow DAG    → Runs: hourly_silver_ingestion.py functions

Data Flow:
  kafka_producer.py → Kafka → server.py → {WebSocket, Accumulator API}
                                                         │
                                                         ↓
                                              hourly_silver_ingestion.py
                                                         │
                                                         ↓
                                                  Silver Layer


5. RECOMMENDATIONS
────────────────────────────────────────────────────────────────────────────

IMMEDIATE - Delete Dead Code:
  ✂️  Remove: kafka_consumer.py (143 lines saved)
  ✂️  Remove: stateful_accumulator.py (412 lines saved)
  ✂️  Remove: hourly_ingestion.py (200 lines saved)
  Total: 755 lines of dead code

DECISION NEEDED - Spark Streaming:
  🤔 spark_streaming_enrichment.py (362 lines)
     Option A: Delete (not being used)
     Option B: Keep (future alternative architecture)
     Current: You chose simple FastAPI approach over Spark Streaming

KEEP - Essential Files:
  ✅ config.py
  ✅ kafka_producer.py
  ✅ mock_data_generator.py
  ✅ route_mapping.py
  ✅ hourly_silver_ingestion.py
  ✅ server.py (in src/webapp/)


6. CODE QUALITY ASSESSMENT
────────────────────────────────────────────────────────────────────────────

Active Code Quality: ✅ GOOD

  ✅ Separation of concerns
     - Producer: Just fetches & sends
     - Server: Consumes, enriches, streams, accumulates
     - DAG logic: Persists to Silver

  ✅ Clean imports
     - DAG imports from hourly_silver_ingestion.py
     - Server imports from route_mapping.py

  ✅ Good documentation
     - Docstrings explain purpose
     - Comments where needed

  ⚠️  Complexity issue: server.py does too many things
     - Kafka consumer
     - Route enrichment
     - Stateful accumulator
     - WebSocket streaming
     - REST API
     This is acceptable for speed layer but could be split if needed


7. ARCHITECTURE SIMPLIFICATION OPPORTUNITY
────────────────────────────────────────────────────────────────────────────

Current: Everything in server.py
Cleaner: Split responsibilities

Option 1 - Keep Current (Simple):
  server.py handles everything
  + Simple deployment
  + Lower complexity
  - Single point of failure
  - Hard to scale components independently

Option 2 - Microservices (Complex):
  1. kafka_consumer_service.py → Consumes & enriches
  2. accumulator_service.py → Maintains state
  3. websocket_service.py → Streams to clients
  4. persistence_api.py → DAG endpoint
  + Scalable
  + Clear separation
  - More complex deployment
  - Network overhead between services

Recommendation: Keep current simple approach for now
EOF