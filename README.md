# Aviation Analytics Pipeline

Real-time and batch aviation data processing pipeline using Lambda Architecture with Apache Spark, Kafka, and PostgreSQL.

## 🚀 Features

- **Real-time Flight Tracking**: Live WebSocket-based flight map with 5-second updates
- **Batch Processing**: Historical flight data processing with Apache Spark
- **Analytics Tables**: 5 PostgreSQL tables with comprehensive flight analytics
- **Auto-restart Capability**: Spark streaming auto-restarts with wrapper scripts
- **S3-Compatible Storage**: MinIO for Bronze/Silver/Gold data lakes
- **Orchestration**: Apache Airflow for workflow management
- **Monitoring**: Spark UI, Kafka UI, and Airflow dashboards
- **Comprehensive Metrics**: Detailed pipeline execution metrics and logging
- **Task Tracking**: Per-task execution times, data volumes, and health checks

## Architecture

```
                                    ┌─────────────────────────────────────────────────────────────┐
                                    │                     DATA SOURCES                            │
                                    │  ┌─────────────────┐         ┌─────────────────┐           │
                                    │  │  OpenSky API    │         │  Historical     │           │
                                    │  │  (Real-time)    │         │  Parquet Files  │           │
                                    │  └────────┬────────┘         └────────┬────────┘           │
                                    └───────────┼────────────────────────────┼────────────────────┘
                                                │                            │
                        ┌───────────────────────┼────────────────────────────┼───────────────────────┐
                        │                       │      INGESTION LAYER       │                       │
                        │                       ▼                            ▼                       │
                        │              ┌─────────────────┐         ┌─────────────────┐              │
                        │              │  Kafka Producer │         │  Airflow DAG    │              │
                        │              │  (15s interval) │         │  (Batch Load)   │              │
                        │              └────────┬────────┘         └────────┬────────┘              │
                        └───────────────────────┼────────────────────────────┼───────────────────────┘
                                                │                            │
        ┌───────────────────────────────────────┼────────────────────────────┼───────────────────────────────────────┐
        │                                       │     PROCESSING LAYER       │                                       │
        │     ┌─────────────────────────────────┼─────────────────┐          │                                       │
        │     │         SPEED LAYER             │                 │          │         BATCH LAYER                   │
        │     │                                 ▼                 │          │                                       │
        │     │  ┌──────────────┐    ┌─────────────────┐         │          │    ┌─────────────────┐                │
        │     │  │    Kafka     │───▶│ Flight Tracker  │         │          │    │  Apache Spark   │                │
        │     │  │   (Topic)    │    │ (Enrichment +   │         │          │    │  (ETL Jobs)     │                │
        │     │  └──────────────┘    │  Accumulator)   │         │          │    └────────┬────────┘                │
        │     │                      └────────┬────────┘         │          │             │                         │
        │     │                               │                  │          │             ▼                         │
        │     │                               ▼                  │          │    ┌─────────────────┐                │
        │     │                      ┌─────────────────┐         │          │    │     MinIO       │                │
        │     │                      │   WebSocket     │         │          │    │  Silver Layer   │◀───────────────┤
        │     │                      │   (Live Map)    │         │          │    │  (Enriched)     │                │
        │     │                      └────────┬────────┘         │          │    └─────────────────┘                │
        │     │                               │                  │          │                                       │
        │     └───────────────────────────────┼──────────────────┘          │                                       │
        │                                     │ (Hourly DAG)                │                                       │
        │                                     ▼                             ▼                                       │
        │                            ┌─────────────────────────────────────────┐                                    │
        │                            │              MinIO Silver               │                                    │
        │                            │    (Batch + Speed Layer Merged)         │                                    │
        │                            └───────────────────┬─────────────────────┘                                    │
        └────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┘
                                                         │
                        ┌────────────────────────────────┼────────────────────────────────┐
                        │                    SERVING LAYER                                │
                        │                                ▼                                │
                        │                     ┌─────────────────┐                         │
                        │                     │   PostgreSQL    │                         │
                        │                     │  (Unified Data) │                         │
                        │                     └────────┬────────┘                         │
                        │                              │                                  │
                        │              ┌───────────────┼───────────────┐                  │
                        │              ▼               ▼               ▼                  │
                        │     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
                        │     │   Metabase   │ │ Flight Map   │ │   Airflow    │         │
                        │     │  Dashboards  │ │  (Real-time) │ │   Web UI     │         │
                        │     └──────────────┘ └──────────────┘ └──────────────┘         │
                        └─────────────────────────────────────────────────────────────────┘
```

## 🏗️ Infrastructure

### Docker Services (15 containers)

| Service | Port | Description |
|---------|------|-------------|
| **Airflow Webserver** | 8080 | DAG management & monitoring |
| **Flight Tracker** | 8050 | Real-time flight map (FastAPI + WebSocket) |
| **Spark Master UI** | 8081 | Spark cluster monitoring |
| **Spark Worker 1** | 8082 | Worker node 1 (2 cores, 2GB RAM) |
| **Spark Worker 2** | 8083 | Worker node 2 (2 cores, 2GB RAM) |
| **Kafka UI** | 8084 | Kafka topics & consumers |
| **MinIO Console** | 9001 | Object storage (S3-compatible) |
| **Metabase** | 3000 | Analytics dashboards |
| **PostgreSQL** | 5432 | Analytics database |
| **Kafka** | 9092 | Message broker |
| **Zookeeper** | 2181 | Kafka coordination |
| **Airflow Scheduler** | - | Background task scheduler |
| **Airflow Triggerer** | - | Deferrable operator handler |
| **MinIO Storage** | 9000 | S3 API endpoint |

### Tech Stack

- **Apache Spark 3.5.1**: Distributed batch & streaming processing
- **Apache Kafka**: Real-time message streaming
- **Apache Airflow 2.10.4**: Workflow orchestration
- **PostgreSQL 13**: Analytics database
- **MinIO**: S3-compatible object storage
- **FastAPI**: Flight tracker backend
- **Python 3.11**: Core language

## 📊 Analytics Tables

The serving layer provides 5 comprehensive PostgreSQL tables:

### 1. **fact_flight_history**
Flight-level aggregations with 30+ metrics per flight:
- Flight duration, distance, speed statistics
- Altitude profile (min, max, avg)
- Position changes and trajectory metrics
- Airspace time distribution

### 2. **dim_route_analytics**
Route-level performance metrics:
- Average flight time per route
- Traffic frequency
- Speed and altitude patterns
- On-ground percentage

### 3. **dim_aircraft_utilization**
Per-aircraft daily utilization:
- Total flight time
- Number of flights
- Distance covered
- Operational efficiency

### 4. **dim_airspace_heatmap**
Spatial and temporal traffic analysis:
- 0.5° x 0.5° geographic grid
- Hourly traffic patterns
- Average altitude per cell
- Flight count distribution

### 5. **raw_telemetry**
Sampled raw data points (up to 1M records):
- Individual telemetry records
- Full flight state data
- Enriched with airline/route information

## 🚦 Quick Start

### Prerequisites
- Docker & Docker Compose v2.0+
- 8GB+ RAM recommended (16GB optimal)
- 20GB free disk space
- OpenSky API credentials (optional, for higher rate limits)

### 1. Clone & Configure
3-5 minutes for all services to initialize. The first startup will:
- Download PostgreSQL JDBC drivers
- Initialize Airflow database
- Create MinIO buckets (aviation-bronze, aviation-silver, aviation-gold)
- Start Spark streaming with auto-restart wrapper

**Check Service Health:**
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

All containers should show "Up" status
```bash
git clone https://github.com/sairam030/Aviation_Analytics.git
cd Aviation_Analytics
```

### 2. Download Historical Data (Optional)

Download OpenSky historical flight data for batch processing:

```bash
# Create data directory
mkdir -p /media/D/data/aviation_data/states

# Download from OpenSky (requires free account)
# Visit: https://opensky-network.org/datasets/states/
# Download parquet files and place in the states folder
```

**Data Source:** [OpenSky Network Datasets](https://opensky-network.org/datasets/states/)

> **Note:** The batch pipeline requires historical parquet files. Without them, only the real-time speed layer will work.

### 3. Set OpenSky Credentials (Optional)

Edit `src/speed/config.py`:
```python
OPENSKY_CLIENT_ID = "your-client-id"
OPENSKY_CLIENT_SECRET = "your-client-secret"
```

Get credentials at: [OpenSky Account](https://opensky-network.org/my-opensky/account)

### 4. Start Services

```bash
docker compose up -d
```

**Wait ~3-5 minutes for first startup.** The following happens automatically:

✅ **All driver Jars** - Downloaded during image build  
✅ **Airflow database** - Initialized automatically  
✅ **MinIO buckets** - Created (aviation-bronze, aviation-silver, aviation-gold)  
✅ **Spark streaming** - Auto-starts with restart wrapper  

**No manual intervention required!** All initialization is automated.
Pipelines

**In Airflow UI (http://localhost:8080):**

1. 📁 Project Structure

```
aviation_dataPipeline/
├── dags/                                    # Airflow DAGs
│   ├── aviation_states_pipeline.py         # Batch: Bronze → Silver ETL
│   ├── aviation_flight_mapping_pipeline.py # Batch: Flight enrichment
│   ├── speed_layer_silver_ingestion.py     # Speed: Hourly persistence
│   └── serving_layer_postgres_load.py      # Serving: Analytics tables
├── src/
│   ├── batch/                               # Batch processing
│   │   ├── bronze.py                        # Raw data ingestion
│   │   ├── extract_india_flights.py         # India-specific extraction
│   │   ├── enrich.py                        # Data enrichment
│   │   └── config.py                        # Batch configuration
│   ├── speed/                               # Real-time processing
│   │   ├── kafka_producer.py                # OpenSky → Kafka
│   │   ├── spark_streaming.py               # Spark structured streaming
│   │   ├── enrichment.py                    # Real-time enrichment
│   │   └── accumulator_state.py             # State management
│   ├── serving/                             # Analytics layer
│   │   └── analytics_loader.py              # PostgreSQL analytics tables
│   ├── webapp/                              # Flight tracker
│   │   ├── server.py                        # FastAPI server
│   │   ├── kafka_consumer.py                # Kafka consumer
│   │   └── static/                          # Web UI (HTML/JS/CSS)
│   └── utils/                               # Shared utilities
│       ├── spark_utils.py                   # Spark session management
│       ├── metadata_loader.py               # Reference data
│       └── metrics_utils.py                 # Pipeline metrics & logging
├── scripts/                                 # Operational scripts
│   ├── spark_master_entrypoint.sh           # Master startup + wrapper
│   ├── spark_streaming_wrapper.sh           # Auto-restart loop
│   └── start_spark_streaming.sh             # Streaming job launcher
├── docker-compose.yml                       # 15-service orchestration
├── Dockerfile                               # Spark + Python image
└── README.md                                # This file
```

## 🔄 Data Flow

### Batch Layer (Historical Processing)
```
Parquet Files → Bronze (MinIO) → Spark ETL → Silver (MinIO) → Gold (MinIO) → PostgreSQL
```
- *📈 Monitoring & Logs
### Comprehensive Pipeline Metrics

Each DAG automatically generates detailed metrics at completion:

```
📊 PIPELINE - COMPREHENSIVE METRICS REPORT
================================================================================
🔄 DAG RUN INFORMATION
  DAG ID:           analytics_serving_layer
  Run ID:           manual__2026-01-04T16:42:34+00:00
  Execution Date:   2026-01-04 16:42:34
  Duration:         245.67s (4.09m)

📋 TASK EXECUTION METRICS
  ✓ create_analytics_database        |  12.34s | success
  ✓ merge_to_gold_bucket              | 156.78s | success
  ✓ create_raw_telemetry              |  34.56s | success
  ✓ create_fact_flight_history        |  23.45s | success
  
  Total Task Time:     227.13s (3.79m)
  Successful Tasks:    7/7

💾 DATA LAYER STATISTICS
  🥈 Silver Layer (Source): aviation-silver
    Files:       1,234
    Total Size:  456.78 MB
    
  🥇 Gold Layer (Master): aviation-gold
    Files:       789
    Total Size:  234.56 MB

📈 DATA PROCESSING METRICS
  Task: merge_to_gold_bucket
    • gold_records:               2,370,307

⚡ PERFORMANCE SUMMARY
  Pipeline Efficiency:  92.5% (task time / wall clock time)
  Avg Task Duration:    32.45s

🏥 SYSTEM HEALTH CHECK
  MinIO Storage:    ✅ Healthy (3 buckets)
  Spark Cluster:    ✅ Healthy
  PostgreSQL:       ✅ Healthy

✅ PIPELINE EXECUTION COMPLETE
```

### Enhanced Task Logging

All processing tasks include detailed logging with timestamps:

```
  [12:34:56] ⚙️ Loading batch layer data...
  [12:35:23] ✅ Batch: 2,370,307 records
  [12:35:24] ⚙️ Loading speed layer data...
  [12:35:45] ✅ Speed: 156,789 records
  [12:35:46] ⚙️ Writing to Gold bucket...
  [12:36:12] ✅ Gold master created: 2,527,096 records
```

### Application Logs
```bash
# All services
docker compose logs -f

# Specific service
docker logs -f spark-master
docker logs -f flight-tracker
docker logs -f airflow-scheduler
```

### Metrics & Dashboards
- **Spark UI**: http://localhost:8081 - Job metrics, executors, stages
- **Kafka UI**: http://localhost:8084 - Topics, consumers, messages
- **Airflow**: http://localhost:8080 - DAG status, task logs, execution metrics
- **MinIO**: http://localhost:9001 - Storage usage, bucket stats

### Pipeline Execution Tracking

Each DAG run provides:
- **Task-level metrics**: Execution time per task
- **Data statistics**: Record counts, file sizes, storage utilization
- **System health**: MinIO, Spark, PostgreSQL status checks
- **Performance metrics**: Throughput, efficiency, average processing time
- **Error tracking**: Failed task identification and retry status

View metrics in Airflow task logs or the final `show_pipeline_metrics` task.

### Health Checks
```bash
# All containers status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Specific service health
docker compose ps spark-master
docker compose ps kafka

# Resource usage
docker stats --no-stream
```

## 🛑 Stopping Services

### Graceful Shutdown
```bash
docker compose down
```

### Full Cleanup (including data volumes)
```bash
docker compose down -v

# This removes:
# - All containers
# - MinIO data (Bronze/Silver/Gold)
# - PostgreSQL data
# - Kafka topics
# - Airflow metadata
```

### Restart Single Service
```bash
docker compose restart spark-master
docker compose restart flight-tracker
```



## 📝 License

MIT

## 🔗 Resources

- [OpenSky Network API](https://openskynetwork.github.io/opensky-api/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

## 📧 Support

For issues and questions:
1. Check [Troubleshooting](#-troubleshooting) section
2. Review logs: `docker compose logs -f`
3. Verify service health: `docker ps`
4. Check Airflow task logs in UI

---

**Built  using Lambda Architecture***Spark Streaming**: Structured streaming with auto-restart
- **Flight Tracker**: Consumes enriched data, streams to WebSocket clients
- **Persistence**: Hourly DAG saves enriched data to MinIO Silver

### Serving Layer (Analytics)
```
Silver (Batch + Speed) → Gold (Merged) → Spark Aggregations → PostgreSQL Tables → Metabase
```
- **Merge Strategy**: Full load (first run) or incremental (subsequent runs)
- **5 Analytics Tables**: See Analytics Tables section above
- **Schedule**: Every 6 hours via Airflow DAG
- **Query Layer**: Metabase dashboards connect to PostgreSQL

## 🐛 Troubleshooting

### Spark Streaming Not Running
```bash
# Check Spark master logs
docker logs spark-master | tail -50

# Should see: "Starting Spark streaming wrapper" and streaming app ID
# Visit http://localhost:8081 - should show active application
```

### Flight Tracker Shows No Data
```bash
# Check Kafka consumer connection
docker logs flight-tracker | grep -E "Consumer|Kafka"

# If "NoBrokersAvailable" error, restart container
docker compose restart flight-tracker

# Verify Kafka has data
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic aviation-enriched-states \
  --max-messages 1
```

### PostgreSQL Tables Empty
```bash
# Check if DAG ran successfully
docker logs airflow-scheduler | grep serving_layer

# Manually trigger analytics load
# In Airflow UI: serving_layer_postgres_load with load_batch_data=True

# Verify tables exist
docker exec postgres psql -U airflow -d analytics -c "\dt"
```

### MinIO Buckets Missing
```bash
# Recreate buckets
docker exec minio-create-buckets sh -c '
  mc alias set myminio http://minio:9000 minioadmin minioadmin
  mc mb myminio/aviation-bronze myminio/aviation-silver myminio/aviation-gold
'
```

### Out of Memory Errors
- Increase Docker memory allocation (recommended: 8GB minimum)
- Reduce Spark worker memory in [docker-compose.yml](docker-compose.yml)
- Check with: `docker stats`

## 🔧 Configuration

### Spark Streaming Auto-Restart
The Spark streaming job automatically restarts if it fails:
- **Wrapper Script**: [scripts/spark_streaming_wrapper.sh](scripts/spark_streaming_wrapper.sh)
- **Restart Delay**: 30s after graceful exit, 60s after error
- **Logs**: `docker logs spark-master`

### Kafka Topics Configuration
- **Retention**: 7 days
- **Partitions**: 1 (increase for higher throughput)
- **Replication**: 1 (single broker)

### Analytics Update Frequency
- **Serving Layer DAG**: Every 6 hours (`0 */6 * * *`)
- **Speed Layer Ingestion**: Every hour
- **Real-time Updates**: 5-second intervals

### OpenSky API Rate Limits
- **Anonymous**: 100 requests/day, ~400 credits/day
- **Authenticated**: 4,000 requests/day, 4,000 credits/day
- **Current Setup**: Uses mock data when rate-limitedrt loop
│   └── start_spark_streaming.sh             # Streaming job launcher
├── docker-compose.yml                       # 15-service orchestration
├── Dockerfile                               # Spark + Python image
└── README.md                                # This  streaming auto-starts on container launch

### 7. Verify Data Flow

**Check Kafka messages:**
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic aviation-enriched-states \
  --max-messages 5
```

**Check Spark streaming:**
```bash
docker logs spark-master | grep "Streaming"
```
Visit: http://localhost:8081 (should show active application)

**Check PostgreSQL tables:**
```bash
docker exec postgres psql -U airflow -d analytics -c "\dt"
```

**Check Flight Tracker:**
Visit: http://localhost:8050 (should show live flights on map)
| Application | URL | Credentials |
|-------------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Flight Tracker | http://localhost:8050 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8084 | - |
| Metabase | http://localhost:3000 | Setup on first visit |

### 6. Trigger Batch Pipeline

In Airflow UI, enable and trigger `aviation_states_pipeline` DAG.

## Project Structure

```
├── dags/                          # Airflow DAGs
│   ├── aviation_states_pipeline.py    # Batch ETL (Bronze → Silver)
│   ├── speed_layer_silver_ingestion.py # Speed layer hourly persistence
│   └── serving_layer_postgres_load.py  # Load to PostgreSQL
├── src/
│   ├── batch/                     # Batch layer code
│   ├── speed/                     # Speed layer (Kafka, enrichment)
│   ├── serving/                   # PostgreSQL loader
│   └── webapp/                    # Flight tracker web app
├── docker-compose.yml
└── Dockerfile
```

## Data Flow

1. **Batch Layer**: Historical data → Spark → MinIO Silver
2. **Speed Layer**: OpenSky API → Kafka → Enrichment → WebSocket + MinIO Silver
3. **Serving Layer**: Silver data → PostgreSQL → Metabase

## Stop Services

```bash
docker compose down
```

To remove all data:
```bash
docker compose down -v
```

## License

MIT
