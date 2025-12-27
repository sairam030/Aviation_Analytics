# Aviation Analytics Pipeline

Real-time and batch aviation data processing pipeline using Lambda Architecture.

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
                        │              │  (5s interval)  │         │  (Batch Load)   │              │
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

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| **Airflow Webserver** | 8080 | DAG management & monitoring |
| **Flight Tracker** | 8050 | Real-time flight map |
| **Spark Master UI** | 8081 | Spark cluster monitoring |
| **Kafka UI** | 8084 | Kafka topics & consumers |
| **MinIO Console** | 9001 | Object storage (S3-compatible) |
| **Metabase** | 3000 | Analytics dashboards |
| **PostgreSQL** | 5432 | Database |
| **Kafka** | 9092 | Message broker |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- OpenSky API credentials (optional, for higher rate limits)

### 1. Clone & Configure

```bash
git clone https://github.com/sairam030/Aviation_Analytics.git
cd Aviation_Analytics
```

### 2. Set OpenSky Credentials (Optional)

Edit `src/speed/config.py`:
```python
OPENSKY_CLIENT_ID = "your-client-id"
OPENSKY_CLIENT_SECRET = "your-client-secret"
```

### 3. Start Services

```bash
docker compose up -d
```

Wait ~2 minutes for all services to initialize.

### 4. Access the Applications

| Application | URL | Credentials |
|-------------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Flight Tracker | http://localhost:8050 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8084 | - |
| Metabase | http://localhost:3000 | Setup on first visit |

### 5. Trigger Batch Pipeline

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
