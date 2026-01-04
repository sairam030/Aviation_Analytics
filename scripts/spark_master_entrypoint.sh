#!/bin/bash
set -e

echo "============================================"
echo "Starting Spark Master Container"
echo "============================================"

# Start Spark Master in background (it will daemonize itself)
echo "[$(date)] Starting Spark Master..."
/opt/spark/sbin/start-master.sh -h spark-master &
MASTER_PID=$!

# Wait for master to initialize
echo "[$(date)] Waiting for Spark Master to initialize..."
sleep 10

# Wait for services to initialize
echo "[$(date)] Waiting for initialization (45 seconds)..."
sleep 45

# Start Spark Streaming in background
echo "[$(date)] Starting Spark Streaming wrapper..."
nohup bash /opt/airflow/scripts/spark_streaming_wrapper.sh > /opt/airflow/logs/wrapper.log 2>&1 &
WRAPPER_PID=$!
echo "[$(date)] Spark Streaming wrapper started with PID: $WRAPPER_PID"

# Keep container alive
echo "[$(date)] Spark Master initialization complete"
echo "============================================"
sleep infinity
