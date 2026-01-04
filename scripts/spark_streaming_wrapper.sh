#!/bin/bash
# Auto-restart wrapper for Spark Streaming

LOG_FILE="/opt/airflow/logs/spark_streaming.log"

echo "============================================"
echo "Spark Streaming Auto-Restart Wrapper"
echo "============================================"

while true; do
    echo "[$(date)] Starting Spark Streaming attempt..."
    
    # Clear old log for fresh start
    rm -f "$LOG_FILE"
    
    # Run the streaming script
    bash /opt/airflow/scripts/start_spark_streaming.sh 2>&1 | tee "$LOG_FILE"
    
    EXIT_CODE=$?
    echo "[$(date)] Spark Streaming exited with code $EXIT_CODE" | tee -a "$LOG_FILE"
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Clean exit - restarting in 30s..." | tee -a "$LOG_FILE"
        sleep 30
    else
        echo "Error exit - retrying in 60s..." | tee -a "$LOG_FILE"
        sleep 60
    fi
done
