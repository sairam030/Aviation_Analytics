#!/bin/bash
# Start Spark Streaming Enrichment Job

echo "============================================"
echo "Starting Spark Streaming Enrichment Service"
echo "============================================"

# Wait for Kafka to be ready - simple sleep approach
echo "[$(date)] Waiting for Kafka and topics to be ready..."
echo "  Waiting 40 seconds for Kafka initialization..."
sleep 40
echo "✓ Kafka should be ready now"

# Change to airflow directory
cd /opt/airflow || exit 1

echo "[$(date)] Submitting Spark Streaming job..."

# Submit the streaming job (JARs already in /opt/spark/jars/)
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  src/speed/spark_streaming_enrichment_sql.py

# If spark-submit exits, print error
echo "[$(date)] Spark Streaming job exited with code $?"
tail -100 /opt/airflow/logs/spark_streaming.log
