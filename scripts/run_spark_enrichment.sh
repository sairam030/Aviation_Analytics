#!/bin/bash
# Run Spark Streaming Enrichment Job
# Reads from aviation-india-states (raw), enriches, writes to aviation-enriched-states

echo "Starting Spark Streaming Enrichment Service..."
echo "Input: aviation-india-states (raw)"
echo "Output: aviation-enriched-states (enriched)"

cd /opt/airflow

# Wait for Kafka to be ready
echo "Waiting for Kafka..."
sleep 10

# Run the Spark Streaming job
spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  src/speed/spark_streaming_enrichment.py
