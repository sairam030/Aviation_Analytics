"""
Kafka Consumer - Consumes flight data from Kafka
Two modes:
1. Real-time: Stream to WebSocket for live map
2. Batch: Collect hourly and write to MinIO
"""
import json
import time
from datetime import datetime
from typing import Callable, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.speed.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INDIA,
    KAFKA_CONSUMER_GROUP,
)


def create_consumer(
    group_id: str = None,
    auto_offset_reset: str = 'latest'
) -> KafkaConsumer:
    """Create Kafka consumer."""
    return KafkaConsumer(
        KAFKA_TOPIC_INDIA,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id or KAFKA_CONSUMER_GROUP,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000,  # Return after 1s if no messages
    )


def consume_realtime(callback: Callable[[dict], None]):
    """
    Real-time consumer - calls callback for each message.
    Use this for WebSocket streaming to frontend.
    
    Args:
        callback: Function to call with each flight state dict
    """
    print("="*60)
    print("AVIATION KAFKA CONSUMER (Real-time)")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_INDIA}")
    print("="*60)
    
    consumer = create_consumer(
        group_id=f"{KAFKA_CONSUMER_GROUP}-realtime",
        auto_offset_reset='latest'  # Only new messages
    )
    
    print("✓ Connected to Kafka")
    print("Streaming messages...")
    
    message_count = 0
    
    try:
        while True:
            # Poll for messages
            for message in consumer:
                state = message.value
                message_count += 1
                
                # Call the callback (e.g., send to WebSocket)
                callback(state)
                
                if message_count % 100 == 0:
                    print(f"  Processed {message_count:,} messages")
                    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        consumer.close()
        print(f"Consumer stopped. Total messages: {message_count:,}")


def consume_batch(duration_seconds: int = 3600) -> List[dict]:
    """
    Batch consumer - collects messages for a duration.
    Use this for hourly ingestion to MinIO.
    
    Args:
        duration_seconds: How long to collect (default 1 hour)
    
    Returns:
        List of flight state dictionaries
    """
    print("="*60)
    print(f"AVIATION KAFKA CONSUMER (Batch: {duration_seconds}s)")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_INDIA}")
    print("="*60)
    
    consumer = create_consumer(
        group_id=f"{KAFKA_CONSUMER_GROUP}-batch",
        auto_offset_reset='earliest'  # Get all available messages
    )
    
    print("✓ Connected to Kafka")
    
    states = []
    start_time = time.time()
    end_time = start_time + duration_seconds
    
    print(f"Collecting until {datetime.fromtimestamp(end_time).isoformat()}...")
    
    try:
        while time.time() < end_time:
            # Poll with timeout
            for message in consumer:
                states.append(message.value)
                
                if len(states) % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"  Collected {len(states):,} messages ({elapsed:.0f}s)")
                
                if time.time() >= end_time:
                    break
                    
    except KeyboardInterrupt:
        print("\n\nStopping collection...")
    finally:
        consumer.close()
    
    print(f"\n✓ Collected {len(states):,} messages in {duration_seconds}s")
    return states


def print_message(state: dict):
    """Simple callback that prints flight info."""
    callsign = state.get('callsign', '').strip() or 'N/A'
    icao24 = state.get('icao24', 'N/A')
    lat = state.get('latitude', 0)
    lon = state.get('longitude', 0)
    alt = state.get('baro_altitude', 0) or 0
    vel = state.get('velocity', 0) or 0
    
    print(f"  ✈️  {callsign:8s} | {icao24} | {lat:.2f}, {lon:.2f} | {alt:.0f}m | {vel:.0f}m/s")


if __name__ == "__main__":
    # Demo: print real-time messages
    consume_realtime(callback=print_message)
