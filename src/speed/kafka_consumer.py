"""
Kafka Consumer - Consumes enriched flight data from Kafka
Reads from aviation-enriched-states topic (data already enriched by Spark)
Passes data to server.py for accumulation and WebSocket streaming
"""
import json
from typing import Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.speed.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_ENRICHED,
    KAFKA_CONSUMER_GROUP,
)


def create_consumer(group_id: str = None) -> KafkaConsumer:
    """Create Kafka consumer for enriched data."""
    return KafkaConsumer(
        KAFKA_TOPIC_ENRICHED,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id or KAFKA_CONSUMER_GROUP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000,
    )


def consume_stream(callback: Callable[[dict], None]):
    """
    Stream consumer - calls callback for each enriched message.
    Data is already enriched with route information from Spark.
    
    Args:
        callback: Function to call with each enriched flight state
    """
    print("="*60)
    print("KAFKA CONSUMER - Enriched Flight Data")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_ENRICHED}")
    print(f"Group: {KAFKA_CONSUMER_GROUP}")
    print("="*60)
    
    consumer = create_consumer()
    
    print("✓ Connected to Kafka")
    print("✓ Consuming enriched data...")
    
    message_count = 0
    
    try:
        while True:
            for message in consumer:
                enriched_flight = message.value
                message_count += 1
                
                # Call the callback (passes to server)
                callback(enriched_flight)
                
                if message_count % 100 == 0:
                    print(f"  Processed {message_count:,} enriched messages")
                    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        consumer.close()
        print(f"Consumer stopped. Total messages: {message_count:,}")


if __name__ == "__main__":
    # Demo: print enriched messages
    def print_flight(flight: dict):
        callsign = flight.get('callsign', '').strip() or 'N/A'
        origin = flight.get('origin_code', 'N/A')
        dest = flight.get('destination_code', 'N/A')
        airline = flight.get('airline_name', 'N/A')
        print(f"  ✈️  {callsign:8s} | {airline:15s} | {origin} → {dest}")
    
    consume_stream(callback=print_flight)
