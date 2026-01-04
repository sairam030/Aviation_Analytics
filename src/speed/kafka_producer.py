"""
Kafka Producer - Fetches flight data from OpenSky API and sends to Kafka
"""
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from src.speed.config import (
    OPENSKY_API_URL,
    INDIA_BBOX,
    USE_MOCK_DATA,
    FETCH_INTERVAL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    OPENSKY_FIELDS,
)
from src.speed.mock_data_generator import generate_mock_data


def create_topic():
    """Create Kafka topic if it doesn't exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topic = NewTopic(
            name=KAFKA_TOPIC_RAW,
            num_partitions=3,
            replication_factor=1
        )
        admin.create_topics([topic])
        print(f"✓ Created topic: {KAFKA_TOPIC_RAW}")
    except TopicAlreadyExistsError:
        print(f"✓ Topic exists: {KAFKA_TOPIC_RAW}")
    except Exception as e:
        print(f"⚠️ Topic creation: {e}")


def create_producer() -> KafkaProducer:
    """Create Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
    )


def fetch_opensky_data() -> dict:
    """Fetch flight states from OpenSky API or mock data."""
    
    # Use mock data if enabled
    if USE_MOCK_DATA:
        print("  Using MOCK data (OpenSky API unavailable)")
        try:
            return generate_mock_data(num_flights=25)
        except Exception as e:
            print(f"  ⚠️ Mock data generation failed: {e}")
            return None
    
    # Try real OpenSky API
    params = INDIA_BBOX.copy()
    
    try:
        response = requests.get(
            OPENSKY_API_URL,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"  ⚠️ Rate limit exceeded (429)")
            if 'X-Rate-Limit-Retry-After-Seconds' in e.response.headers:
                retry_after = e.response.headers['X-Rate-Limit-Retry-After-Seconds']
                print(f"  Retry after {retry_after} seconds")
        else:
            print(f"  ⚠️ HTTP Error {e.response.status_code}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching OpenSky data: {e}")
        print("  💡 Tip: Set USE_MOCK_DATA=true to use simulated data")
        return None


def parse_state_vector(state_array: list) -> dict:
    """Convert OpenSky state array to dictionary."""
    if not state_array or len(state_array) < len(OPENSKY_FIELDS):
        return None
    
    state = {}
    for i, field in enumerate(OPENSKY_FIELDS):
        state[field] = state_array[i]
    
    # Add metadata
    state['ingestion_time'] = datetime.utcnow().isoformat()
    
    return state


def run_producer():
    """Main producer loop - fetches data and sends to Kafka."""
    print("="*60)
    print("AVIATION KAFKA PRODUCER")
    print(f"API: {OPENSKY_API_URL}")
    print(f"Region: India ({INDIA_BBOX})")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_RAW} (raw data)")
    print(f"Interval: {FETCH_INTERVAL}s")
    print("="*60)
    
    # Create topic if needed
    create_topic()
    
    producer = create_producer()
    print("✓ Kafka producer connected")
    
    fetch_count = 0
    total_flights = 0
    
    try:
        while True:
            fetch_count += 1
            print(f"\n[{datetime.utcnow().isoformat()}] Fetch #{fetch_count}")
            
            # Fetch from OpenSky
            data = fetch_opensky_data()
            
            if not data or 'states' not in data or not data['states']:
                print("  ⚠️ No data received")
                time.sleep(FETCH_INTERVAL)
                continue
            
            states = data['states']
            
            print(f"  Received {len(states)} flights")
            
            # Send each flight state to Kafka
            sent = 0
            for state_array in states:
                state = parse_state_vector(state_array)
                if not state:
                    continue
                
                # Use icao24 as key for partitioning (same aircraft always to same partition)
                key = state.get('icao24', '')
                
                try:
                    future = producer.send(KAFKA_TOPIC_RAW, key=key, value=state)
                    # Don't block on every message, but track
                    sent += 1
                except KafkaError as e:
                    print(f"  Error sending to Kafka: {e}")
            
            # Flush to ensure all messages are sent
            producer.flush()
            
            total_flights += sent
            print(f"  ✓ Sent {sent} messages to Kafka (Total: {total_flights:,})")
            
            # Wait before next fetch
            time.sleep(FETCH_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        producer.close()
        print(f"Producer stopped. Total messages sent: {total_flights:,}")


if __name__ == "__main__":
    run_producer()
