"""
Consumer Thread - Runs kafka_consumer and feeds data to server accumulator
Bridges kafka_consumer.py and server.py
"""
import threading
import queue
from src.speed.kafka_consumer import consume_stream


def start_consumer_thread(callback=None):
    """
    Start background thread that consumes from Kafka and calls callback.
    
    Args:
        callback: Function to call with each enriched flight
    """
    
    def consumer_worker():
        """Worker function for the consumer thread."""
        print("Starting Kafka consumer thread...")
        try:
            consume_stream(callback=callback)
        except Exception as e:
            print(f"Consumer thread error: {e}")
            import traceback
            traceback.print_exc()
    
    # Start consumer in background thread
    thread = threading.Thread(target=consumer_worker, daemon=True)
    thread.start()
    print("✓ Kafka consumer thread started")
    
    return thread
