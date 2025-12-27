"""
Flight Tracker Server - Unified Real-time + Stateful Accumulator
- Consumes from Kafka
- Enriches with route mapping (origin/destination)
- Maintains stateful accumulator (update existing, add new)
- Streams to WebSocket
- Exposes API for Airflow DAG to trigger hourly Bronze persistence
"""
import json
import asyncio
import os
from datetime import datetime
from typing import Set, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import threading
import queue

from src.speed.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INDIA,
)
from src.speed.route_mapping import enrich_flight_data


# =============================================================================
# CONFIGURATION
# =============================================================================

PERSISTENCE_API_KEY = os.getenv("PERSISTENCE_API_KEY", "aviation-secret-key")


# =============================================================================
# STATEFUL ACCUMULATOR
# =============================================================================

class FlightAccumulator:
    """
    Stateful flight accumulator - maintains current state of all flights.
    - Update existing flights with latest position
    - Add new flights when first seen
    - Track first_seen, last_seen, position_count
    """
    
    def __init__(self):
        self.flights: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.stats = {
            "total_messages": 0,
            "updates": 0,
            "new_flights": 0,
        }
    
    def update(self, flight: dict) -> dict:
        """Update flight state - add new or update existing."""
        icao24 = flight.get('icao24')
        if not icao24:
            return None
        
        now = datetime.utcnow().isoformat()
        
        # Enrich with route data (airline, origin, destination)
        enriched = enrich_flight_data(flight.copy())
        
        with self.lock:
            self.stats['total_messages'] += 1
            
            if icao24 in self.flights:
                # Update existing - keep first_seen
                existing = self.flights[icao24]
                enriched['first_seen'] = existing.get('first_seen', now)
                enriched['last_seen'] = now
                enriched['position_count'] = existing.get('position_count', 0) + 1
                self.stats['updates'] += 1
            else:
                # New flight
                enriched['first_seen'] = now
                enriched['last_seen'] = now
                enriched['position_count'] = 1
                self.stats['new_flights'] += 1
            
            self.flights[icao24] = enriched
        
        return enriched
    
    def get_all(self) -> list:
        """Get all current flights."""
        with self.lock:
            return list(self.flights.values())
    
    def get_for_persistence(self) -> list:
        """Get flights data for Bronze persistence."""
        with self.lock:
            return [f.copy() for f in self.flights.values()]
    
    def clear(self):
        """Clear all state (after persistence)."""
        with self.lock:
            count = len(self.flights)
            self.flights.clear()
            self.stats = {"total_messages": 0, "updates": 0, "new_flights": 0}
            return count
    
    def get_stats(self) -> dict:
        with self.lock:
            return {**self.stats, "unique_flights": len(self.flights)}


# Global instances
accumulator = FlightAccumulator()
message_queue = queue.Queue(maxsize=1000)
active_connections: Set[WebSocket] = set()


def kafka_consumer_thread():
    """Background thread - consume from Kafka, enrich, accumulate."""
    print("Starting Kafka consumer thread...")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INDIA,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='flight-tracker-consumer',  # Required for Kafka UI visibility
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000,
        )
        print(f"✓ Connected to Kafka: {KAFKA_TOPIC_INDIA}")
        print(f"✓ Consumer group: flight-tracker-consumer")
        print("✓ Route enrichment: Active")
        print("✓ Stateful accumulator: Active")
        
        while True:
            for message in consumer:
                flight = message.value
                
                # Update accumulator (enriches + tracks state)
                enriched = accumulator.update(flight)
                
                if enriched:
                    # Queue for WebSocket broadcast
                    try:
                        message_queue.put_nowait(enriched)
                    except queue.Full:
                        pass
                        
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        import traceback
        traceback.print_exc()


async def broadcast_messages():
    """Broadcast Kafka messages to all connected WebSocket clients."""
    while True:
        try:
            # Get message from queue (non-blocking)
            try:
                state = message_queue.get_nowait()
                
                # Broadcast to all connected clients
                if active_connections:
                    message = json.dumps({
                        "type": "flight_update",
                        "data": state,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
                    disconnected = set()
                    for ws in active_connections:
                        try:
                            await ws.send_text(message)
                        except:
                            disconnected.add(ws)
                    
                    # Remove disconnected clients
                    for ws in disconnected:
                        active_connections.discard(ws)
                        
            except queue.Empty:
                pass
                
        except Exception as e:
            print(f"Broadcast error: {e}")
            
        await asyncio.sleep(0.1)  # Small delay


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    broadcast_task = asyncio.create_task(broadcast_messages())
    
    print("="*60)
    print("✈️  FLIGHT TRACKER SERVER STARTED")
    print("="*60)
    print("  ✓ Kafka consumer with route enrichment")
    print("  ✓ Stateful accumulator (update/add flights)")
    print("  ✓ WebSocket real-time streaming")
    print("  ✓ REST API for Airflow DAG persistence")
    print("="*60)
    
    yield
    
    broadcast_task.cancel()
    print("Server shutdown")


app = FastAPI(
    title="Aviation Flight Tracker",
    description="Real-time India flight tracking",
    lifespan=lifespan
)

# Serve static files
app.mount("/static", StaticFiles(directory="/opt/airflow/src/webapp/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home():
    """Serve the main flight tracker page."""
    return FileResponse("/opt/airflow/src/webapp/templates/index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time flight updates."""
    await websocket.accept()
    active_connections.add(websocket)
    
    print(f"Client connected. Total: {len(active_connections)}")
    
    try:
        # Send current accumulated flights on connect
        flights = accumulator.get_all()
        await websocket.send_text(json.dumps({
            "type": "initial_data",
            "data": flights,
            "count": len(flights),
            "timestamp": datetime.utcnow().isoformat()
        }))
        
        # Keep connection alive
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "ping"}))
                
    except WebSocketDisconnect:
        pass
    finally:
        active_connections.discard(websocket)
        print(f"Client disconnected. Total: {len(active_connections)}")


@app.get("/api/flights")
async def get_flights():
    """REST endpoint to get current flight data."""
    flights = accumulator.get_all()
    return {
        "flights": flights,
        "count": len(flights),
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/stats")
async def get_stats():
    """Get statistics about current flights."""
    flights = accumulator.get_all()
    acc_stats = accumulator.get_stats()
    
    on_ground = sum(1 for f in flights if f.get('on_ground', False))
    in_air = len(flights) - on_ground
    
    countries = {}
    for f in flights:
        country = f.get('origin_country', 'Unknown')
        countries[country] = countries.get(country, 0) + 1
    
    # Airline stats (from enrichment)
    airlines = {}
    for f in flights:
        airline = f.get('airline_name')
        if airline:
            airlines[airline] = airlines.get(airline, 0) + 1
    
    # Route stats
    routes = {}
    for f in flights:
        origin = f.get('origin_code')
        dest = f.get('destination_code')
        if origin and dest and origin != 'UNK':
            route = f"{origin}-{dest}"
            routes[route] = routes.get(route, 0) + 1
    
    return {
        "total": len(flights),
        "in_air": in_air,
        "on_ground": on_ground,
        "by_country": countries,
        "by_airline": airlines,
        "by_route": dict(sorted(routes.items(), key=lambda x: -x[1])[:10]),
        "accumulator": acc_stats,
        "connections": len(active_connections),
        "timestamp": datetime.utcnow().isoformat()
    }


# =============================================================================
# PERSISTENCE API (Called by Airflow DAG)
# =============================================================================

@app.get("/api/accumulator/data")
async def get_accumulator_data(api_key: str = None):
    """Get accumulated flight data for persistence (called by Airflow DAG)."""
    if api_key != PERSISTENCE_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    flights = accumulator.get_for_persistence()
    stats = accumulator.get_stats()
    
    return {
        "flights": flights,
        "count": len(flights),
        "stats": stats,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/api/accumulator/clear")
async def clear_accumulator(api_key: str = None):
    """Clear accumulated state after persistence (called by Airflow DAG)."""
    if api_key != PERSISTENCE_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    cleared_count = accumulator.clear()
    print(f"✓ Accumulator cleared: {cleared_count} flights")
    
    return {
        "status": "cleared",
        "cleared_count": cleared_count,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "flights": len(accumulator.flights),
        "connections": len(active_connections),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8050)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8050)
