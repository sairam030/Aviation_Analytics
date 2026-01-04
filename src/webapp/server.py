"""
Flight Tracker Server - Stateful Accumulator + WebSocket Streaming
- Receives enriched data from kafka_consumer
- Maintains stateful accumulator (update existing, add new)
- Streams to WebSocket for real-time map
- Exposes API for Airflow DAG to fetch/clear accumulated data
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

import threading
import queue

from src.speed.consumer_thread import start_consumer_thread


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
    
    def update(self, enriched_flight: dict) -> dict:
        """Update accumulator with enriched flight state (already enriched by Spark)."""
        icao24 = enriched_flight.get('icao24')
        if not icao24:
            return None
        
        now = datetime.utcnow().isoformat()
        
        with self.lock:
            self.stats['total_messages'] += 1
            
            if icao24 in self.flights:
                # Update existing - keep first_seen
                existing = self.flights[icao24]
                enriched_flight['first_seen'] = existing.get('first_seen', now)
                enriched_flight['last_seen'] = now
                enriched_flight['position_count'] = existing.get('position_count', 0) + 1
                self.stats['updates'] += 1
            else:
                # New flight - set tracking fields
                enriched_flight['first_seen'] = now
                enriched_flight['last_seen'] = now
                enriched_flight['position_count'] = 1
                self.stats['new_flights'] += 1
            
            self.flights[icao24] = enriched_flight
        
        return enriched_flight
    
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


def process_enriched_flights():
    """Background thread - receives enriched flights from kafka_consumer."""
    print("Starting consumer thread for enriched flights...")
    
    def message_callback(enriched_flight: dict):
        """Callback when enriched flight received from Kafka."""
        # Update accumulator (tracks state)
        tracked = accumulator.update(enriched_flight)
        
        if tracked:
            # Queue for WebSocket broadcast
            try:
                message_queue.put_nowait(tracked)
            except queue.Full:
                pass
    
    # Start consumer thread with callback
    start_consumer_thread(callback=message_callback)


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
    consumer_thread = threading.Thread(target=process_enriched_flights, daemon=True)
    consumer_thread.start()
    
    broadcast_task = asyncio.create_task(broadcast_messages())
    
    print("="*60)
    print("✈️  FLIGHT TRACKER SERVER STARTED")
    print("="*60)
    print("  ✓ Consuming enriched flights from Kafka")
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
