"""
Mock Data Generator - Generates realistic flight data for testing
Simulates OpenSky API response format
"""
import random
import time
from datetime import datetime

# Indian airports with coordinates
AIRPORTS = {
    "DEL": {"lat": 28.5665, "lon": 77.1031, "city": "Delhi"},
    "BOM": {"lat": 19.0896, "lon": 72.8656, "city": "Mumbai"},
    "BLR": {"lat": 13.1986, "lon": 77.7066, "city": "Bangalore"},
    "MAA": {"lat": 12.9941, "lon": 80.1709, "city": "Chennai"},
    "CCU": {"lat": 22.6547, "lon": 88.4467, "city": "Kolkata"},
    "HYD": {"lat": 17.2403, "lon": 78.4294, "city": "Hyderabad"},
    "COK": {"lat": 10.1520, "lon": 76.3987, "city": "Kochi"},
    "GOI": {"lat": 15.3808, "lon": 73.8314, "city": "Goa"},
    "AMD": {"lat": 23.0772, "lon": 72.6347, "city": "Ahmedabad"},
    "PNQ": {"lat": 18.5822, "lon": 73.9197, "city": "Pune"},
}

# Indian airlines
AIRLINES = {
    "IGO": {"name": "IndiGo", "callsign_prefix": "IGO"},
    "AIC": {"name": "Air India", "callsign_prefix": "AIC"},
    "VTI": {"name": "Vistara", "callsign_prefix": "VTI"},
    "SEJ": {"name": "SpiceJet", "callsign_prefix": "SEJ"},
    "AKJ": {"name": "Akasa Air", "callsign_prefix": "AKJ"},
}

# Flight state tracking for continuity
_flight_states = {}

# ─── Route data loaded from CSV ──────────────────────────────────────────────
# Maps ICAO prefix -> list of (numeric_flight_number, origin, destination)
_ROUTE_DATA = {}
_ROUTE_BY_CALLSIGN = {}  # callsign -> (origin, destination)

def _load_routes():
    """Load actual flight numbers from routes CSV (called once at import)."""
    global _ROUTE_DATA, _ROUTE_BY_CALLSIGN
    import csv, re, os
    
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'routes.csv')
    if not os.path.exists(csv_path):
        # Try container path
        csv_path = '/opt/airflow/data/routes.csv'
    if not os.path.exists(csv_path):
        print("⚠️ routes.csv not found, using fallback flight numbers")
        return
    
    iata_to_icao = {
        '6E': 'IGO', 'AI': 'AIC', 'SG': 'SEJ', 'QP': 'AKJ',
        'UK': 'VTI', 'IX': 'IAD', 'G8': 'GOW', '9I': 'LLR',
    }
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            fn = row.get('FlightNo', '')
            origin = row.get('Origin', '')
            dest = row.get('Destination', '')
            if not fn or not origin or not dest or origin == dest:
                continue
            
            num_match = re.search(r'(\d+)$', fn)
            if not num_match:
                continue
            
            iata = fn[:2]
            icao = iata_to_icao.get(iata)
            if not icao or icao not in AIRLINES:
                continue
            
            flight_num = num_match.group(1)
            callsign = f"{icao}{flight_num}"
            
            if icao not in _ROUTE_DATA:
                _ROUTE_DATA[icao] = []
            _ROUTE_DATA[icao].append((flight_num, origin, dest))
            _ROUTE_BY_CALLSIGN[callsign] = (origin, dest)
    
    for prefix, routes in _ROUTE_DATA.items():
        # Deduplicate
        _ROUTE_DATA[prefix] = list({r[0]: r for r in routes}.values())
    
    total = sum(len(v) for v in _ROUTE_DATA.values())
    print(f"✓ Loaded {total} routes for mock data ({', '.join(f'{k}:{len(v)}' for k,v in _ROUTE_DATA.items())})")

# Load routes at module import
_load_routes()


def generate_icao24():
    """Generate a realistic ICAO24 transponder address."""
    # Indian aircraft typically start with 8, A, or VT
    prefixes = ['800', '801', '840', 'a80', 'vt0']
    prefix = random.choice(prefixes)
    suffix = ''.join(random.choices('0123456789abcdef', k=3))
    return prefix + suffix


def generate_callsign():
    """Generate airline callsign using actual flight numbers from routes CSV.
    
    Callsign format: {ICAO_PREFIX}{NUMERIC_FLIGHT_NUMBER}
    E.g., CSV has '6E102' → Spark extracts '102' → callsign 'IGO102'
    """
    # Pick from airlines that have routes loaded
    available = [k for k in AIRLINES if k in _ROUTE_DATA and _ROUTE_DATA[k]]
    if not available:
        # Fallback if CSV not loaded
        airline_code = random.choice(list(AIRLINES.keys()))
        return f"{airline_code}{random.randint(100, 999)}"
    
    airline_code = random.choice(available)
    route = random.choice(_ROUTE_DATA[airline_code])
    flight_num = route[0]  # numeric part
    
    return f"{airline_code}{flight_num}"


def generate_route():
    """Generate origin and destination airports."""
    airports = list(AIRPORTS.keys())
    origin = random.choice(airports)
    destination = random.choice([a for a in airports if a != origin])
    return origin, destination


def interpolate_position(origin, destination, progress):
    """Interpolate position between origin and destination."""
    orig = AIRPORTS[origin]
    dest = AIRPORTS[destination]
    
    lat = orig["lat"] + (dest["lat"] - orig["lat"]) * progress
    lon = orig["lon"] + (dest["lon"] - orig["lon"]) * progress
    
    return lat, lon


def calculate_heading(lat1, lon1, lat2, lon2):
    """Calculate heading between two points."""
    import math
    
    dlon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    
    heading = math.degrees(math.atan2(x, y))
    return (heading + 360) % 360


def generate_flight_state(icao24=None, existing_state=None):
    """Generate a single flight state vector."""
    current_time = int(time.time())
    
    if existing_state:
        # Update existing flight
        state = existing_state.copy()
        
        # Random progress increment (flights move at different speeds)
        progress_increment = random.uniform(0.015, 0.035)  # Varied flight speeds
        state["progress"] = min(state["progress"] + progress_increment, 1.0)
        
        # Update position
        lat, lon = interpolate_position(
            state["origin"], 
            state["destination"], 
            state["progress"]
        )
        
        # Add some randomness to position (wind, course corrections)
        lat += random.uniform(-0.02, 0.02)
        lon += random.uniform(-0.02, 0.02)
        
        # Altitude changes during flight with randomness
        if state["progress"] < 0.15:  # Taking off
            base_altitude = 500 + (state["cruise_altitude"] - 500) * (state["progress"] / 0.15)
            altitude = base_altitude + random.uniform(-200, 200)
            vertical_rate = random.uniform(5, 15)  # Climbing
        elif state["progress"] > 0.85:  # Landing
            base_altitude = 500 + (state["cruise_altitude"] - 500) * ((1.0 - state["progress"]) / 0.15)
            altitude = base_altitude + random.uniform(-200, 200)
            vertical_rate = random.uniform(-15, -5)  # Descending
        else:  # Cruising with turbulence
            altitude = state["cruise_altitude"] + random.uniform(-500, 500)
            vertical_rate = random.uniform(-3, 3)  # Small variations
        
        # Calculate heading with some variation
        base_heading = calculate_heading(
            state["last_lat"], state["last_lon"],
            lat, lon
        )
        heading = base_heading + random.uniform(-10, 10)  # Course adjustments
        heading = (heading + 360) % 360
        
        state["last_lat"] = lat
        state["last_lon"] = lon
        
        # Check if flight completed
        if state["progress"] >= 1.0:
            on_ground = True
            velocity = random.uniform(0, 50)  # Taxiing
            vertical_rate = 0.0
            altitude = 500.0
        else:
            on_ground = False
            # Velocity varies by flight phase and aircraft
            if state["progress"] < 0.15:  # Taking off
                velocity = state["base_velocity"] * (0.3 + 0.7 * (state["progress"] / 0.15))
            elif state["progress"] > 0.85:  # Landing
                velocity = state["base_velocity"] * (0.3 + 0.7 * ((1.0 - state["progress"]) / 0.15))
            else:  # Cruising
                velocity = state["base_velocity"] + random.uniform(-20, 20)
        
        return [
            state["icao24"],                    # 0: icao24
            state["callsign"],                  # 1: callsign
            "India",                            # 2: origin_country
            current_time,                       # 3: time_position
            current_time,                       # 4: last_contact
            lon,                                # 5: longitude
            lat,                                # 6: latitude
            altitude,                           # 7: baro_altitude
            on_ground,                          # 8: on_ground
            velocity,                           # 9: velocity
            heading,                            # 10: true_track
            vertical_rate,                      # 11: vertical_rate
            None,                               # 12: sensors
            altitude + random.uniform(-100, 100), # 13: geo_altitude
            None,                               # 14: squawk
            False,                              # 15: spi
            0,                                  # 16: position_source (ADS-B)
        ]
    else:
        # Create new flight with random starting state
        icao24 = icao24 or generate_icao24()
        callsign = generate_callsign()
        origin, destination = generate_route()
        
        # Random starting progress (some flights already in progress)
        start_progress = random.choice([0.0, 0.0, 0.0, random.uniform(0.1, 0.7)])
        
        # Random cruise parameters
        cruise_altitude = random.uniform(8000, 12000)  # Varied altitudes
        base_velocity = random.uniform(180, 260)  # Different aircraft speeds
        
        # Calculate initial position
        lat, lon = interpolate_position(origin, destination, start_progress)
        lat += random.uniform(-0.05, 0.05)
        lon += random.uniform(-0.05, 0.05)
        
        # Determine initial altitude and state
        if start_progress == 0.0:
            # On ground, ready for takeoff
            orig_coord = AIRPORTS[origin]
            lat = orig_coord["lat"] + random.uniform(-0.05, 0.05)
            lon = orig_coord["lon"] + random.uniform(-0.05, 0.05)
            initial_altitude = 500.0
            on_ground = True
            initial_velocity = 0.0
        else:
            # Already in flight
            initial_altitude = cruise_altitude + random.uniform(-500, 500)
            on_ground = False
            initial_velocity = base_velocity + random.uniform(-20, 20)
        
        # New flight state
        state = {
            "icao24": icao24,
            "callsign": callsign,
            "origin": origin,
            "destination": destination,
            "progress": start_progress,
            "cruise_altitude": cruise_altitude,
            "base_velocity": base_velocity,
            "last_lat": lat,
            "last_lon": lon,
        }
        
        _flight_states[icao24] = state
        
        # Calculate initial heading
        dest_coord = AIRPORTS[destination]
        initial_heading = calculate_heading(lat, lon, dest_coord["lat"], dest_coord["lon"])
        initial_heading += random.uniform(-15, 15)
        
        return [
            icao24,                             # 0: icao24
            callsign,                           # 1: callsign
            "India",                            # 2: origin_country
            current_time,                       # 3: time_position
            current_time,                       # 4: last_contact
            lon,                                # 5: longitude
            lat,                                # 6: latitude
            initial_altitude,                   # 7: baro_altitude
            on_ground,                          # 8: on_ground
            initial_velocity,                   # 9: velocity
            initial_heading % 360,              # 10: true_track
            0 if on_ground else random.uniform(-5, 5),  # 11: vertical_rate
            None,                               # 12: sensors
            initial_altitude + random.uniform(-50, 50), # 13: geo_altitude
            None,                               # 14: squawk
            False,                              # 15: spi
            0,                                  # 16: position_source
        ]


def generate_mock_data(num_flights=20):
    """Generate mock OpenSky API response with dynamic flight count."""
    global _flight_states
    
    # Clean up completed flights (with some randomness)
    for icao24, state in list(_flight_states.items()):
        if state.get("progress", 0) >= 1.0:
            # Keep landed flights visible for a bit before removing
            if random.random() > 0.3:  # 70% chance to remove
                del _flight_states[icao24]
    
    # Vary the target number of flights slightly for realism
    target_flights = random.randint(int(num_flights * 0.8), int(num_flights * 1.2))
    
    # Maintain some existing flights and add new ones
    existing_count = len(_flight_states)
    new_count = max(0, target_flights - existing_count)
    
    states = []
    
    # Update existing flights
    for icao24, state in list(_flight_states.items()):
        flight_state = generate_flight_state(existing_state=state)
        states.append(flight_state)
        
        # Update the stored state
        _flight_states[icao24] = state
    
    # Add new flights with randomness
    for _ in range(new_count):
        # Occasionally skip adding a flight for variety
        if random.random() > 0.1:  # 90% chance to add
            states.append(generate_flight_state())
    
    return {
        "time": int(time.time()),
        "states": states
    }


if __name__ == "__main__":
    # Test the generator
    import json
    
    print("Generating mock flight data...")
    data = generate_mock_data(10)
    
    print(f"\nGenerated {len(data['states'])} flights")
    print(f"Timestamp: {data['time']}")
    print(f"\nSample flight:")
    print(json.dumps(data['states'][0], indent=2, default=str))
