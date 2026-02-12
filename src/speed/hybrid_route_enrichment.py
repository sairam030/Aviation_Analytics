"""
Hybrid Route Enrichment Strategy
Tries multiple methods to determine flight routes:
1. Exact route database lookup (most accurate)
2. Geographic proximity detection (fallback)
3. Pattern-based heuristics (last resort)
"""
from typing import Optional, Tuple, Dict, Any
from src.speed.route_database import get_route_from_database, EXACT_ROUTES
from src.speed.geographic_router import find_nearest_airport, haversine_distance


# Indian Airports (from existing data)
INDIAN_AIRPORTS = {
    "DEL": {"name": "Indira Gandhi International", "city": "Delhi", "lat": 28.5562, "lon": 77.1000},
    "BOM": {"name": "Chhatrapati Shivaji Maharaj", "city": "Mumbai", "lat": 19.0896, "lon": 72.8656},
    "BLR": {"name": "Kempegowda International", "city": "Bangalore", "lat": 13.1986, "lon": 77.7066},
    "MAA": {"name": "Chennai International", "city": "Chennai", "lat": 12.9941, "lon": 80.1709},
    "CCU": {"name": "Netaji Subhas Chandra Bose", "city": "Kolkata", "lat": 22.6547, "lon": 88.4467},
    "HYD": {"name": "Rajiv Gandhi International", "city": "Hyderabad", "lat": 17.2403, "lon": 78.4294},
    "COK": {"name": "Cochin International", "city": "Kochi", "lat": 10.1520, "lon": 76.4019},
    "GOI": {"name": "Goa International", "city": "Goa", "lat": 15.3808, "lon": 73.8314},
    "AMD": {"name": "Sardar Vallabhbhai Patel", "city": "Ahmedabad", "lat": 23.0772, "lon": 72.6347},
    "PNQ": {"name": "Pune Airport", "city": "Pune", "lat": 18.5822, "lon": 73.9197},
    "JAI": {"name": "Jaipur International", "city": "Jaipur", "lat": 26.8242, "lon": 75.8122},
    "IXC": {"name": "Chandigarh International", "city": "Chandigarh", "lat": 30.6735, "lon": 76.7884},
    "GAU": {"name": "Lokpriya Gopinath Bordoloi", "city": "Guwahati", "lat": 26.1061, "lon": 91.5859},
    "IXB": {"name": "Bagdogra Airport", "city": "Bagdogra", "lat": 26.6815, "lon": 88.3286},
}

# Airline info
INDIAN_AIRLINES = {
    "IGO": {"name": "IndiGo", "iata": "6E", "icao": "IGO"},
    "AIC": {"name": "Air India", "iata": "AI", "icao": "AIC"},
    "VTI": {"name": "Vistara", "iata": "UK", "icao": "VTI"},
    "SEJ": {"name": "SpiceJet", "iata": "SG", "icao": "SEJ"},
    "AKJ": {"name": "Akasa Air", "iata": "QP", "icao": "AKJ"},
    "AXB": {"name": "Air India Express", "iata": "IX", "icao": "AXB"},
}


def enrich_flight_route(flight: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich flight with route information using hybrid approach.
    
    Priority:
    1. Exact route database
    2. Geographic detection (if near airports)
    3. Return None (let filter remove it)
    
    Args:
        flight: Flight data dict with callsign, lat, lon, altitude, etc.
        
    Returns:
        Enriched flight dict with route info or original dict
    """
    callsign = flight.get('callsign', '').strip().upper()
    latitude = flight.get('latitude')
    longitude = flight.get('longitude')
    altitude = flight.get('baro_altitude') or flight.get('geo_altitude')
    on_ground = flight.get('on_ground', False)
    
    # Extract airline prefix
    airline_prefix = None
    for prefix in INDIAN_AIRLINES.keys():
        if callsign.startswith(prefix):
            airline_prefix = prefix
            break
    
    enriched = flight.copy()
    enriched['airline_prefix'] = airline_prefix
    enriched['route_detection_method'] = None
    
    # Method 1: Exact database lookup
    origin, destination = get_route_from_database(callsign)
    if origin and destination:
        enriched['route_detection_method'] = 'exact_database'
        return add_route_info(enriched, origin, destination, airline_prefix)
    
    # Method 2: Geographic detection
    if latitude and longitude:
        origin, destination = detect_route_geographically(
            latitude, longitude, altitude, on_ground
        )
        if origin and destination:
            enriched['route_detection_method'] = 'geographic'
            return add_route_info(enriched, origin, destination, airline_prefix)
    
    # No route found - return without route info
    # (filter will remove this)
    return enriched


def detect_route_geographically(
    lat: float, 
    lon: float, 
    altitude: Optional[float], 
    on_ground: bool
) -> Tuple[Optional[str], Optional[str]]:
    """
    Detect route based on geographic position.
    
    Returns:
        (origin_code, destination_code) or (None, None)
    """
    
    # If on ground or very low altitude, check nearest airport
    if on_ground or (altitude and altitude < 500):
        nearest = find_nearest_airport(lat, lon, INDIAN_AIRPORTS, max_distance_km=15)
        if nearest:
            # On ground at an airport, but can't determine if origin or destination
            # Would need historical tracking
            return (None, None)
    
    # If in flight, find the two nearest airports
    # (assumption: flight is between these two)
    if altitude and altitude > 1000:
        distances = []
        for code, info in INDIAN_AIRPORTS.items():
            dist = haversine_distance(lat, lon, info['lat'], info['lon'])
            distances.append((code, dist))
        
        # Sort by distance
        distances.sort(key=lambda x: x[1])
        
        # If both airports are within reasonable range (< 1000km)
        if len(distances) >= 2 and distances[0][1] < 500 and distances[1][1] < 500:
            # We have two candidate airports but can't determine direction
            # This is a limitation without tracking history
            # For now, return them (could be wrong order)
            return (distances[0][0], distances[1][0])
    
    return (None, None)


def add_route_info(
    flight: Dict[str, Any], 
    origin_code: str, 
    destination_code: str,
    airline_prefix: Optional[str]
) -> Dict[str, Any]:
    """
    Add route and airline information to flight dict.
    """
    # Add origin info
    origin_info = INDIAN_AIRPORTS.get(origin_code, {})
    flight['origin_code'] = origin_code
    flight['origin_city'] = origin_info.get('city', '')
    flight['origin_airport'] = origin_info.get('name', '')
    flight['origin_lat'] = origin_info.get('lat')
    flight['origin_lon'] = origin_info.get('lon')
    
    # Add destination info
    dest_info = INDIAN_AIRPORTS.get(destination_code, {})
    flight['destination_code'] = destination_code
    flight['destination_city'] = dest_info.get('city', '')
    flight['destination_airport'] = dest_info.get('name', '')
    flight['destination_lat'] = dest_info.get('lat')
    flight['destination_lon'] = dest_info.get('lon')
    
    # Add airline info
    if airline_prefix:
        airline_info = INDIAN_AIRLINES.get(airline_prefix, {})
        flight['airline_name'] = airline_info.get('name', '')
        flight['airline_iata'] = airline_info.get('iata', '')
        flight['airline_icao'] = airline_info.get('icao', '')
    else:
        flight['airline_name'] = None
        flight['airline_iata'] = None
        flight['airline_icao'] = None
    
    return flight


# For testing
if __name__ == "__main__":
    # Test with sample flights
    test_flights = [
        {
            "callsign": "IGO981",
            "latitude": 28.5,
            "longitude": 77.1,
            "baro_altitude": 10000,
            "on_ground": False,
        },
        {
            "callsign": "AIC118",
            "latitude": 19.0,
            "longitude": 72.9,
            "baro_altitude": 100,
            "on_ground": True,
        },
        {
            "callsign": "IGO9999",  # Not in database
            "latitude": 20.0,
            "longitude": 75.0,
            "baro_altitude": 8000,
            "on_ground": False,
        }
    ]
    
    print("Testing hybrid route enrichment:")
    print("=" * 60)
    for flight in test_flights:
        enriched = enrich_flight_route(flight)
        print(f"\nCallsign: {flight['callsign']}")
        print(f"  Method: {enriched.get('route_detection_method', 'none')}")
        print(f"  Route: {enriched.get('origin_code', 'N/A')} → {enriched.get('destination_code', 'N/A')}")
        print(f"  Airline: {enriched.get('airline_name', 'N/A')}")
