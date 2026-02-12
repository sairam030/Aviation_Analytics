"""
Geographic Route Detection
Uses aircraft position and velocity to determine likely origin/destination airports
"""
import math
from typing import Optional, Tuple, List


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great circle distance between two points in kilometers.
    """
    R = 6371  # Earth radius in km
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


def find_nearest_airport(lat: float, lon: float, airports: dict, max_distance_km: float = 50) -> Optional[str]:
    """
    Find nearest airport to given coordinates.
    
    Args:
        lat, lon: Aircraft position
        airports: Dict of airport_code -> {lat, lon, ...}
        max_distance_km: Maximum distance to consider (default 50km)
        
    Returns:
        Airport code or None if no airport within range
    """
    nearest = None
    min_dist = float('inf')
    
    for code, info in airports.items():
        airport_lat = info.get('lat')
        airport_lon = info.get('lon')
        
        if airport_lat is None or airport_lon is None:
            continue
            
        dist = haversine_distance(lat, lon, airport_lat, airport_lon)
        
        if dist < min_dist and dist <= max_distance_km:
            min_dist = dist
            nearest = code
    
    return nearest


def determine_route_from_position(
    latitude: float,
    longitude: float,
    altitude: float,
    velocity: float,
    on_ground: bool,
    airports: dict
) -> Tuple[Optional[str], Optional[str], str]:
    """
    Determine likely origin/destination based on position and flight state.
    
    Returns:
        (origin_code, destination_code, confidence)
        confidence: 'high', 'medium', 'low'
    """
    
    # If on ground, assume at origin or destination
    if on_ground or (altitude is not None and altitude < 1000):
        nearest = find_nearest_airport(latitude, longitude, airports, max_distance_km=20)
        if nearest:
            # At airport - could be origin or destination
            # Without history, we can't determine which
            return (nearest, None, 'low')
    
    # If in cruise (high altitude, high speed), check proximity to airports
    if altitude and altitude > 5000 and velocity and velocity > 50:
        # In flight - try to find nearby airports as potential origin/dest
        nearby_airports = []
        for code, info in airports.items():
            dist = haversine_distance(latitude, longitude, info['lat'], info['lon'])
            if dist <= 500:  # Within 500km
                nearby_airports.append((code, dist))
        
        if len(nearby_airports) >= 2:
            # Sort by distance
            nearby_airports.sort(key=lambda x: x[1])
            # Nearest two could be origin/dest (but we don't know order)
            return (nearby_airports[0][0], nearby_airports[1][0], 'low')
    
    return (None, None, 'none')


def track_flight_history(icao24: str, position_history: List[dict], airports: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Use historical positions to determine origin/destination.
    
    Args:
        icao24: Aircraft identifier
        position_history: List of historical positions [{lat, lon, altitude, timestamp}, ...]
        airports: Airport database
        
    Returns:
        (origin_code, destination_code)
    """
    if len(position_history) < 2:
        return (None, None)
    
    # Sort by timestamp
    history = sorted(position_history, key=lambda x: x['timestamp'])
    
    # Check first position (takeoff)
    first_pos = history[0]
    if first_pos.get('altitude', 0) < 1000:
        origin = find_nearest_airport(
            first_pos['lat'], 
            first_pos['lon'], 
            airports, 
            max_distance_km=30
        )
    else:
        origin = None
    
    # Check last position (landing)
    last_pos = history[-1]
    if last_pos.get('altitude', 0) < 1000:
        destination = find_nearest_airport(
            last_pos['lat'], 
            last_pos['lon'], 
            airports, 
            max_distance_km=30
        )
    else:
        destination = None
    
    return (origin, destination)
