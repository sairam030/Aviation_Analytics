"""
Route Mapping - Indian Airlines Route Database
Maps callsign prefixes to airline info and common routes
"""

# Indian airline callsign prefixes
INDIAN_AIRLINES = {
    "IGO": {"name": "IndiGo", "iata": "6E", "icao": "IGO"},
    "AIC": {"name": "Air India", "iata": "AI", "icao": "AIC"},
    "VTI": {"name": "Vistara", "iata": "UK", "icao": "VTI"},
    "SEJ": {"name": "SpiceJet", "iata": "SG", "icao": "SEJ"},
    "AKJ": {"name": "Akasa Air", "iata": "QP", "icao": "AKJ"},
    "JAI": {"name": "Air India Express", "iata": "IX", "icao": "JAI"},
    "ALK": {"name": "Alliance Air", "iata": "9I", "icao": "ALK"},
    "SAG": {"name": "Star Air", "iata": "S5", "icao": "SAG"},
    "FLY": {"name": "Fly91", "iata": "91", "icao": "FLY"},
}

# Major Indian airports
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
    "LKO": {"name": "Chaudhary Charan Singh", "city": "Lucknow", "lat": 26.7606, "lon": 80.8893},
    "GAU": {"name": "Lokpriya Gopinath Bordoloi", "city": "Guwahati", "lat": 26.1061, "lon": 91.5859},
    "TRV": {"name": "Trivandrum International", "city": "Thiruvananthapuram", "lat": 8.4821, "lon": 76.9200},
    "IXC": {"name": "Chandigarh International", "city": "Chandigarh", "lat": 30.6735, "lon": 76.7885},
    "PAT": {"name": "Jay Prakash Narayan", "city": "Patna", "lat": 25.5913, "lon": 85.0880},
    "VNS": {"name": "Lal Bahadur Shastri", "city": "Varanasi", "lat": 25.4524, "lon": 82.8593},
    "IXR": {"name": "Birsa Munda Airport", "city": "Ranchi", "lat": 23.3143, "lon": 85.3217},
    "BBI": {"name": "Biju Patnaik International", "city": "Bhubaneswar", "lat": 20.2444, "lon": 85.8178},
    "SXR": {"name": "Sheikh ul-Alam International", "city": "Srinagar", "lat": 33.9871, "lon": 74.7742},
    "IXB": {"name": "Bagdogra Airport", "city": "Bagdogra", "lat": 26.6812, "lon": 88.3286},
    "CJB": {"name": "Coimbatore International", "city": "Coimbatore", "lat": 11.0300, "lon": 77.0434},
    "IXM": {"name": "Madurai Airport", "city": "Madurai", "lat": 9.8345, "lon": 78.0934},
    "NAG": {"name": "Dr. Babasaheb Ambedkar", "city": "Nagpur", "lat": 21.0922, "lon": 79.0472},
    "IDR": {"name": "Devi Ahilya Bai Holkar", "city": "Indore", "lat": 22.7218, "lon": 75.8011},
    "VTZ": {"name": "Visakhapatnam Airport", "city": "Visakhapatnam", "lat": 17.7212, "lon": 83.2245},
    "RPR": {"name": "Swami Vivekananda Airport", "city": "Raipur", "lat": 21.1804, "lon": 81.7388},
    "BDQ": {"name": "Vadodara Airport", "city": "Vadodara", "lat": 22.3362, "lon": 73.2266},
    "RAJ": {"name": "Rajkot Airport", "city": "Rajkot", "lat": 22.3092, "lon": 70.7795},
    "UDR": {"name": "Maharana Pratap Airport", "city": "Udaipur", "lat": 24.6177, "lon": 73.8961},
}

# Common route patterns (flight number ranges to routes)
# Format: "PREFIX": [(min_num, max_num, origin, destination), ...]
ROUTE_PATTERNS = {
    "IGO": [
        # IndiGo domestic routes
        (100, 199, "DEL", "BOM"),
        (200, 299, "DEL", "BLR"),
        (300, 399, "DEL", "CCU"),
        (400, 499, "BOM", "BLR"),
        (500, 599, "BOM", "DEL"),
        (600, 699, "BLR", "DEL"),
        (700, 799, "DEL", "MAA"),
        (800, 899, "BOM", "CCU"),
        (900, 999, "DEL", "HYD"),
        (1000, 1099, "BLR", "BOM"),
        (1100, 1199, "HYD", "DEL"),
        (1200, 1299, "MAA", "DEL"),
        (1300, 1399, "CCU", "DEL"),
        (1400, 1499, "DEL", "COK"),
        (1500, 1599, "BOM", "HYD"),
        (2000, 2999, "DEL", "GOI"),  # Goa routes
        (5000, 5999, "BOM", "MAA"),  # South routes
        (6000, 6999, "BLR", "CCU"),  # East routes
    ],
    "AIC": [
        # Air India routes
        (100, 199, "DEL", "BOM"),
        (200, 299, "DEL", "BLR"),
        (300, 399, "DEL", "MAA"),
        (400, 499, "DEL", "CCU"),
        (500, 599, "BOM", "DEL"),
        (600, 699, "BOM", "BLR"),
        (800, 899, "DEL", "HYD"),
        (900, 999, "BLR", "DEL"),
    ],
    "VTI": [
        # Vistara routes
        (800, 899, "DEL", "BOM"),
        (810, 819, "DEL", "BLR"),
        (820, 829, "DEL", "HYD"),
        (830, 839, "DEL", "MAA"),
        (840, 849, "DEL", "CCU"),
        (900, 999, "BOM", "DEL"),
    ],
    "SEJ": [
        # SpiceJet routes
        (100, 199, "DEL", "BOM"),
        (200, 299, "DEL", "GOI"),
        (300, 399, "DEL", "HYD"),
        (400, 499, "BOM", "DEL"),
        (500, 599, "BOM", "BLR"),
    ],
    "AKJ": [
        # Akasa Air routes
        (100, 199, "BOM", "BLR"),
        (200, 299, "DEL", "BOM"),
        (300, 399, "BOM", "DEL"),
        (400, 499, "BLR", "DEL"),
    ],
}


def get_airline_from_callsign(callsign: str) -> dict:
    """Extract airline info from callsign prefix."""
    if not callsign:
        return None
    
    callsign = callsign.strip().upper()
    
    for prefix, info in INDIAN_AIRLINES.items():
        if callsign.startswith(prefix):
            return {
                "airline_prefix": prefix,
                "airline_name": info["name"],
                "airline_iata": info["iata"],
                "airline_icao": info["icao"],
            }
    
    return None


def get_flight_number(callsign: str) -> int:
    """Extract numeric flight number from callsign."""
    if not callsign:
        return None
    
    callsign = callsign.strip()
    # Extract digits from callsign (e.g., "IGO6135" -> 6135)
    digits = ''.join(filter(str.isdigit, callsign))
    return int(digits) if digits else None


def get_route_from_callsign(callsign: str) -> dict:
    """Lookup route based on callsign and flight number pattern."""
    if not callsign:
        return None
    
    callsign = callsign.strip().upper()
    flight_num = get_flight_number(callsign)
    
    if not flight_num:
        return None
    
    for prefix, routes in ROUTE_PATTERNS.items():
        if callsign.startswith(prefix):
            for min_num, max_num, origin, dest in routes:
                if min_num <= flight_num <= max_num:
                    origin_info = INDIAN_AIRPORTS.get(origin, {})
                    dest_info = INDIAN_AIRPORTS.get(dest, {})
                    return {
                        "origin_code": origin,
                        "origin_city": origin_info.get("city", origin),
                        "origin_airport": origin_info.get("name", ""),
                        "origin_lat": origin_info.get("lat"),
                        "origin_lon": origin_info.get("lon"),
                        "destination_code": dest,
                        "destination_city": dest_info.get("city", dest),
                        "destination_airport": dest_info.get("name", ""),
                        "destination_lat": dest_info.get("lat"),
                        "destination_lon": dest_info.get("lon"),
                    }
    
    return None


def enrich_flight_data(flight: dict) -> dict:
    """Add airline and route information to flight data."""
    callsign = flight.get("callsign", "")
    
    # Get airline info
    airline_info = get_airline_from_callsign(callsign)
    if airline_info:
        flight.update(airline_info)
    
    # Get route info
    route_info = get_route_from_callsign(callsign)
    if route_info:
        flight.update(route_info)
    
    # Extract flight number
    flight["flight_number"] = get_flight_number(callsign)
    
    return flight


# Create DataFrame-compatible schema for Spark
ROUTE_MAPPING_SCHEMA = """
    airline_prefix STRING,
    airline_name STRING,
    airline_iata STRING,
    airline_icao STRING,
    min_flight_num INT,
    max_flight_num INT,
    origin_code STRING,
    origin_city STRING,
    origin_airport STRING,
    origin_lat DOUBLE,
    origin_lon DOUBLE,
    destination_code STRING,
    destination_city STRING,
    destination_airport STRING,
    destination_lat DOUBLE,
    destination_lon DOUBLE
"""


def generate_route_mapping_rows():
    """Generate route mapping data for Spark DataFrame."""
    rows = []
    for prefix, routes in ROUTE_PATTERNS.items():
        airline = INDIAN_AIRLINES.get(prefix, {})
        for min_num, max_num, origin, dest in routes:
            origin_info = INDIAN_AIRPORTS.get(origin, {})
            dest_info = INDIAN_AIRPORTS.get(dest, {})
            rows.append({
                "airline_prefix": prefix,
                "airline_name": airline.get("name", ""),
                "airline_iata": airline.get("iata", ""),
                "airline_icao": airline.get("icao", ""),
                "min_flight_num": min_num,
                "max_flight_num": max_num,
                "origin_code": origin,
                "origin_city": origin_info.get("city", ""),
                "origin_airport": origin_info.get("name", ""),
                "origin_lat": origin_info.get("lat"),
                "origin_lon": origin_info.get("lon"),
                "destination_code": dest,
                "destination_city": dest_info.get("city", ""),
                "destination_airport": dest_info.get("name", ""),
                "destination_lat": dest_info.get("lat"),
                "destination_lon": dest_info.get("lon"),
            })
    return rows
