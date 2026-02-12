"""
Route Database - Exact flight routes based on real data
This can be populated from:
1. Historical flight tracking data
2. Airline schedule websites
3. Flight data APIs (AviationStack, FlightAware)
4. Manual data entry for known routes
"""

# Exact route mappings: callsign_pattern -> (origin, destination)
# You can populate this from real data sources
EXACT_ROUTES = {
    # IndiGo (6E/IGO)
    "IGO100": ("DEL", "BOM"),
    "IGO101": ("DEL", "BOM"),
    "IGO102": ("BOM", "DEL"),
    "IGO103": ("BOM", "DEL"),
    "IGO200": ("DEL", "BLR"),
    "IGO201": ("DEL", "BLR"),
    "IGO202": ("BLR", "DEL"),
    "IGO203": ("BLR", "DEL"),
    "IGO300": ("DEL", "CCU"),
    "IGO301": ("DEL", "CCU"),
    "IGO302": ("CCU", "DEL"),
    "IGO400": ("BOM", "BLR"),
    "IGO401": ("BOM", "BLR"),
    "IGO402": ("BLR", "BOM"),
    "IGO500": ("BOM", "MAA"),
    "IGO600": ("BLR", "CCU"),
    "IGO700": ("DEL", "MAA"),
    "IGO800": ("BOM", "CCU"),
    "IGO900": ("DEL", "HYD"),
    "IGO981": ("DEL", "HYD"),
    "IGO1064": ("BLR", "BOM"),
    "IGO2441": ("GOI", "DEL"),
    "IGO5058": ("BOM", "MAA"),
    "IGO5077": ("BOM", "MAA"),
    "IGO6014": ("BLR", "CCU"),
    "IGO6019": ("BLR", "CCU"),
    "IGO6169": ("CCU", "BLR"),
    "IGO6510": ("BLR", "CCU"),
    "IGO6559": ("CCU", "BLR"),
    "IGO7207": ("MAA", "BLR"),
    "IGO7266": ("MAA", "HYD"),
    
    # Air India (AI/AIC)
    "AIC100": ("DEL", "BOM"),
    "AIC101": ("DEL", "BOM"),
    "AIC102": ("BOM", "DEL"),
    "AIC118": ("DEL", "BOM"),
    "AIC173": ("DEL", "BOM"),
    "AIC200": ("DEL", "BLR"),
    "AIC300": ("DEL", "MAA"),
    "AIC400": ("DEL", "CCU"),
    "AIC500": ("BOM", "DEL"),
    "AIC600": ("BOM", "BLR"),
    "AIC800": ("DEL", "HYD"),
    "AIC2927": ("BOM", "GOI"),
    "AIC2952": ("GOI", "BOM"),
    
    # SpiceJet (SG/SEJ)
    "SEJ100": ("DEL", "BOM"),
    "SEJ200": ("DEL", "GOI"),
    "SEJ300": ("DEL", "HYD"),
    "SEJ334": ("DEL", "HYD"),
    "SEJ400": ("BOM", "DEL"),
    "SEJ500": ("BOM", "BLR"),
    
    # Vistara (UK/VTI)
    "VTI800": ("DEL", "BOM"),
    "VTI810": ("DEL", "BLR"),
    "VTI820": ("DEL", "HYD"),
    "VTI830": ("DEL", "MAA"),
    "VTI840": ("DEL", "CCU"),
    "VTI900": ("BOM", "DEL"),
    
    # Akasa Air (QP/AKJ)
    "AKJ100": ("BOM", "BLR"),
    "AKJ200": ("DEL", "BOM"),
    "AKJ300": ("BOM", "DEL"),
    "AKJ400": ("BLR", "DEL"),
    
    # Air India Express (IX/AXB)
    "AXB1513": ("BLR", "COK"),
}


# Global cache for CSV routes
_CSV_ROUTES_CACHE = None

def get_route_from_database(callsign: str, use_csv: bool = True) -> tuple:
    """
    Lookup exact route from database.
    
    Args:
        callsign: Flight callsign (e.g., "IGO100")
        use_csv: Whether to load from CSV (default True)
        
    Returns:
        (origin_code, dest_code) or (None, None) if not found
    """
    global _CSV_ROUTES_CACHE
    
    callsign = callsign.strip().upper()
    
    # Try CSV first (if enabled and available)
    if use_csv:
        if _CSV_ROUTES_CACHE is None:
            _CSV_ROUTES_CACHE = load_routes_from_csv()
        
        if callsign in _CSV_ROUTES_CACHE:
            return _CSV_ROUTES_CACHE[callsign]
    
    # Fall back to embedded database
    if callsign in EXACT_ROUTES:
        return EXACT_ROUTES[callsign]
    
    return (None, None)


def load_routes_from_csv(csv_path: str = '/opt/airflow/data/routes.csv'):
    """
    Load routes from CSV file.
    
    CSV format:
    callsign,origin,destination,airline,airline_code,route_type
    IGO100,DEL,BOM,IndiGo,6E,domestic
    AIC200,DEL,BLR,Air India,AI,domestic
    """
    import csv
    routes = {}
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                callsign = row['callsign'].strip().upper()
                origin = row['origin'].strip().upper()
                destination = row['destination'].strip().upper()
                routes[callsign] = (origin, destination)
        print(f"✓ Loaded {len(routes)} routes from CSV: {csv_path}")
        return routes
    except Exception as e:
        print(f"Error loading routes from CSV: {e}")
        return {}


def load_routes_from_api(api_url: str, api_key: str):
    """
    Load routes from external API.
    
    Example: AviationStack, FlightAware, etc.
    """
    # TODO: Implement API integration
    pass


# Example: How to populate EXACT_ROUTES from historical data
def build_route_database_from_historical_data():
    """
    Analyze historical flight tracking data to build route database.
    
    This should:
    1. Read historical parquet files
    2. For each unique callsign, find most common origin/destination airports
    3. Store in database
    """
    pass
