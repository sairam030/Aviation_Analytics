# How to Add Exact Flight Routes

This document explains different methods to populate accurate flight route data for your aviation pipeline.

## Problem

The current system uses **pattern-based route guessing** (flight number ranges), which is inaccurate. We need **actual flight schedules and routes**.

---

## Solution Options

### 1. **Manual Database (Quick Start)** ⭐ RECOMMENDED FOR TESTING

Update `/src/speed/route_database.py` with known routes.

**How to find real routes:**
1. Visit airline websites (IndiGo, Air India, etc.)
2. Check flight schedules
3. Add to `EXACT_ROUTES` dictionary

Example:
```python
EXACT_ROUTES = {
    "IGO981": ("DEL", "HYD"),  # IndiGo flight 981: Delhi → Hyderabad
    "AIC118": ("DEL", "BOM"),  # Air India 118: Delhi → Mumbai
    # Add more...
}
```

**Pros:** Free, accurate, immediate
**Cons:** Manual work, limited coverage

---

### 2. **CSV Import (Scalable)** ⭐ RECOMMENDED FOR PRODUCTION

Create a CSV file with routes and load it.

**File format** (`routes.csv`):
```csv
callsign,origin,destination,airline
IGO981,DEL,HYD,IndiGo
IGO100,DEL,BOM,IndiGo
AIC118,DEL,BOM,Air India
SEJ334,DEL,HYD,SpiceJet
```

**How to use:**
```python
from src.speed.route_database import load_routes_from_csv

routes = load_routes_from_csv('/data/routes.csv')
```

**Where to get CSV data:**
- Export from airline schedule APIs
- Scrape from FlightRadar24
- Download from OurAirports.com
- Build from historical tracking data

---

### 3. **Flight Data APIs** ⭐ MOST ACCURATE

Use commercial APIs for real-time flight data.

#### **Option A: AviationStack**
- **Cost:** $10-50/month
- **Coverage:** Worldwide flights, schedules
- **API:** https://aviationstack.com

```python
import requests

def get_route_from_aviationstack(callsign, api_key):
    url = f"http://api.aviationstack.com/v1/flights"
    params = {
        'access_key': api_key,
        'flight_iata': callsign[:2] + callsign[3:]  # Convert IGO981 → 6E981
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    if data.get('data'):
        flight = data['data'][0]
        origin = flight['departure']['iata']
        dest = flight['arrival']['iata']
        return (origin, dest)
    return (None, None)
```

#### **Option B: FlightAware (Most Comprehensive)**
- **Cost:** $90+/month
- **Coverage:** Best available
- **API:** https://flightaware.com/commercial/aeroapi/

#### **Option C: RapidAPI Flight APIs**
- **Cost:** Various (some free tiers)
- **API:** https://rapidapi.com/hub

---

### 4. **OpenSky Network Extended API**

OpenSky provides limited flight plan data for registered users.

**How to use:**
```python
import requests

def get_route_from_opensky(icao24, username, password):
    url = f"https://opensky-network.org/api/flights/aircraft"
    params = {'icao24': icao24, 'begin': begin_time, 'end': end_time}
    response = requests.get(url, params=params, auth=(username, password))
    
    # Parse flight plan if available
    data = response.json()
    # Extract origin/destination from flight plan
```

**Pros:** Free for registered users
**Cons:** Limited data, requires authentication

---

### 5. **Historical Data Analysis** ⭐ BUILD YOUR OWN

Analyze your existing tracked flights to build route database.

**Steps:**
1. Collect 1-2 weeks of flight tracking data
2. For each callsign, track first/last airports visited
3. Find most common origin-destination pairs
4. Build route database

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Read historical parquet files
df = spark.read.parquet("/data/aviation_data/batch/bronze/")

# Find takeoff/landing airports (low altitude positions)
low_altitude = df.filter(col("baro_altitude") < 1000)

# Group by callsign and find first/last airports
routes = (low_altitude
    .withColumn("nearest_airport", udf_find_nearest_airport(col("latitude"), col("longitude")))
    .groupBy("callsign")
    .agg(
        first("nearest_airport").alias("origin"),
        last("nearest_airport").alias("destination")
    ))

# Save to CSV
routes.write.csv("/data/routes_database.csv")
```

---

### 6. **Web Scraping (Free but fragile)**

Scrape flight information from public websites.

**Sources:**
- FlightRadar24.com
- FlightAware.com
- Airline websites

**Example with BeautifulSoup:**
```python
import requests
from bs4 import BeautifulSoup

def scrape_flightaware(flight_number):
    url = f"https://flightaware.com/live/flight/{flight_number}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Parse origin/destination
    # (HTML structure changes, requires maintenance)
    origin = soup.find(class_='origin').text
    dest = soup.find(class_='destination').text
    
    return (origin, dest)
```

**Pros:** Free
**Cons:** Against ToS, breaks easily, slow

---

### 7. **Geographic Tracking (Automated)**

Track each flight over time and infer routes from position history.

See `/src/speed/geographic_router.py` for implementation.

**How it works:**
1. When flight first appears (on ground), mark as origin airport
2. Track flight trajectory
3. When flight lands (altitude drops), mark as destination

**Pros:** Automatic, no external data needed
**Cons:** Requires continuous tracking, can't get route immediately

---

## **Recommended Implementation Plan**

### **Phase 1: Quick Start (1 hour)**
1. Manually add 50-100 most common routes to `route_database.py`
2. Use airline websites to find schedules
3. Focus on major airlines (IndiGo, Air India)

### **Phase 2: CSV Database (1 day)**
1. Create `routes.csv` file
2. Populate from multiple sources
3. Update enrichment to load from CSV

### **Phase 3: API Integration (1 week)**
1. Sign up for AviationStack or similar
2. Implement API lookup with caching
3. Fall back to CSV database if API fails

### **Phase 4: Historical Analysis (2 weeks)**
1. Build route inference from your tracked data
2. Continuously update route database
3. Use ML to improve accuracy

---

## **Integration with Current System**

To use the hybrid enrichment:

1. **Option A:** Replace Spark enrichment with Python UDF:
```python
from src.speed.hybrid_route_enrichment import enrich_flight_route

# In Spark streaming
def enrich_udf(flight_json):
    flight = json.loads(flight_json)
    enriched = enrich_flight_route(flight)
    return json.dumps(enriched)

enriched = df.withColumn("enriched", udf(enrich_udf)(col("value")))
```

2. **Option B:** Keep Spark SQL join but update route mapping source:
- Load from CSV instead of embedded dictionary
- Query API for unknown routes
- Cache results

---

## **Quick Win: Use What You Have**

Your logs show real callsigns. Let's add them manually right now!

From your data, I see:
- IGO981, IGO6169, IGO6559, IGO2441, IGO7207, IGO7266
- AIC2952, AIC2927, AIC118, AIC173
- SEJ334
- AXB1513

Visit https://www.flightradar24.com and search each one to find real routes, then add to database.

---

## **Files Created**

1. `/src/speed/route_database.py` - Static route database
2. `/src/speed/geographic_router.py` - Geographic detection
3. `/src/speed/hybrid_route_enrichment.py` - Combined approach
4. This guide

**Next steps:** Choose your preferred method and implement!
