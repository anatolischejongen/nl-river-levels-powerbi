import requests
import csv
import io

# This script fetches the catalogue of water level stations 
# from the Rijkswaterstaat WFS service,
# filters for water level stations (GROOTHEIDCODE == "WATHTE"), 
# and searches for stations matching a list of target names.

TARGET_NAMES = [
    "lobith",
    "nijmegen",
    "tiel",
    "olst",
    "deventer",
    "kampen",
    "zutphen",
    "borgharen",
    "maastricht",
    "venlo",
    "grave",
    "arnhem",
    "roermond",
    "groningen",
    "leeuwarden",
    "zwolle",
    "enschede",
    "rotterdam",
    "utrecht",
]

WFS_URL = (
    "https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows"
    "?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
    "&TYPENAME=locatiesmetlaatstewaarneming&outputFormat=csv"
)

# Fetch the station catalogue from the WFS endpoint

print("Fetching catalogue from WFS...")
response = requests.get(WFS_URL, timeout=120)
print(f"Status: {response.status_code}")

if response.status_code != 200:
    print(f"WFS request failed. Body: {response.text[:500]}")
    exit()

# Parse the CSV response
reader = csv.DictReader(io.StringIO(response.text))

# Filter for water level stations and extract relevant info

water_level_stations = []
for row in reader:
    if row.get("GROOTHEIDCODE") == "WATHTE":
        water_level_stations.append({
            "code": row.get("CODE"),
            "name": row.get("NAAM"),
            "compartiment": row.get("COMPARTIMENTCODE"),
            "hoedanigheid": row.get("HOEDANIGHEIDCODE"),
            "last_value": row.get("WAARDE_LAATSTE_METING"),
            "last_time": row.get("TIJDSTIP_LAATSTE_METING"),
        })

print(f"Total WATHTE stations: {len(water_level_stations)}")

print("\n" + "=" * 70)
print("STATION SEARCH RESULTS")
print("=" * 70)

# Search for target names in station code or name
results = {}

for target in TARGET_NAMES:
    matches = []
    for station in water_level_stations:
        name = (station["name"] or "").lower()
        code = (station["code"] or "").lower()
        if target in name or target in code:
            matches.append(station)
    results[target] = matches

# Define a helper to check if a station 
# has recent measurements (2025 or 2026) for better sorting
def is_recent(station):
    last = station.get("last_time", "") or ""
    return last.startswith("2025") or last.startswith("2026")


# make a simple report of matches, sorted by recent activity and then code
for target, matches in results.items():
    print(f"\n--- {target.upper()} ---")
    
    if not matches:
        print("  ❌ No matches found")
        continue
    
    matches.sort(key=lambda s: (not is_recent(s), s.get("code", "")))
    
    print(f"  Found {len(matches)} match(es):")
    for s in matches:
        active_marker = "🟢" if is_recent(s) else "⚪"
        print(f"  {active_marker} {s['code']:35s} | {s['name']:40s} | last: {s['last_time']}")