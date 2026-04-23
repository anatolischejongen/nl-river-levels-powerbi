import requests
import csv
import io

wfs_url = (
    "https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows"
    "?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
    "&TYPENAME=locatiesmetlaatstewaarneming&outputFormat=csv"
)

print("Fetching catalogue...")
r = requests.get(wfs_url, timeout=120)
print("Status:", r.status_code)

reader = csv.DictReader(io.StringIO(r.text))

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

print(f"\nTotal WATHTE stations found: {len(water_level_stations)}")


print("\n=== Stations with 'lobith' in name or code ===")
for s in water_level_stations:
    name = (s["name"] or "").lower()
    code = (s["code"] or "").lower()
    if "lobith" in name or "lobith" in code:
        print(s)


print("\n=== Sample of all WATHTE stations (first 30) ===")
for s in water_level_stations[:30]:
    print(f"  {s['code']:30s} | {s['name']}")