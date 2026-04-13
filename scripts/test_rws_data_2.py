import requests
import csv
import io

# Tüm istasyonları + son ölçümleri CSV olarak ver
wfs_url = (
    "https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows"
    "?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
    "&TYPENAME=locatiesmetlaatstewaarneming&outputFormat=csv"
)

print("Fetching station catalogue from WFS...")
r = requests.get(wfs_url, timeout=60)
print("Status:", r.status_code)

# CSV'yi parse et, içinde "lobith" geçenleri filtrele
reader = csv.DictReader(io.StringIO(r.text))
matches = []
for row in reader:
    name = (row.get("NAAM") or "").lower()
    code = (row.get("CODE") or "").lower()
    if "lobith" in name or "lobith" in code:
        matches.append({
            "code": row.get("CODE"),
            "name": row.get("NAAM"),
            "grootheid": row.get("GROOTHEIDCODE"),
            "compartiment": row.get("COMPARTIMENTCODE"),
            "last_value": row.get("WAARDE_LAATSTE_METING"),
            "last_time": row.get("TIJDSTIP_LAATSTE_METING"),
        })

print(f"\nFound {len(matches)} Lobith-related rows:")
for m in matches:
    print(m)