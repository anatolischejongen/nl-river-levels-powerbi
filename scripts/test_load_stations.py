import yaml
from pathlib import Path

# Find the path to the YAML file (relative to the script location)
SCRIPT_DIR = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"

print(f"Loading stations from: {STATIONS_FILE}")
print(f"File exists: {STATIONS_FILE.exists()}")
print()

# Install YAML
with open(STATIONS_FILE, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

stations = config.get("stations", [])

print(f"Total stations loaded: {len(stations)}")
print()

# River-based census
rivers = {}
for s in stations:
    river = s.get("river", "Unknown")
    rivers[river] = rivers.get(river, 0) + 1

print("Stations per river:")
for river, count in rivers.items():
    print(f"  {river:15s} : {count}")
print()

# Show details of the first 3 stations
print("First 3 stations (full detail):")
for s in stations[:3]:
    print(f"  - code:   {s['code']}")
    print(f"    name:   {s['name']}")
    print(f"    river:  {s['river']}")
    print(f"    region: {s['region']}")
    print(f"    notes:  {s['notes']}")
    print()

# Validation: required fields present?
missing = []
for s in stations:
    for required_field in ["code", "name", "river"]:
        if not s.get(required_field):
            missing.append((s.get("code", "???"), required_field))

if missing:
    print("⚠️  WARNING: missing required fields:")
    for code, field in missing:
        print(f"  - {code}: missing '{field}'")
else:
    print("✅ All stations have required fields (code, name, river)")