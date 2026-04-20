"""
find_all_stations.py — Discover active stations in the Rijkswaterstaat WFS catalogue.

Two sections:
1. STATION SEARCH: Search TARGET_NAMES across WATHTE stations (original use,
   kept for reference). Useful when adding new stations to stations.yaml.
2. HYDRAULICS CHECK: Cross-reference our 12 active stations from stations.yaml
   against DEBIET and STROOMV availability. Run this before fetch_hydraulics.py
   to know exactly which stations to target.

Usage:
    python scripts/find_all_stations.py
"""

import requests
import csv
import io
import yaml
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"

# Original target names for WATHTE station discovery
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

# Grootheid codes to check in the hydraulics cross-reference section
GROOTHEID_FILTERS = ["WATHTE", "DEBIET", "STROOMV"]

WFS_URL = (
    "https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows"
    "?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
    "&TYPENAME=locatiesmetlaatstewaarneming&outputFormat=csv"
)


# ── Helpers ───────────────────────────────────────────────────────────

def is_recent(station):
    """Return True if the station's last measurement is from 2025 or 2026."""
    last = station.get("last_time", "") or ""
    return last.startswith("2025") or last.startswith("2026")


def load_active_station_codes(yaml_path):
    """
    Load station codes from stations.yaml.

    Returns
    -------
    list of str
        The 'code' field for each entry in the YAML stations list.
    """
    with open(yaml_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return [s["code"] for s in config.get("stations", [])]


# ── Fetch WFS catalogue (all grootheids in one request) ───────────────

print("Fetching catalogue from WFS...")
response = requests.get(WFS_URL, timeout=120)
print(f"Status: {response.status_code}")

if response.status_code != 200:
    print(f"WFS request failed. Body: {response.text[:500]}")
    exit()

# Parse the full CSV — one row per (station, grootheid) combination
reader = csv.DictReader(io.StringIO(response.text))
all_catalogue = list(reader)
print(f"Total catalogue entries: {len(all_catalogue):,}\n")

# Build per-grootheid station lists (keeps same structure as before)
stations_by_grootheid = {}
for grootheid in GROOTHEID_FILTERS:
    stations_by_grootheid[grootheid] = [
        {
            "code":         row.get("CODE"),
            "name":         row.get("NAAM"),
            "compartiment": row.get("COMPARTIMENTCODE"),
            "hoedanigheid": row.get("HOEDANIGHEIDCODE"),
            "last_value":   row.get("WAARDE_LAATSTE_METING"),
            "last_time":    row.get("TIJDSTIP_LAATSTE_METING"),
        }
        for row in all_catalogue
        if row.get("GROOTHEIDCODE") == grootheid
    ]

water_level_stations = stations_by_grootheid["WATHTE"]
print(f"Total WATHTE stations:  {len(water_level_stations)}")
for g in ["DEBIET", "STROOMV"]:
    print(f"Total {g} stations: {len(stations_by_grootheid[g])}")


# ── Section 1: WATHTE station name search (original) ─────────────────

print("\n" + "=" * 70)
print("STATION SEARCH RESULTS  (WATHTE only)")
print("=" * 70)

results = {}
for target in TARGET_NAMES:
    matches = []
    for station in water_level_stations:
        name = (station["name"] or "").lower()
        code = (station["code"] or "").lower()
        if target in name or target in code:
            matches.append(station)
    results[target] = matches

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


# ── Section 2: HYDRAULICS CHECK — our 12 stations vs DEBIET / STROOMV ─

print("\n\n" + "=" * 70)
print("HYDRAULICS AVAILABILITY CHECK")
print("Cross-reference: stations.yaml codes vs DEBIET and STROOMV")
print("=" * 70)

active_codes = load_active_station_codes(STATIONS_FILE)

# Build a lookup: code → set of available grootheids
available = {code: set() for code in active_codes}
for grootheid, stations in stations_by_grootheid.items():
    catalogue_codes = {s["code"] for s in stations}
    for code in active_codes:
        if code in catalogue_codes:
            available[code].add(grootheid)

# Print result table
print(f"\n{'Station code':<45} {'WATHTE':^8} {'DEBIET':^8} {'STROOMV':^8}")
print("-" * 73)
for code in active_codes:
    g = available[code]
    w = "✅" if "WATHTE"  in g else "—"
    d = "✅" if "DEBIET"  in g else "—"
    s = "✅" if "STROOMV" in g else "—"
    print(f"  {code:<43} {w:^8} {d:^8} {s:^8}")

# Summary: which stations to target in fetch_hydraulics.py
debiet_codes  = [c for c in active_codes if "DEBIET"  in available[c]]
stroomv_codes = [c for c in active_codes if "STROOMV" in available[c]]

print(f"\n→ {len(debiet_codes)} station(s) with DEBIET:  {debiet_codes}")
print(f"→ {len(stroomv_codes)} station(s) with STROOMV: {stroomv_codes}")
print("\nUpdate DEBIET_STATIONS / STROOMV_STATIONS in fetch_hydraulics.py"
      " with the lists above before running it.")
