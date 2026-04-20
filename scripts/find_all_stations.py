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


# ── Section 2: HYDRAULICS NAME SEARCH — DEBIET and STROOMV by location ─
#
# DEBIET / STROOMV stations often have DIFFERENT codes from WATHTE stations
# at the same physical location (e.g. lobith.bovenrijn.tolkamer for WATHTE
# but lobith.debiet or just lobith for DEBIET). Exact-code matching would
# always return empty. Instead we search by the same TARGET_NAMES across
# each grootheid's station list — same approach as Section 1.

print("\n\n" + "=" * 70)
print("HYDRAULICS NAME SEARCH  (DEBIET and STROOMV)")
print("Same TARGET_NAMES searched across each grootheid's catalogue")
print("=" * 70)

for grootheid in ["DEBIET", "STROOMV"]:
    stations = stations_by_grootheid[grootheid]
    print(f"\n{'─' * 70}")
    print(f"  {grootheid}  ({len(stations)} stations in catalogue)")
    print(f"{'─' * 70}")

    any_found = False
    for target in TARGET_NAMES:
        matches = []
        for station in stations:
            name = (station["name"] or "").lower()
            code = (station["code"] or "").lower()
            if target in name or target in code:
                matches.append(station)

        if not matches:
            continue

        any_found = True
        matches.sort(key=lambda s: (not is_recent(s), s.get("code", "")))
        print(f"\n  --- {target.upper()} ---")
        for s in matches:
            active_marker = "🟢" if is_recent(s) else "⚪"
            print(f"  {active_marker} {s['code']:40s} | {s['name']:35s} | last: {s['last_time']}")

    if not any_found:
        print(f"\n  ❌ No matches for any TARGET_NAME under {grootheid}")

print("\n\nNext step:")
print("  Copy the 🟢 codes above into fetch_hydraulics.py:")
print("    DEBIET_STATIONS  = [...]")
print("    STROOMV_STATIONS = [...]")
