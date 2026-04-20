"""
discover_grootheids.py — Find which of our 12 stations publish DEBIET/STROOMV.

Strategy: call OphalenWaarnemingen directly with each grootheid for a short
7-day window. If the response contains actual measurements, the station
publishes that grootheid. Empty WaarnemingenLijst or HTTP error → not available.

This is more reliable than metadata endpoints, which have stricter request
format requirements not documented publicly for the new Wadar service.

Usage:
    python scripts/discover_grootheids.py
"""

import time
import yaml
import requests
from pathlib import Path
from datetime import datetime, timedelta, UTC

# ── Paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"

API_URL = (
    "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
    "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"
)
TIME_FORMAT   = "%Y-%m-%dT%H:%M:%S.000+00:00"
REQUEST_DELAY = 0.8
DEFAULT_TIMEOUT = 30

# "Q" is the Aquo standard code for discharge (debiet). "DEBIET" is not
# a valid grootheid code in this API — confirmed via WFS catalogue inspection.
CHECK_GROOTHEDEN = ["Q"]

# Short recent window — enough to confirm availability without pulling much data
END_TIME   = datetime.now(UTC)
START_TIME = END_TIME - timedelta(days=7)


def load_stations(yaml_path):
    with open(yaml_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])


def has_data(station_code, grootheid):
    """
    Return True if the station publishes at least one measurement for this
    grootheid in the last 7 days.

    Calls OphalenWaarnemingen and checks whether WaarnemingenLijst is
    non-empty and contains at least one MetingenLijst entry with data.
    Any HTTP error (including 400) is treated as 'not available'.
    """
    body = {
        "Locatie": {"Code": station_code},
        "AquoPlusWaarnemingMetadata": {
            "AquoMetadata": {
                "Compartiment": {"Code": "OW"},
                "Grootheid":    {"Code": grootheid},
            }
        },
        "Periode": {
            "Begindatumtijd": START_TIME.strftime(TIME_FORMAT),
            "Einddatumtijd":  END_TIME.strftime(TIME_FORMAT),
        },
    }
    try:
        r = requests.post(API_URL, json=body, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        for entry in data.get("WaarnemingenLijst", []):
            if entry.get("MetingenLijst"):
                return True
        return False
    except Exception:
        return False


def main():
    stations = load_stations(STATIONS_FILE)
    print(f"Probing {len(stations)} stations × {len(CHECK_GROOTHEDEN)} grootheids "
          f"via OphalenWaarnemingen (last 7 days)...\n")

    results = {g: [] for g in CHECK_GROOTHEDEN}

    header = f"{'Station code':<45}" + "".join(f"{g:^10}" for g in CHECK_GROOTHEDEN)
    print(header)
    print("-" * len(header))

    for s in stations:
        code = s["code"]
        row  = f"  {code:<43}"
        for grootheid in CHECK_GROOTHEDEN:
            found = has_data(code, grootheid)
            row  += f"{'✅':^10}" if found else f"{'—':^10}"
            if found:
                results[grootheid].append(code)
            time.sleep(REQUEST_DELAY)
        print(row)

    print("\n" + "=" * 65)
    for grootheid in CHECK_GROOTHEDEN:
        print(f"{grootheid}_STATIONS  = {results[grootheid]}")
    print("\nNote: Q = debiet (discharge, m³/s). Hoedanigheid = NVT.")
    print("These codes are already filled in fetch_hydraulics.py.")


if __name__ == "__main__":
    main()
