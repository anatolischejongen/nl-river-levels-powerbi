"""
discover_grootheids.py — Query the Wadar metadata API to find which
grootheids (WATHTE, DEBIET, STROOMV, …) are available at each of our
12 active stations.

The WFS catalogue (locatiesmetlaatstewaarneming) only lists WATHTE stations.
This script uses OphalenCatalogus — the Wadar metadata service — which
returns all available measurement types per station directly from the API.

Usage:
    python scripts/discover_grootheids.py
"""

import time
import yaml
import requests
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"

CATALOGUE_URL = (
    "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
    "/METADATASERVICES/OphalenCatalogus"
)

REQUEST_DELAY = 0.5
DEFAULT_TIMEOUT = 30

TARGET_GROOTHEDEN = {"DEBIET", "STROOMV", "WATHTE"}


def load_stations(yaml_path):
    with open(yaml_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])


def fetch_catalogue(station_code):
    """
    Ask the Wadar metadata API what measurements are available at a station.

    Returns a list of grootheid codes, or an empty list on error.
    """
    body = {"Locatie": {"Code": station_code}}
    try:
        response = requests.post(CATALOGUE_URL, json=body, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        # Response structure: AquoMetadataLijst → each entry has Grootheid.Code
        return [
            entry.get("Grootheid", {}).get("Code", "?")
            for entry in data.get("AquoMetadataLijst", [])
        ]
    except Exception as e:
        print(f"  ⚠ {station_code}: {type(e).__name__}: {e}")
        return []


def main():
    stations = load_stations(STATIONS_FILE)
    print(f"Querying OphalenCatalogus for {len(stations)} stations...\n")

    debiet_stations  = []
    stroomv_stations = []

    print(f"{'Station code':<45} {'Available grootheids'}")
    print("-" * 75)

    for s in stations:
        code = s["code"]
        grootheids = fetch_catalogue(code)
        interesting = [g for g in grootheids if g in TARGET_GROOTHEDEN]
        all_str = ", ".join(sorted(grootheids)) if grootheids else "—"
        print(f"  {code:<43} {all_str}")

        if "DEBIET"  in grootheids:
            debiet_stations.append(code)
        if "STROOMV" in grootheids:
            stroomv_stations.append(code)

        time.sleep(REQUEST_DELAY)

    print("\n" + "=" * 75)
    print(f"DEBIET_STATIONS  = {debiet_stations}")
    print(f"STROOMV_STATIONS = {stroomv_stations}")
    print("\nPaste these lists into fetch_hydraulics.py")


if __name__ == "__main__":
    main()
