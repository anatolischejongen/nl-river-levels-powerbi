"""
fetch_hydraulics.py — Fetch 3+ years of discharge (Q) data.

Mirrors fetch_historical.py in structure and style. Fetches Q (debiet,
m³/s) for the 5 stations confirmed to publish discharge data, discovered
via discover_grootheids.py.

API notes:
- Grootheid code for discharge is "Q" (Aquo standard), not "DEBIET".
- Hoedanigheid for Q measurements is "NVT" (Niet Van Toepassing —
  unit is m³/s, no reference datum applies). Parser must accept ("NVT",).
- STROOMV: none of our 12 WATHTE stations publish stroomsnelheid — omitted.

Output files: data/raw/{station_code}_debiet_3y.csv
  These are picked up by load_measurements.py (globs *_3y.csv,
  strips the _debiet suffix from the filename).

Usage:
    python scripts/fetch_hydraulics.py
"""

import time
import yaml
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from rws_api import fetch_station_data, parse_response_to_rows

# ── Paths (fetch_historical.py ile aynı pattern) ──────────────────────
SCRIPT_DIR    = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"
OUTPUT_DIR    = SCRIPT_DIR.parent / "data" / "raw"

REQUEST_DELAY = 0.8

# Yıl bazlı chunk stratejisi — fetch_historical.py ile identik
# Neden chunks? API başına ~160k satır limiti var. 3 yıl tek istekte sığmaz.
YEAR_RANGES = {
    2023: (datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2023, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2024: (datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2024, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2025: (datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2026: (datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC)),
}

# Confirmed via discover_grootheids.py + WFS catalogue cross-check (2026-04-20).
# Grootheid "Q" (Aquo code for discharge), Hoedanigheid "NVT".
DEBIET_STATIONS = [
    "lobith.bovenrijn.tolkamer",
    "arnhem.nederrijn",
    "venlo",
    "maastricht.borgharen.maas.beneden",
    "olst",
]


# ── Helpers ───────────────────────────────────────────────────────────

def load_stations(yaml_path):
    """Load station list from stations.yaml — identical to fetch_historical.py."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])


def fetch_one_station_historical(station_code):
    """
    Fetch historical Q data for one station across all YEAR_RANGES.

    Mirrors fetch_historical.py:fetch_one_station_historical() exactly.
    Key differences:
    - grootheid="Q" (discharge, not water level)
    - accepted_hoedanigheid=("NVT",) because Q measurements carry no
      reference datum (NVT = Niet Van Toepassing)
    """
    all_rows = []

    for year, (start_time, end_time) in YEAR_RANGES.items():
        try:
            response_json = fetch_station_data(
                station_code, start_time, end_time, grootheid="Q"
            )
            rows = parse_response_to_rows(
                response_json, station_code,
                grootheid="Q",
                accepted_hoedanigheid=("NVT",),
            )
            all_rows.extend(rows)
            tqdm.write(f"    {year}: {len(rows):,} rows")
        except Exception as e:
            tqdm.write(f"    {year}: ✗ {type(e).__name__}: {e}")

        time.sleep(REQUEST_DELAY)

    return all_rows


def main():
    print("=" * 60)
    print("Rijkswaterstaat — Discharge fetch (Q, 3 years)")
    print(f"Stations: {len(DEBIET_STATIONS)}")
    print("=" * 60)

    all_stations = load_stations(STATIONS_FILE)
    station_map = {s["code"]: s for s in all_stations}

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for code in tqdm(DEBIET_STATIONS, desc="Q fetch", unit="station"):
        station = station_map.get(code, {"name": code, "river": "?"})
        tqdm.write(f"\n→ {station['name']} ({station['river']})")

        rows = fetch_one_station_historical(code)

        if rows:
            df = pd.DataFrame(rows)
            out_path = OUTPUT_DIR / f"{code}_debiet_3y.csv"
            df.to_csv(out_path, index=False, encoding="utf-8")
            tqdm.write(f"  ✓ {len(rows):,} rows → {out_path.name}")
        else:
            tqdm.write(f"  ✗ No data returned for {code}")

    print("\n" + "=" * 60)
    print("Done. Load into Postgres with:")
    print("  python scripts/load_measurements.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
