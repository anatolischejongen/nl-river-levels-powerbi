"""
fetch_hydraulics.py — Fetch 3+ years of DEBIET and STROOMV data.

Mirrors fetch_historical.py in structure and style. The key difference:
instead of fetching WATHTE for all 12 stations, this script fetches DEBIET
and STROOMV only for stations that are known to publish those measurements
(verified via find_all_stations.py → HYDRAULICS AVAILABILITY CHECK section).

Before running this script:
1. Run `python scripts/find_all_stations.py`
2. Read the HYDRAULICS AVAILABILITY CHECK output at the bottom.
3. Update DEBIET_STATIONS and STROOMV_STATIONS below with the actual results.

Output files: data/raw/{station_code}_debiet_3y.csv
              data/raw/{station_code}_stroomv_3y.csv

These files are picked up by load_measurements.py automatically (it globs
all *_3y.csv files and strips the grootheid suffix from the filename).

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

# ── Station lists ──────────────────────────────────────────────────────
# UPDATE THESE after running find_all_stations.py.
# Only list stations that actually publish the measurement — fetching a
# station that has no DEBIET data wastes an API call and returns empty.
#
# Example (fill in real codes after discovery):
#   DEBIET_STATIONS  = ["lobith.bovenrijn.tolkamer", "grave.beneden", ...]
#   STROOMV_STATIONS = ["lobith.bovenrijn.tolkamer", ...]
#
# Leave as empty list to skip that grootheid entirely.

DEBIET_STATIONS  = []   # <-- vul aan na find_all_stations.py uitvoer
STROOMV_STATIONS = []   # <-- vul aan na find_all_stations.py uitvoer


# ── Helpers ───────────────────────────────────────────────────────────

def load_stations(yaml_path):
    """Load station list from stations.yaml — identical to fetch_historical.py."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])


def fetch_one_station_historical(station, grootheid):
    """
    Fetch historical data for one station and one grootheid.

    Mirrors fetch_historical.py:fetch_one_station_historical() exactly,
    adding the grootheid parameter to fetch_station_data() and
    parse_response_to_rows().

    Failures per year are logged but do not abort the run.
    """
    code = station["code"]
    all_rows = []

    for year, (start_time, end_time) in YEAR_RANGES.items():
        try:
            response_json = fetch_station_data(code, start_time, end_time, grootheid=grootheid)
            rows = parse_response_to_rows(response_json, code, grootheid=grootheid)
            all_rows.extend(rows)
            tqdm.write(f"    {year}: {len(rows):,} rows")
        except Exception as e:
            tqdm.write(f"    {year}: ✗ {type(e).__name__}: {e}")

        time.sleep(REQUEST_DELAY)

    return all_rows


def fetch_grootheid(grootheid, target_codes, all_stations):
    """
    Fetch historical data for all stations in target_codes for a given grootheid.

    Parameters
    ----------
    grootheid : str
        Aquo grootheid code, e.g. "DEBIET" or "STROOMV".
    target_codes : list of str
        Station codes to fetch (from DEBIET_STATIONS / STROOMV_STATIONS).
    all_stations : list of dict
        Full station list from stations.yaml (used to get name/river metadata).
    """
    if not target_codes:
        print(f"\n⚠  No stations configured for {grootheid} — skipping.")
        print(f"   Update {grootheid}_STATIONS at the top of this script.")
        return

    # Build lookup: code → station metadata (for tqdm labels)
    station_map = {s["code"]: s for s in all_stations}

    print(f"\n{'=' * 60}")
    print(f"Fetching {grootheid} — {len(target_codes)} station(s)")
    print(f"{'=' * 60}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for code in tqdm(target_codes, desc=grootheid, unit="station"):
        station = station_map.get(code)
        if station is None:
            tqdm.write(f"  ⚠ {code} not found in stations.yaml — skipping")
            continue

        tqdm.write(f"\n→ {station['name']} ({station['river']}) — {grootheid}")
        rows = fetch_one_station_historical(station, grootheid)

        if rows:
            df = pd.DataFrame(rows)
            suffix = grootheid.lower()
            out_path = OUTPUT_DIR / f"{code}_{suffix}_3y.csv"
            df.to_csv(out_path, index=False, encoding="utf-8")
            tqdm.write(f"  ✓ {len(rows):,} rows → {out_path.name}")
        else:
            tqdm.write(f"  ✗ No data returned for {code} ({grootheid})")


def main():
    print("=" * 60)
    print("Rijkswaterstaat — Hydraulics fetch (DEBIET + STROOMV, 3 years)")
    print("=" * 60)

    all_stations = load_stations(STATIONS_FILE)
    print(f"{len(all_stations)} stations in stations.yaml")

    fetch_grootheid("DEBIET",  DEBIET_STATIONS,  all_stations)
    fetch_grootheid("STROOMV", STROOMV_STATIONS, all_stations)

    print("\n" + "=" * 60)
    print("Done. Load into Postgres with:")
    print("  python scripts/load_measurements.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
