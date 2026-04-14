"""
fetch_all_stations.py — Fetch water level data for all 13 stations.

Loads the station list from data/reference/stations.yaml, then fetches
the last 7 days of measurements for each station via rws_api. Failures
on individual stations are logged but do not stop the run.

Usage:
    python scripts/fetch_all_stations.py
"""

import time
import yaml
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from rws_api import fetch_station_data, parse_response_to_rows, get_default_time_range


# Path to stations config — resolved relative to this script's location
SCRIPT_DIR = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"

# Politeness delay between API requests (seconds)
REQUEST_DELAY = 0.8

# Output directory for raw CSV files
OUTPUT_DIR = SCRIPT_DIR.parent / "data" / "raw"

def load_stations(yaml_path):
    """
    Load the list of stations from the YAML config file.

    Parameters
    ----------
    yaml_path : Path or str
        Path to stations.yaml

    Returns
    -------
    list of dict
        Each dict has keys: code, name, river, region, notes
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config.get("stations", [])


def fetch_one_station(station, start_time, end_time):
    """
    Fetch and parse data for a single station, with error handling.

    On any API or network error, this function logs the error and
    returns an empty list — it never raises. This lets the caller
    continue processing other stations even when one fails.

    Parameters
    ----------
    station : dict
        Station dict from stations.yaml (must contain at least 'code')
    start_time : datetime
        Start of the requested period (timezone-aware UTC)
    end_time : datetime
        End of the requested period (timezone-aware UTC)

    Returns
    -------
    list of dict
        Parsed rows for this station, or empty list on failure
    """
    code = station["code"]
    
    try:
        response_json = fetch_station_data(code, start_time, end_time)
        rows = parse_response_to_rows(response_json, code)
        return rows
    except Exception as e:
        tqdm.write(f"  ⚠️  Failed to fetch {code}: {type(e).__name__}: {e}")
        return []

# Helper to save rows to CSV with a timestamped filename
def save_rows_to_csv(rows, output_dir, suffix="7days"):
    """
    Save a list of row dicts to a timestamped CSV file.

    Creates the output directory if it doesn't exist. The filename
    includes today's date so that repeated runs on different days
    don't overwrite each other.

    Parameters
    ----------
    rows : list of dict
        Parsed rows to save (output of parse_response_to_rows)
    output_dir : Path
        Directory where the CSV should be written. Will be created
        if it doesn't exist.
    suffix : str, optional
        Short descriptor appended to the filename (default: "7days")

    Returns
    -------
    Path
        The full path of the written CSV file
    """
    # Make sure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Build filename: water_levels_7days_2026-04-14.csv
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"water_levels_{suffix}_{today}.csv"
    output_path = output_dir / filename
    
    # Convert to DataFrame and write
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8")
    
    return output_path


def main():
    print("=" * 60)
    print("Rijkswaterstaat — Multi-station fetch (last 7 days)")
    print("=" * 60)
    print()
    
    # 1. Load station config
    print(f"Loading stations from: {STATIONS_FILE}")
    stations = load_stations(STATIONS_FILE)
    print(f"  → {len(stations)} stations loaded")
    print()
    
    # 2. Determine time range
    start_time, end_time = get_default_time_range(days=7)
    print(f"Time range: {start_time.isoformat()} → {end_time.isoformat()}")
    print()
    
    # 3. Fetch loop
    all_rows = []
    success_count = 0
    fail_count = 0
    
    for station in tqdm(stations, desc="Fetching stations", unit="station"):
        rows = fetch_one_station(station, start_time, end_time)
        
        if rows:
            all_rows.extend(rows)
            success_count += 1
        else:
            fail_count += 1
        
        time.sleep(REQUEST_DELAY)
    
    # 4. Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Stations attempted: {len(stations)}")
    print(f"  ✅ Successful:    {success_count}")
    print(f"  ❌ Failed:        {fail_count}")
    print(f"Total rows fetched: {len(all_rows):,}")
    print()
    
    # 5. Per-station breakdown
    if all_rows:
        from collections import Counter
        per_station = Counter(r["station_code"] for r in all_rows)
        
        print("Rows per station:")
        for station in stations:
            code = station["code"]
            count = per_station.get(code, 0)
            marker = "✅" if count > 0 else "❌"
            print(f"  {marker} {code:40s} {count:>6,} rows  ({station['name']})")
    
    # 6. Save to CSV
    if all_rows:
        print()
        print("=" * 60)
        print("SAVING TO CSV")
        print("=" * 60)
        
        output_path = save_rows_to_csv(all_rows, OUTPUT_DIR)
        file_size_kb = output_path.stat().st_size / 1024
        
        print(f"✅ Saved {len(all_rows):,} rows to:")
        print(f"   {output_path}")
        print(f"   File size: {file_size_kb:,.1f} KB")

if __name__ == "__main__":
    main()