"""
scripts/add_coordinates.py
--------------------------
Fetches station coordinates from the Rijkswaterstaat DDAPI catalogue,
converts RD New (EPSG:28992) → WGS84 (EPSG:4326), and writes
lat/lon columns to dim_station in Postgres.

Two-phase workflow:
  Phase 1: Set INSPECT_MODE = True  → prints the raw API response, exit.
           Read the output, confirm the field structure, adjust parsing
           in extract_coordinates() if needed.
  Phase 2: Set INSPECT_MODE = False → runs the full migration.

Why coordinates come from the API, not hardcoded:
  The Rijkswaterstaat DDAPI is our single source of truth for all station
  metadata. Hardcoding coordinates from memory or Wikipedia introduces
  a maintenance risk and weakens the portfolio narrative ("all data from RWS").
  The catalogue endpoint returns the same station registry that the API
  uses internally — the coordinates are official.
"""

import os
import sys
import json
import yaml
import requests
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# ─────────────────────────────────────────────────────────
# CONFIGURATION — change only these values if needed
# ─────────────────────────────────────────────────────────

INSPECT_MODE = False  # ← Set to False after you've inspected the response

# Same base hostname used in rws_api.py (new Wadar endpoints, live Dec 2025)
# Path: METADATASERVICES (no _DBO suffix — that was the old API)
CATALOGUS_URL = (
    "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
    "/METADATASERVICES/OphalenCatalogus"
)

YAML_PATH = Path(__file__).parent.parent / "data" / "reference" / "stations.yaml"

# ─────────────────────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────────────────────

def load_station_codes(yaml_path: str) -> list[str]:
    """
    Read stations.yaml and return a list of station codes.

    Expected YAML structure:
      stations:
        - code: lobith.bovenrijn.tolkamer
          name: Lobith Tolkamer
          river: Bovenrijn
        - ...

    We only need the codes. Names and rivers are already in dim_station,
    seeded by seed_stations.py.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [s["code"] for s in data["stations"]]


def fetch_catalogue() -> dict:
    """
    POST to OphalenCatalogus filtered for WATHTE (waterhoogte) in OW
    (oppervlaktewater / surface water).

    Why one call for all 12 stations instead of 12 individual calls?
      The catalogue endpoint returns metadata for every matching station
      in one response. We filter to our 12 on the client side.
      Fewer API calls = more respectful of Rijkswaterstaat's rate limit,
      and we get a consistent snapshot of the full catalogue.

    Why WATHTE + OW?
      Same filters used throughout this project (rws_api.py, find_all_stations.py).
      Without them, the catalogue returns every measurement type at every
      station — water quality, salinity, flow rate — hundreds of entries
      we don't need.
    """
    body = {
        "CatalogusFilter": {
            "Grootheid": [{"Code": "WATHTE"}],
            "Compartiment": [{"Code": "OW"}]
        }
    }
    response = requests.post(CATALOGUS_URL, json=body, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_coordinates(catalogue_response: dict, our_codes: list[str]) -> dict:
    our_codes_set = set(our_codes)
    coords = {}

    locatie_list = catalogue_response.get("LocatieLijst", [])
    
    # DEBUG — kaldır sonra
    print(f"LocatieLijst uzunluğu: {len(locatie_list)}")
    print(f"YAML'dan gelen ilk 3 kod: {list(our_codes_set)[:3]}")
    print(f"API'den gelen ilk 3 kod: {[e.get('Code') for e in locatie_list[:3]]}")
    # DEBUG sonu

    for entry in locatie_list:
        code = entry.get("Code", "")
        if code in our_codes_set:
            lat = entry.get("Lat")
            lon = entry.get("Lon")
            if lat is not None and lon is not None:
                coords[code] = (float(lat), float(lon))
    
    return coords


def migrate_schema(conn) -> None:
    """
    ALTER TABLE dim_station to add lat and lon columns if they don't exist.

    NUMERIC(9,6): 9 total digits, 6 after the decimal point.
      Valid range: -999.999999 to +999.999999
      This covers all valid lat/lon values (-90 to +90, -180 to +180)
      with 10cm-level precision — far more than we need for a map visual.

    ADD COLUMN IF NOT EXISTS: idempotent — safe to re-run this script.
    If the columns already exist (e.g., partial earlier run), Postgres
    skips the ALTER silently instead of raising an error.
    """
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE dim_station
                ADD COLUMN IF NOT EXISTS lat NUMERIC(9,6),
                ADD COLUMN IF NOT EXISTS lon NUMERIC(9,6);
        """)
    conn.commit()
    print("  Schema: lat + lon columns ready in dim_station.")


def update_station_coordinates(
    conn,
    station_code: str,
    lat: float,
    lon: float
) -> None:
    """
    UPDATE one row in dim_station with its lat/lon.

    We match on code (text), not id (integer), because:
      - code is what we have from the API and YAML
      - Looking up id first would require an extra SELECT, for no gain
      - code has a UNIQUE constraint (set during seed_stations.py)

    cur.rowcount == 0 means no row matched the WHERE clause.
    This is a data integrity warning — the station exists in YAML but
    not in dim_station (shouldn't happen if seed_stations.py ran cleanly).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dim_station
               SET lat = %s,
                   lon = %s
             WHERE code = %s
            """,
            (lat, lon, station_code)
        )
        if cur.rowcount == 0:
            print(
                f"  WARNING: station_code '{station_code}' not found in "
                f"dim_station — did seed_stations.py run successfully?"
            )
    conn.commit()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()

    # 1. Load our 12 station codes from YAML
    codes = load_station_codes(YAML_PATH)
    print(f"Loaded {len(codes)} station codes from {YAML_PATH}.\n")

    # 2. Fetch the full WATHTE/OW catalogue from the API
    print("Fetching catalogue from Rijkswaterstaat DDAPI ...")
    catalogue = fetch_catalogue()
    print("  Catalogue received.\n")

    # ── INSPECT MODE ─────────────────────────────────────────────────
    # What to look for in the output:
    #
    # (a) "Top-level keys" → tells you which key holds the list.
    #     If you see ["AquoMetadataLijst", ...] → good, already handled.
    #     If you see something else → add it to the .get() chain in
    #     extract_coordinates() above.
    #
    # (b) "First entry in '...'" → confirm:
    #     - Does entry["Locatie"]["Code"] match our YAML codes (uppercased)?
    #       e.g. "LOBITH.BOVENRIJN.TOLKAMER" — if yes, matching will work.
    #       If the API uses a different code format, we'll need a mapping dict.
    #     - Does entry["Locatie"]["X"] and ["Y"] exist with numeric values?
    #       Those are the RD New coordinates we need.
    #
    # Set INSPECT_MODE = False once you're satisfied with the structure.
    # ─────────────────────────────────────────────────────────────────
    if INSPECT_MODE:
        print("=" * 55)
        print("INSPECT MODE — set INSPECT_MODE = False to run migration")
        print("=" * 55)
        print(f"\nTop-level keys: {list(catalogue.keys())}\n")

        # Tüm liste anahtarlarının ilk elemanını göster
        for key, val in catalogue.items():
            if isinstance(val, list) and len(val) > 0:
                print(f"── {key} (toplam {len(val)} giriş) ──")
                print(json.dumps(val[0], indent=2, ensure_ascii=False))
                print()
        # Lobith'in LocatieLijst'teki Code formatını görmek için
        print("── LocatieLijst içinde 'lobith' araması ──")
        for entry in catalogue.get("LocatieLijst", []):
            code_val = entry.get("Code", "")
            naam_val = entry.get("Naam", "")
            if "lobith" in code_val.lower() or "lobith" in naam_val.lower():
                print(json.dumps(entry, indent=2, ensure_ascii=False))
        sys.exit(0)
    # ─────────────────────────────────────────────────────────────────

    # 3. Extract coordinates for our 12 stations
    coords = extract_coordinates(catalogue, codes)
    print(f"Coordinates matched: {len(coords)}/{len(codes)} stations.")

    missing = [c for c in codes if c not in coords]
    if missing:
        print(f"\nWARNING — No coordinates found for:")
        for m in missing:
            print(f"  {m}")
        print(
            "\nPossible causes:\n"
            "  1. The API code format differs from the YAML code format.\n"
            "     Check INSPECT_MODE output — what does Locatie.Code look like?\n"
            "  2. The station is not in the WATHTE/OW catalogue.\n"
            "     Unlikely — we already proved these codes work for measurements.\n"
        )

    if not coords:
        print("No coordinates found — aborting. Run in INSPECT_MODE first.")
        sys.exit(1)

    # 4. Connect to Postgres
    print("\nConnecting to Postgres ...")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    try:
        # 4a. Add lat/lon columns if not present
        migrate_schema(conn)

        # 4b. Convert and UPDATE each matched station
        print("\nUpdating coordinates:\n")
        for code, (lat, lon) in coords.items():
            update_station_coordinates(conn, code, lat, lon)
            print(f"  ✓ {code:<42}  lat={lat:>10.6f}  lon={lon:>10.6f}")

        print(f"\n{'─'*55}")
        print(f"Done. {len(coords)} stations updated in dim_station.")
        if missing:
            print(f"Still missing: {len(missing)} stations — see warnings above.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()