"""
scripts/load_thresholds.py
--------------------------
Reads data/reference/thresholds.yaml and writes all 7 threshold
columns to dim_station in Postgres.

Columns added (all NUMERIC(8,1), nullable):
  verlaagd, normaal_min, normaal_max, licht_verhoogd,
  verhoogd, hoogwater, extreem_hoogwater

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import os
import yaml
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────

YAML_PATH = Path(__file__).parent.parent / "data" / "reference" / "thresholds.yaml"

THRESHOLD_COLUMNS = [
    "verlaagd",
    "normaal_min",
    "normaal_max",
    "licht_verhoogd",
    "verhoogd",
    "hoogwater",
    "extreem_hoogwater",
]

# ─────────────────────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────────────────────


def load_thresholds(yaml_path: Path) -> dict:
    """
    Read thresholds.yaml and return the 'thresholds' dict.

    Expected YAML structure:
      thresholds:
        lobith.bovenrijn.tolkamer:
          verlaagd: 600
          normaal_min: 800
          ...
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["thresholds"]


def migrate_schema(conn) -> None:
    """
    Add all 7 threshold columns to dim_station if not present.

    NUMERIC(8,1): 8 total digits, 1 after the decimal point.
      Supports values like -45.0 up to 9999999.9 — covers all stations
      including Borgharen (~4000+ cm range).

    ADD COLUMN IF NOT EXISTS: idempotent — safe to re-run this script.
    """
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE dim_station
                ADD COLUMN IF NOT EXISTS verlaagd          NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS normaal_min       NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS normaal_max       NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS licht_verhoogd   NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS verhoogd          NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS hoogwater         NUMERIC(8,1),
                ADD COLUMN IF NOT EXISTS extreem_hoogwater NUMERIC(8,1);
        """)
    conn.commit()
    print("  Schema: 7 threshold columns ready in dim_station.\n")


def update_thresholds(conn, station_code: str, values: dict) -> None:
    """
    UPDATE one row in dim_station with all 7 threshold values.

    Matches on 'code' column (text, unique) — same approach as
    update_station_coordinates() in add_coordinates.py.

    cur.rowcount == 0 means no row matched the WHERE clause.
    This is a data integrity warning — the station exists in YAML
    but not in dim_station (shouldn't happen if seed_stations.py ran).
    """
    set_clause = ", ".join(f"{col} = %s" for col in THRESHOLD_COLUMNS)
    params = [values.get(col) for col in THRESHOLD_COLUMNS]
    params.append(station_code)

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE dim_station SET {set_clause} WHERE code = %s",
            params,
        )
        if cur.rowcount == 0:
            print(
                f"  WARNING: '{station_code}' not found in dim_station — "
                f"did seed_stations.py run successfully?"
            )
    conn.commit()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────


def main() -> None:
    load_dotenv()

    # 1. Load thresholds from YAML
    thresholds = load_thresholds(YAML_PATH)
    print(f"Loaded thresholds for {len(thresholds)} stations.\n")

    # 2. Connect to Postgres
    print("Connecting to Postgres ...")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    try:
        # 2a. Add threshold columns if not present
        migrate_schema(conn)

        # 2b. UPDATE each station
        print("Updating thresholds:\n")
        for code, values in thresholds.items():
            update_thresholds(conn, code, values)
            hw  = values.get("hoogwater", "—")
            ext = values.get("extreem_hoogwater", "—")
            print(f"  ✓ {code:<45}  hoogwater={hw}  extreem={ext}")

        print(f"\n{'─'*60}")
        print(f"Done. {len(thresholds)} stations updated in dim_station.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()