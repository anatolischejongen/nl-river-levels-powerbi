"""
seed_stations.py — Load 12 stations from stations.yaml into dim_station.

Run once. Re-running is safe: ON CONFLICT DO NOTHING skips existing rows.

Usage:
    python scripts/seed_stations.py
"""

import yaml
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os

# ── Paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR    = Path(__file__).parent
PROJECT_ROOT  = SCRIPT_DIR.parent
STATIONS_FILE = PROJECT_ROOT / "data" / "reference" / "stations.yaml"

# ── Load .env ──────────────────────────────────────────────────────────
# python-dotenv, .env dosyasını okuyup os.environ'a yükler.
# Böylece kod içinde şifre yazmadan DATABASE_URL'ye erişiriz.
load_dotenv(PROJECT_ROOT / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

def load_stations():
    with open(STATIONS_FILE, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])


def seed(stations):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    for s in stations:
        cur.execute("""
            INSERT INTO dim_station (code, name, river, region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING
        """, (s["code"], s["name"], s["river"], s.get("region")))

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dim_station")
    count = cur.fetchone()[0]
    print(f"dim_station → {count} rows in database")

    cur.close()
    conn.close()


if __name__ == "__main__":
    stations = load_stations()
    print(f"Loaded {len(stations)} stations from YAML")
    seed(stations)