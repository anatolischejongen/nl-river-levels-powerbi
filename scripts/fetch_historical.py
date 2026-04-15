"""
fetch_historical.py — Fetch 3+ years of water level data for all 13 stations.

Yıl bazlı chunk stratejisi: her istasyon için yıl başına 1 istek.
Periyot: 2023 (tam), 2024 (tam), 2025 (tam), 2026 (3 ay: Ocak-Mart).
Neden chunks? Tek 3-yıllık istek ~302k satır → Rijkswaterstaat 160k limitini aşar.

Usage:
    python scripts/fetch_historical.py
"""

import time
import yaml
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from rws_api import fetch_station_data, parse_response_to_rows

# ── Paths (fetch_all_stations.py ile aynı pattern) ────────────────────
SCRIPT_DIR    = Path(__file__).parent
STATIONS_FILE = SCRIPT_DIR.parent / "data" / "reference" / "stations.yaml"
OUTPUT_DIR    = SCRIPT_DIR.parent / "data" / "raw"

REQUEST_DELAY = 0.8

# Her yıl için sabit UTC aralıkları (datetime objeleri)
YEAR_RANGES = {
    2023: (datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2023, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2024: (datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2024, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2025: (datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)),
    2026: (datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC), datetime(2026, 3, 31, 23, 59, 59, tzinfo=UTC))
}

# fetch_all_stations.py'den birebir kopyalandı
def load_stations(yaml_path):
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("stations", [])

def calculate_expected_rows(year):
    """
    Verilen yıl için beklenen satır sayısını hesapla (10-dk aralıklar).
    Leap year kontrolü ile doğru hesaplama.
    """
    if year == 2026:
        # 2026: sadece 3 ay (Ocak-Mart)
        days = 31 + 28 + 31  # 90 gün
    elif year == 2024:
        # 2024: leap year
        days = 366
    else:
        # 2023, 2025, 2027, ...: normal yıl
        days = 365
    
    # 10-dakikalık aralıklar: (gün × 24 × 60) / 10
    return days * 24 * 60 // 10

def fetch_one_station_historical(station):
    """
    Bir istasyon için yılları sırayla çeker, birleştirilmiş liste döner.
    fetch_all_stations.py'deki fetch_one_station() ile aynı mantık —
    sadece tek istek yerine yıl döngüsü var.
    """
    code = station["code"]
    all_rows = []

    for year, (start_time, end_time) in YEAR_RANGES.items():
        try:
            response_json = fetch_station_data(code, start_time, end_time)
            rows = parse_response_to_rows(response_json, code)
            all_rows.extend(rows)
            tqdm.write(f"    {year}: {len(rows):,} rows")
        except Exception as e:
            tqdm.write(f"    {year}: ✗ {type(e).__name__}: {e}")

        time.sleep(REQUEST_DELAY)

    return all_rows

def main():
    print("=" * 60)
    print("Rijkswaterstaat — Historical fetch (2022–2024, 3 years)")
    print("=" * 60)

    stations = load_stations(STATIONS_FILE)
    print(f"{len(stations)} stations loaded\n")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    quality_rows = []

    for station in tqdm(stations, desc="Stations", unit="station"):
        code = station["code"]
        tqdm.write(f"\n→ {station['name']} ({station['river']})")

        rows = fetch_one_station_historical(station)

        if rows:
            df = pd.DataFrame(rows)
            out_path = OUTPUT_DIR / f"{code}_3y.csv"
            df.to_csv(out_path, index=False, encoding="utf-8")
            tqdm.write(f"  ✓ {len(rows):,} rows → {out_path.name}")
        else:
            df = pd.DataFrame()
            tqdm.write(f"  ✗ No data for {code}")

        # Kalite raporu için satır
        meting_count = len(df[df["proces_type"] == "meting"]) if not df.empty else 0
        # Tüm periyotlar için beklenen toplamı hesapla
        expected = sum(calculate_expected_rows(year) for year in YEAR_RANGES.keys())
        quality_rows.append({
            "station_code":  code,
            "station_name":  station["name"],
            "river":         station["river"],
            "actual_meting": meting_count,
            "expected":      expected,
            "coverage_pct":  round(meting_count / expected * 100, 1) if expected > 0 else 0,
        })

    # Kalite raporu
    print("\n" + "=" * 60)
    quality_df = pd.DataFrame(quality_rows).sort_values("coverage_pct")
    print(quality_df.to_string(index=False))

    report_path = OUTPUT_DIR.parent / "sample" / "historical_quality_report.csv"
    quality_df.to_csv(report_path, index=False)
    print(f"\nQuality report saved: {report_path}")


if __name__ == "__main__":
    main()



