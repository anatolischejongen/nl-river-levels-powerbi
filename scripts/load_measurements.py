"""
load_measurements.py — Load all _3y.csv files into fact_measurements.

Strategy:
- Reads station_id lookup from dim_station (no hardcoding)
- Reads date_id lookup from dim_date (no hardcoding)
- Batch inserts via execute_values (1000 rows per batch)
- ON CONFLICT DO NOTHING for idempotency
- Verification: row count from database after each file

Usage:
    python scripts/load_measurements.py
"""

import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ── Paths & config ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
load_dotenv(PROJECT_ROOT / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

BATCH_SIZE = 1000  # kaç satırda bir Postgres'e gönderilsin


def test_connection(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.close()
    print("✓ Database connection OK\n")


def load_lookups(conn) -> tuple[dict, dict]:
    """
    dim_station ve dim_date tablolarından lookup dict'leri oluştur.

    Neden hardcode değil?
    dim_station'daki station_id'ler otomatik SERIAL — hangi değer
    atandığını önceden bilemeyiz. Veritabanından okumak tek güvenli yol.

    Döndürür:
        station_lookup: {"lobith.bovenrijn.tolkamer": 1, ...}
        date_lookup:    {20230101: 1, ...}  (date_id integer → date_id)
    """
    cur = conn.cursor()

    cur.execute("SELECT code, station_id FROM dim_station")
    station_lookup = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT date_id FROM dim_date")
    date_lookup = {row[0]: row[0] for row in cur.fetchall()}

    cur.close()
    print(f"✓ Loaded {len(station_lookup)} stations from dim_station")
    print(f"✓ Loaded {len(date_lookup)} dates from dim_date\n")
    return station_lookup, date_lookup


def csv_to_rows(
    df: pd.DataFrame,
    station_lookup: dict,
    date_lookup: dict,
    station_code: str
) -> tuple[list[tuple], int]:
    """
    DataFrame'i fact_measurements için tuple listesine dönüştür.

    Her satır için:
    - station_id: station_lookup'tan al
    - date_id: timestamp'ten YYYYMMDD integer üret, date_lookup'ta doğrula
    - Diğer alanlar: olduğu gibi

    Döndürür:
        rows: yüklenecek tuple listesi
        skipped: date_lookup'ta olmayan satır sayısı (veri aralığı dışı)
    """
    station_id = station_lookup.get(station_code)
    if station_id is None:
        print(f"  ✗ {station_code} not found in dim_station — skipping file")
        return [], 0

    # timestamp → datetime, sonra YYYYMMDD integer
    df["_ts"] = pd.to_datetime(df["timestamp"], utc=True)
    df["_date_id"] = df["_ts"].dt.strftime("%Y%m%d").astype(int)

    rows = []
    skipped = 0

    for _, row in df.iterrows():
        date_id = row["_date_id"]
        if date_id not in date_lookup:
            skipped += 1
            continue

        rows.append((
            station_id,
            date_id,
            row["_ts"].isoformat(),   # measured_at — TIMESTAMPTZ
            # Sentinel değeri filtrele: 999999999 = Rijkswaterstaat "no data" flag
            float(row["value_cm"]) if pd.notna(row["value_cm"]) and row["value_cm"] < 999999 else None,
            row["proces_type"],
            row["hoedanigheid"],
            int(row["quality_code"]) if pd.notna(row["quality_code"]) else None,
            row["status"],
        ))

    return rows, skipped


def insert_batches(cur, rows: list[tuple]) -> int:
    """
    Satırları BATCH_SIZE'lık gruplar halinde gönder.

    Neden batch?
    2 milyon satırı tek seferde göndermek belleği patlatır.
    1000'lik gruplar hem bellek dostu hem hızlı.
    """
    total_inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        execute_values(cur, """
            INSERT INTO fact_measurements
                (station_id, date_id, measured_at, value_cm,
                 proces_type, hoedanigheid, quality_code, status)
            VALUES %s
            ON CONFLICT (station_id, measured_at, proces_type) DO NOTHING
        """, batch)
        total_inserted += len(batch)
    return total_inserted


def load_file(path: Path, conn, station_lookup, date_lookup):
    """Tek bir _3y.csv dosyasını yükle."""
    station_code = path.stem.replace("_3y", "")
    print(f"→ {station_code}")

    df = pd.read_csv(path)
    print(f"  CSV rows: {len(df):,}")

    rows, skipped = csv_to_rows(df, station_lookup, date_lookup, station_code)

    if not rows:
        print(f"  ✗ No valid rows to insert\n")
        return

    if skipped:
        print(f"  ⚠ {skipped:,} rows skipped (outside date range)")

    cur = conn.cursor()
    insert_batches(cur, rows)
    conn.commit()

    # Prensip 1 & 3: gerçek sayıyı veritabanından al
    cur.execute(
        "SELECT COUNT(*) FROM fact_measurements WHERE station_id = %s",
        (station_lookup[station_code],)
    )
    db_count = cur.fetchone()[0]
    cur.close()

    print(f"  ✓ {db_count:,} rows confirmed in database for this station\n")


def main():
    csv_files = sorted(RAW_DIR.glob("*_3y.csv"))
    print(f"Found {len(csv_files)} CSV files in {RAW_DIR}\n")

    if not csv_files:
        print("✗ No _3y.csv files found. Run fetch_historical.py first.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    test_connection(conn)

    station_lookup, date_lookup = load_lookups(conn)

    for path in csv_files:
        load_file(path, conn, station_lookup, date_lookup)

    # Final verification
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fact_measurements")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()

    print("=" * 50)
    print(f"TOTAL rows in fact_measurements: {total:,}")
    print("=" * 50)


if __name__ == "__main__":
    main()