"""
seed_dates.py — Populate dim_date from 2023-01-01 to 2026-12-31.

Safe to re-run: ON CONFLICT DO NOTHING skips existing rows.
Verification: counts rows in database after commit.

Usage:
    python scripts/seed_dates.py
"""

import os
import psycopg2
from pathlib import Path
from datetime import date, timedelta
from dotenv import load_dotenv

# ── Paths & config ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

START_DATE = date(2023, 1, 1)
END_DATE   = date(2026, 12, 31)


def get_season(month: int) -> str:
    """
    Meteorolojik mevsim — Hollanda standartı:
    Winter: Dec, Jan, Feb
    Spring: Mar, Apr, May
    Summer: Jun, Jul, Aug
    Autumn: Sep, Oct, Nov
    """
    if month in (12, 1, 2):
        return "winter"
    elif month in (3, 4, 5):
        return "spring"
    elif month in (6, 7, 8):
        return "summer"
    else:
        return "autumn"


def generate_date_rows(start: date, end: date) -> list[tuple]:
    """
    start'tan end'e kadar her gün için bir tuple üret.
    date_id = YYYYMMDD integer (örnek: 20230115)
    day_of_week = ISO standarı: 1=Pazartesi, 7=Pazar
    """
    rows = []
    current = start
    while current <= end:
        rows.append((
            int(current.strftime("%Y%m%d")),  # date_id
            current,                           # full_date
            current.year,                      # year
            current.month,                     # month
            int(current.strftime("%V")),        # week (ISO)
            current.isoweekday(),              # day_of_week (1=Mon)
            get_season(current.month),         # season
        ))
        current += timedelta(days=1)
    return rows


def test_connection(conn):
    """Bağlantıyı doğrula — başarısız olursa hemen dur."""
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.close()
    print("✓ Database connection OK")


def seed(rows: list[tuple]):
    conn = psycopg2.connect(DATABASE_URL)

    # Prensip 2: önce bağlantıyı test et
    test_connection(conn)

    cur = conn.cursor()

    # Toplu insert — psycopg2.extras.execute_values tek seferde gönderir
    # Satır satır değil, ~1460 satır tek seferde → çok daha hızlı
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO dim_date
            (date_id, full_date, year, month, week, day_of_week, season)
        VALUES %s
        ON CONFLICT (date_id) DO NOTHING
    """, rows)

    conn.commit()

    # Prensip 1 & 3: rowcount değil, veritabanından say
    cur.execute("SELECT COUNT(*) FROM dim_date")
    count = cur.fetchone()[0]
    print(f"dim_date → {count} rows confirmed in database")
    print(f"           (expected: {len(rows)}, range: {START_DATE} → {END_DATE})")

    cur.close()
    conn.close()


if __name__ == "__main__":
    rows = generate_date_rows(START_DATE, END_DATE)
    print(f"Generated {len(rows)} date rows ({START_DATE} → {END_DATE})")
    seed(rows)

    