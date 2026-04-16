# Session Context — nl-river-levels-powerbi

> This document captures the full state of the project so a new Claude 
> session can continue without losing context. Last updated: April 14, 2026.

## Who am I

Sam — job seeker in the Netherlands with Industrial Engineering + data/AI 
background, targeting Dutch public sector data analyst / data scientist / 
AI engineer roles. Learning Dutch (NT2 Staatsexamen II). Partner works in 
Dutch water management (waterbeheer), so the water sector is both a 
portfolio differentiator and a shared professional interest.

## Project

**Repo**: `nl-river-levels-powerbi` (week 1 of a 12-week portfolio roadmap)

**Goal**: A published Power BI dashboard tracking water levels at 13 
Rijkswaterstaat stations across 5 Dutch rivers (Bovenrijn, Nederrijn, 
Waal, IJssel, Maas), using 3 years of historical data, surfacing where 
critical thresholds are crossed most often.

**LinkedIn hook**: "Three years of Dutch river levels in one Power BI 
dashboard — here's where Lobith surprised me."

## Tech stack

- **Data source**: Rijkswaterstaat WaterWebservices API (the **new Wadar 
  endpoints**, live since Dec 5, 2025 — the classic endpoints are being 
  retired end of April 2026)
- **Ingestion**: Python (`requests`, `PyYAML`, `tqdm`, `pandas`)
- **Intermediate storage**: CSV in `data/raw/`
- **Target storage**: PostgreSQL via Supabase free tier (not yet set up)
- **Modeling**: SQL star schema + DAX in Power BI
- **Visualization**: Power BI Desktop → Power BI Service

## Key architectural decisions

1. **Python → Postgres → Power BI**, not Power Query alone. Decided 
   against DuckDB (extra ODBC setup for Power BI was demotivating) and 
   NoSQL (wrong fit for tabular time-series).
2. **YAML config** (`data/reference/stations.yaml`) separates station 
   metadata from code — Don't Repeat Yourself.
3. **Shared module** (`scripts/rws_api.py`) holds all API logic. Other 
   scripts import from it. DRY + testable.
4. **Defensive error handling**: one failing station does not stop the 
   whole run. Uses `try/except` + `tqdm.write()` for logs.
5. **Rate limiting**: 0.8 sec between API requests — respectful of 
   Rijkswaterstaat's "no unreasonable use" policy.
6. **CSV as intermediate layer** before Postgres. Lets us re-run 
   analysis without hitting the API, and provides a disaster-recovery 
   snapshot.

## Selected stations (12 active, 5 rivers)

Tiel (tiel.waal) was removed — JSONDecodeError for 2023–2025, only 2026 
data available. Excluded from dim_station and all analysis.

| # | Code | Name | River |
|---|---|---|---|
| 1 | `lobith.bovenrijn.tolkamer` | Lobith Tolkamer | Bovenrijn |
| 2 | `arnhem.nederrijn` | Arnhem | Nederrijn |
| 3 | `nijmegen.waal` | Nijmegen | Waal |
| 4 | `zutphen.ijssel` | Zutphen | IJssel |
| 5 | `deventer` | Deventer | IJssel |
| 6 | `olst` | Olst | IJssel |
| 7 | `zwolle.ijssel` | Zwolle | IJssel |
| 8 | `kampen.ijssel` | Kampen | IJssel |
| 9 | `maastricht.borgharen.maas.beneden` | Borgharen | Maas |
| 10 | `roermond.boven` | Roermond | Maas |
| 11 | `venlo` | Venlo | Maas |
| 12 | `grave.beneden` | Grave | Maas |

**Stations explicitly rejected and why**:
- Rotterdam: tidal-dominated, would break the threshold analysis
- Groningen: different hydrological system (northern canals), off-scope
- `lobith.ponton`: water quality station, not water level (GROOTHEIDCODE 
  = CONCTTE, not WATHTE) — initial trap we discovered
- Maastricht: geographically redundant with Borgharen (5 km apart)
- Tiel: API returns JSONDecodeError for 2023–2025, only 2026 partial data

## Key discoveries (lessons-learned.md content)

### 1. The `lobith.ponton` trap
When searching the WFS catalogue for "lobith", the first hit was 
`lobith.ponton` — but its `GROOTHEIDCODE` is `CONCTTE` (concentration, 
water quality), not `WATHTE` (waterhoogte, water level). Same location, 
different purpose. **Lesson**: filter by `GROOTHEIDCODE = WATHTE` 
first, then search by name.

### 2. Wadar API migration
Rijkswaterstaat is in the middle of migrating from classic 
WaterWebservices (being retired end of April 2026) to new Wadar 
endpoints (live since Dec 5, 2025). This project builds on the new 
API from day one. Hostname change: `waterwebservices.rijkswaterstaat.nl` 
→ `ddapi20-waterwebservices.rijkswaterstaat.nl`. Path change: 
`METADATASERVICES_DBO` → `METADATASERVICES` (no more `_DBO` suffix).

### 3. meting vs verwachting (two parallel streams)
Every station's response contains up to 2 entries in 
`WaarnemingenLijst`: one `ProcesType: "meting"` (real sensor readings) 
and one `ProcesType: "verwachting"` (model forecasts). Rijkswaterstaat 
stores both for forecast-accuracy evaluation. The parser captures 
both. Default Power BI view shows only `meting`; `verwachting` is 
available for a future bonus "forecast accuracy" page. Arnhem is the 
exception — no `verwachting` is published for the Nederrijn arm.

### 4. NAP vs TAW — cross-border reference systems
Borgharen returned **3 entries** instead of 2. Investigation revealed 
a second `meting` entry with `Hoedanigheid: TAW` (Tweede Algemene 
Waterpassing — Belgium's reference datum, based on Oostende low-tide 
level, ~2.33 m offset from Dutch NAP). Borgharen is a few km from the 
Belgian border; Rijkswaterstaat publishes in both national references 
for cross-border water management coordination. **Solution**: parser 
now accepts a `accepted_hoedanigheid=("NAP",)` parameter (default: 
NAP only). This filters out TAW cleanly. Great interview story 
material.

### 5. Sluis "boven" vs "beneden"
At weir stations (Grave, Roermond), "boven" (upstream of weir) and 
"beneden" (downstream) measure completely different water levels 
because the weir holds water back. Grave: picked `beneden`. 
Roermond: only `boven` is active (the `beneden` sensor died in 1995). 
Noted as a documentation limitation.

### 6. Active ≠ reliable
Some stations report recent data but have gaps. The WFS catalogue 
last-measurement field tells you if a station is alive, but not 
whether its time series is continuous. Must check both.

### 7. Rijkswaterstaat sentinel value
`value_cm = 999999999` means "no data" — not a real measurement. 
Must be filtered to NULL before loading to Postgres. Discovered during 
Phase 6 when NUMERIC(7,1) overflow error appeared.

### 8. WMCN maatgevende meetpunten
WMCN (Watermanagementcentrum Nederland) uses only two reference points 
for national alarm color codes: **Lobith** (Rijn) and **Sint Pieter** 
(Maas). All other stations are derived. Official thresholds only exist 
for these two points. Sint Pieter and Borgharen are 5 km apart on the 
same river with no weir between them — Sint Pieter thresholds apply 
to Borgharen.

Source: LDHO 2023 Bijlage D — 
https://iplo.nl/publish/pages/225787/landelijk-draaiboek-hoogwater-en-overstromingsdreiging-ldho-2023.pdf

## Current file structure

```
nl-river-levels-powerbi/
├── README.md
├── requirements.txt        # requests, PyYAML, tqdm, pandas, psycopg2-binary, python-dotenv
├── .gitignore              # standard Python + data/raw/* + .env
├── .env                    # DATABASE_URL — gitignored, must be recreated manually
├── docs/
│   ├── ROADMAP.md
│   └── session-context.md
├── data/
│   ├── reference/
│   │   └── stations.yaml   # 12 active stations (Tiel removed)
│   ├── raw/                # gitignored
│   │   ├── {station_code}_3y.csv   # 12 files, ~2.2M rows total
│   │   └── historical_quality_report.csv
│   └── sample/
│       └── sample.csv      # 100 rows, committed
└── scripts/
    ├── rws_api.py               # shared module (API + parser)
    ├── fetch_all_stations.py    # 7-day operational fetch
    ├── fetch_historical.py      # 3-year historical fetch (2023–2026)
    ├── seed_stations.py         # loads dim_station from YAML
    ├── seed_dates.py            # generates + loads dim_date (2023–2026)
    ├── load_measurements.py     # loads fact_measurements from CSVs
    ├── fetch_lobith_real.py     # legacy sanity-check (can be deleted)
    ├── find_all_stations.py     # WFS catalogue search
    ├── inspect_anomalies.py     # one-off diagnostic
    └── test_load_stations.py    # YAML loader validation
```

## Module: rws_api.py

Four functions:

1. `build_request_body(station_code, start_time, end_time)` — constructs 
   the POST body for OphalenWaarnemingen. Location by code only (no 
   X/Y coordinates — that was an earlier mistake).
2. `fetch_station_data(station_code, start_time, end_time, timeout=30)` 
   — makes the POST call. Uses `response.raise_for_status()`. Returns 
   parsed JSON dict. Raises on HTTP errors.
3. `parse_response_to_rows(response_json, station_code, accepted_hoedanigheid=("NAP",))` 
   — walks `WaarnemingenLijst → MetingenLijst`, produces flat row 
   dicts with 7 fields: station_code, timestamp, value_cm, 
   proces_type, hoedanigheid, quality_code, status. Filters on 
   `Hoedanigheid` (default NAP only — this is where the TAW filter 
   lives).
4. `get_default_time_range(days=7)` — convenience helper returning 
   `(start, end)` tuple of timezone-aware UTC datetimes.

**Endpoint**: `https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen`  
**Time format**: `"%Y-%m-%dT%H:%M:%S.000+00:00"`  
**Compartiment**: always `OW` (oppervlaktewater)  
**Grootheid**: always `WATHTE` (waterhoogte)

## Supabase database

**Project**: `nl-river-levels-powerbi`  
**Host**: `db.hdapdtwkisfwgdxjohsp.supabase.co`  
**Region**: West EU (Paris) — eu-west-3, t4g.nano  
**Connection**: Session Pooler (IPv4 compatible)  
**Pooler host**: `aws-0-eu-west-3.pooler.supabase.com:5432`  
**User**: `postgres.hdapdtwkisfwgdxjohsp`

### Star schema

**`dim_station`** — 12 rows, seeded from stations.yaml  
**`dim_date`** — 1,461 rows (2023-01-01 → 2026-12-31)  
**`fact_measurements`** — 2,258,144 rows (meting + verwachting both loaded)

```sql
fact_measurements
  id           BIGSERIAL PK
  station_id   INTEGER FK → dim_station
  date_id      INTEGER FK → dim_date  (YYYYMMDD format)
  measured_at  TIMESTAMPTZ
  value_cm     NUMERIC       -- sentinel 999999999 stored as NULL
  proces_type  TEXT          -- 'meting' | 'verwachting'
  hoedanigheid TEXT          -- 'NAP' (TAW filtered out at parse time)
  quality_code SMALLINT
  status       TEXT
  UNIQUE (station_id, measured_at, proces_type)
```

## Power BI

**Connection method**: ODBC via PostgreSQL Unicode(x64) driver  
(Native PostgreSQL connector had SSL certificate incompatibility with Supabase pooler)

**Model**: 3 tables loaded, relationships auto-detected correctly:
- `dim_station` (1) → `fact_measurements` (*)
- `dim_date` (1) → `fact_measurements` (*)

### DAX Measures (all in fact_measurements table)

```
Avg Level (cm NAP)     — AVERAGE(value_cm) WHERE proces_type = "meting"
Max Level (cm NAP)     — MAX(value_cm) WHERE proces_type = "meting"
Total Measurements     — COUNTROWS WHERE proces_type = "meting"
Days Above Geel        — DISTINCTCOUNT(date_id) WHERE value_cm >= threshold
                         Lobith: 1200 cm | Borgharen: 4500 cm | others: BLANK()
Days Above Oranje      — same pattern
                         Lobith: 1500 cm | Borgharen: 4620 cm | others: BLANK()
YoY Avg Level Delta    — current year avg minus previous year avg
```

### Official thresholds (LDHO 2023 Bijlage D)

| Station | Geel | Oranje | Rood | Source |
|---|---|---|---|---|
| Lobith | 1200 (zomer) / 1300 (winter) cm | 1500 cm | 1650 cm | LDHO 2023 |
| Borgharen | 4500 cm | 4620 cm | 4725 cm | LDHO 2023 Sint Pieter (5km upstream, same river) |
| All others | — | — | — | No official meldpeilen available |

### Key insight confirmed in data

January 2024 flood event: Lobith reached 14.35m NAP — visible as a sharp 
peak in the line chart. Documented in WMCN hoogwater reports and 
Rijkswaterstaat news archives.

## Progress vs ROADMAP.md

- ✅ Phase 0 — Repo skeleton
- ✅ Phase 1 — API connection
- ✅ Phase 1.5 — Station discovery and selection (12 stations, Tiel excluded)
- ✅ Phase 2 — Multi-station fetch (rws_api module + fetch_all_stations.py)
- ✅ Phase 3 — CSV intermediate layer
- ✅ Phase 4 — Historical data 2023–2026 (2.2M rows, 12 files)
- ✅ Phase 5 — Supabase setup + star schema
- ✅ Phase 6 — CSV → Postgres loading (2,258,144 rows)
- ✅ Phase 7 — Power BI connection (via ODBC)
- ✅ Phase 8 — DAX measures (6 measures)
- ✅ Phase 9 — Threshold research (LDHO 2023, Lobith + Borgharen)
- 🔵 **Phase 10 — Dashboard page 1: Overview** ← NEXT
- ⬜ Phase 11 — Dashboard page 2: Trends
- ⬜ Phase 12 — Dashboard page 3: Threshold Analysis
- ⬜ Phase 13 — Documentation
- ⬜ Phase 14 — Power BI Service publish
- ⬜ Phase 15 — LinkedIn post

## Phase 10 — next steps

Overview page plan (agreed):
1. **KPI cards** (top strip): Total Measurements, Active Stations (12), Days Above Geel (Lobith)
2. **Map**: stations as bubbles, colored by river, sized by Avg Level
   - ⚠️ dim_station has no lat/lon columns yet — need to add coordinates
   - Power BI Map visual needs lat/lon or will try to geocode from name (unreliable)
3. **Station table**: name, river, Avg Level, Max Level, Days Above Geel
4. **River slicer**: filters map + table

**Pending decision**: Add lat/lon to dim_station before building the map visual.
Known coordinates for all 12 stations available — Claude can provide them.

## Sam's learning style

- Prefers understanding over execution. No "run this script" without 
  explaining what it does, what it's looking for, where, and why.
- Values modular, professional code style over quick hacks.
- Appreciates architectural reasoning (DRY, YAGNI, PEP 8 import groups, 
  defensive programming).
- Thinks about interview narratives — what story can I tell?
- Bilingual communication: Turkish primary, English for code/docs, 
  Dutch summary at the end of each major response.

## Communication preferences (user preferences)

- Respond in Turkish by default, with a Dutch summary at the end. 
  No Turkish in committed files (code comments, README, docs are EN+NL).
- For Dutch language help, include Turkish translations and 
  dictionary-style explanations.
- No paraphrasing. Don't restate what the user said.
- Source discipline: flag estimations, never fabricate statistics.
- Co-creation: verify direction before producing work.
- Pre-solution check: evaluate before proposing solutions.
- Constructive critique when fundamental assumptions look problematic.
- Code explanations always required: every snippet must explain what,
  where, why — no ready-to-run scripts without understanding.

## Known limitations / TODOs

- `fetch_lobith_real.py` — legacy sanity-check, can be deleted
- `inspect_anomalies.py` — one-off diagnostic, keep as reference
- `dim_station` has no lat/lon — needed for Power BI map visual
- Tiel excluded from analysis — only 2026 partial data available
- Thresholds only available for Lobith and Borgharen — 10 other 
  stations show BLANK() in threshold measures, documented as limitation
- Supabase free tier — no backups, no branching
