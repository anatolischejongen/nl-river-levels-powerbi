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

## Selected stations (13 total, 5 rivers)

| # | Code | Name | River |
|---|---|---|---|
| 1 | `lobith.bovenrijn.tolkamer` | Lobith Tolkamer | Bovenrijn |
| 2 | `arnhem.nederrijn` | Arnhem | Nederrijn |
| 3 | `nijmegen.waal` | Nijmegen | Waal |
| 4 | `tiel.waal` | Tiel | Waal |
| 5 | `zutphen.ijssel` | Zutphen | IJssel |
| 6 | `deventer` | Deventer | IJssel |
| 7 | `olst` | Olst | IJssel |
| 8 | `zwolle.ijssel` | Zwolle | IJssel |
| 9 | `kampen.ijssel` | Kampen | IJssel |
| 10 | `maastricht.borgharen.maas.beneden` | Borgharen | Maas |
| 11 | `roermond.boven` | Roermond | Maas |
| 12 | `venlo` | Venlo | Maas |
| 13 | `grave.beneden` | Grave | Maas |

**Stations explicitly rejected and why**:
- Rotterdam: tidal-dominated, would break the threshold analysis
- Groningen: different hydrological system (northern canals), off-scope
- `lobith.ponton`: water quality station, not water level (GROOTHEIDCODE 
  = CONCTTE, not WATHTE) — initial trap we discovered
- Maastricht: geographically redundant with Borgharen (5 km apart)

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

## Current file structure
nl-river-levels-powerbi/
├── README.md                        # empty / minimal so far
├── requirements.txt                 # requests, PyYAML, tqdm, pandas
├── .gitignore                       # standard Python + data/raw/*
├── docs/
│   ├── ROADMAP.md                   # bilingual EN/NL, 15 phases
│   └── session-context.md           # THIS FILE
├── data/
│   ├── reference/
│   │   └── stations.yaml            # 13 stations with metadata
│   ├── raw/                         # gitignored
│   │   └── water_levels_7days_2026-04-14.csv  # ~2 MB, 25123 rows
│   └── sample/
│       └── sample.csv               # 100 rows, committed
└── scripts/
├── rws_api.py                   # shared module (API + parser)
├── fetch_all_stations.py        # main ingestion script
├── fetch_lobith_real.py         # sanity check, single station
├── find_all_stations.py         # WFS catalogue search
├── inspect_anomalies.py         # one-off diagnostic
└── test_load_stations.py        # YAML loader validation


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

## Progress vs ROADMAP.md

- ✅ Phase 0 — Repo skeleton
- ✅ Phase 1 — API connection
- ✅ Phase 1.5 — Station discovery and selection
- ✅ Phase 2 — Multi-station fetch (rws_api module + fetch_all_stations.py)
- ✅ Phase 3 — CSV intermediate layer (25,123 rows, 2 MB, verified)
- ⬜ **Phase 4 — Scale to 3 years historical data** ← NEXT
- ⬜ Phase 5 — Supabase / Postgres setup
- ⬜ Phase 6 — CSV → Postgres loading
- ⬜ Phase 7 — Power BI connection
- ⬜ Phase 8 — DAX measures
- ⬜ Phase 9 — Threshold research (riskiest phase, save Opus for this)
- ⬜ Phases 10–12 — Dashboard pages
- ⬜ Phase 13 — Documentation
- ⬜ Phase 14 — Publish
- ⬜ Phase 15 — LinkedIn post

## Phase 4 preview (what's next)

Scale from 7 days → 3 years.

**Known risk**: Rijkswaterstaat API has a **160,000 measurements per 
request** limit. One station × 3 years × 10-minute intervals ≈ 158,000 
rows. Right at the edge. If we exceed the limit for any station, we 
need **chunking by year** — split one 3-year request into three 1-year 
requests and concatenate.

**Likely approach**:
1. Modify `fetch_all_stations.py` (or create `fetch_historical.py`) to 
   accept a date range parameter instead of hardcoded 7 days
2. Add chunking logic if a single request would exceed 160k
3. Save per-station files: `data/raw/{station_code}_3y.csv`
4. Produce a data quality report (missing days, gaps, total counts)

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

## Known limitations / TODOs


- `fetch_lobith_real.py` is a legacy sanity-check; can be deleted once 
  `fetch_all_stations.py` is battle-tested
- `inspect_anomalies.py` is a one-off diagnostic; keep as reference
- Threshold values (meldpeilen) still not located — biggest outstanding 
  risk for Phase 9