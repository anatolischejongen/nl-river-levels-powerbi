# Project Roadmap — Dutch River Levels Monitor

> Bilingual document: English first, Nederlandse versie below.
> 
> *Last updated: April 2026*

---

## 🇬🇧 EN

### Project goal

Build a published Power BI dashboard tracking water levels across **5 Dutch rivers** (Bovenrijn, Nederrijn, Waal, IJssel, Maas) at **13 active Rijkswaterstaat measurement stations**, covering **3 years of historical data**, and surfacing where critical thresholds were crossed most often.

### Tech stack

- **Data source**: Rijkswaterstaat WaterWebservices API (new Wadar endpoints, live since Dec 2025)
- **Ingestion**: Python (`requests`, `PyYAML`)
- **Storage**: PostgreSQL via Supabase free tier
- **Modeling**: SQL (star schema) + DAX (Power BI measures)
- **Visualization**: Power BI Desktop + Power BI Service
- **Documentation**: Markdown, in `docs/`

### Architecture (target)

<!-- Rijkswaterstaat API (POST/JSON)
│
▼
Python ingestion scripts ─── stations.yaml (config)
│
▼
data/raw/*.csv   (intermediate, gitignored)
│
▼
PostgreSQL (Supabase) — star schema
│
▼
Power BI Desktop (.pbix)
│
▼
Power BI Service (published, public link) -->

### Status overview

| Phase | Description | Status |
|---|---|---|
| 0 | Repo skeleton & folder structure | ✅ Done |
| 1 | API connection & first successful fetch | ✅ Done |
| 1.5 | Station discovery & selection (13 stations, 5 rivers) | ✅ Done |
| 2 | Multi-station fetch loop with shared module | 🔵 In progress |
| 3 | CSV intermediate layer | ⬜ Planned |
| 4 | Scale to 3 years historical data | ⬜ Planned |
| 5 | Supabase setup & schema design | ⬜ Planned |
| 6 | CSV → Postgres loading | ⬜ Planned |
| 7 | Power BI connection | ⬜ Planned |
| 8 | DAX measures & data model | ⬜ Planned |
| 9 | Threshold research (riskiest step) | ⬜ Planned |
| 10–12 | Dashboard pages (Overview, Trends, Threshold Analysis) | ⬜ Planned |
| 13 | Documentation | ⬜ Planned |
| 14 | Power BI Service publish | ⬜ Planned |
| 15 | LinkedIn post | ⬜ Planned |

### Detailed steps

#### Phase 0 — Repo skeleton ✅
- [x] Folder structure created
- [x] Git repo initialized

#### Phase 1 — API connection ✅
- [x] Test connection to Rijkswaterstaat API
- [x] Discover the migration from classic to new Wadar endpoints (Dec 2025)
- [x] First successful POST request
- [x] First real data fetched (Lobith Tolkamer, 7 days, 1005 measurements)
- [x] Discover meting vs. verwachting parallel data streams

#### Phase 1.5 — Station discovery ✅
- [x] WFS catalogue exploration script
- [x] Discovery: `lobith.ponton` is a water-quality station, not water level
- [x] Filter by `GROOTHEIDCODE = WATHTE`
- [x] Search 12 target city names → analyze active vs. historical matches
- [x] Final selection: 13 stations across 5 rivers
- [x] `data/reference/stations.yaml` written and validated
- [x] `requirements.txt` created

#### Phase 2 — Multi-station fetch 🔵
- [ ] 2.1 — `stations.yaml` config ✅
- [ ] 2.2 — Refactor: extract reusable module `rws_api.py`
- [ ] 2.3 — Loop over 13 stations (7 days first)
- [ ] 2.4 — Apply parser to each station's response
- [ ] 2.5 — Add rate limiting (politeness toward API)
- [ ] 2.6 — Error handling: one failure shouldn't stop the loop
- [ ] 2.7 — Progress indicator

#### Phase 3 — CSV intermediate layer
- [ ] 3.1 — Convert parser output to `pandas.DataFrame`
- [ ] 3.2 — Save as `data/raw/water_levels_7days.csv`
- [ ] 3.3 — Visual inspection in Excel
- [ ] 3.4 — Add `data/raw/*` to `.gitignore`
- [ ] 3.5 — Create `data/sample/sample.csv` (first 100 rows, committed)

#### Phase 4 — Historical data (3 years)
- [ ] 4.1 — Extend time range from 7 days → 3 years
- [ ] 4.2 — Discovery point: Rijkswaterstaat's 160k measurement limit per request
- [ ] 4.3 — If needed: chunking by year
- [ ] 4.4 — Save per-station files: `data/raw/{station_code}_3y.csv`
- [ ] 4.5 — Data quality report (missing days, gaps, total counts)

#### Phase 5 — Supabase / PostgreSQL setup
- [ ] 5.1 — Create Supabase account & project
- [ ] 5.2 — Save connection string to `.env` (gitignored)
- [ ] 5.3 — Star schema design:
  - `fact_measurements`
  - `dim_station`
  - `dim_date`
- [ ] 5.4 — Write `sql/01_create_tables.sql`
- [ ] 5.5 — Run in Supabase SQL editor
- [ ] 5.6 — Seed `dim_station` from YAML

#### Phase 6 — CSV → Postgres loading
- [ ] 6.1 — Install `psycopg2` or `sqlalchemy`
- [ ] 6.2 — Write loader script `load_to_postgres.py`
- [ ] 6.3 — Convert string timestamp → `timestamp with time zone`
- [ ] 6.4 — Batch inserts (not row-by-row)
- [ ] 6.5 — Idempotency: re-runs shouldn't duplicate
- [ ] 6.6 — Row count verification

#### Phase 7 — Power BI connection
- [ ] 7.1 — Install Power BI Desktop *(macOS hurdle: Windows VM may be required)*
- [ ] 7.2 — Connect to Supabase via PostgreSQL connector
- [ ] 7.3 — Import fact + dim tables
- [ ] 7.4 — Build relationships in Model view
- [ ] 7.5 — First test chart: Lobith 3-year line graph

#### Phase 8 — Data model & DAX measures
- [ ] 8.1 — Create date table
- [ ] 8.2 — Build relationships
- [ ] 8.3 — Five core measures:
  - `Avg Level (cm NAP)`
  - `Max Level (cm NAP)`
  - `Days Above Threshold`
  - `YoY Avg Level Δ`
  - `Threshold Breach Rate`

#### Phase 9 — Threshold research ⚠️ Critical risk
- [ ] 9.1 — Locate official Rijkswaterstaat `meldpeilen` documentation
- [ ] 9.2 — Find verhoogd / hoog / extreem thresholds per station
- [ ] 9.3 — If unavailable: use what exists, mark gaps as "illustrative"
- [ ] 9.4 — Load to Postgres via `sql/03_seed_thresholds.sql`

> **Risk**: if real meldpeilen cannot be found, the LinkedIn hook ("Lobith surprised me") needs reframing.

#### Phase 10 — Dashboard page 1: Overview
- [ ] 10.1 — Netherlands map with station points
- [ ] 10.2 — Color by river, size by threshold breaches
- [ ] 10.3 — KPI cards (total stations, total measurements, top breaching station)

#### Phase 11 — Dashboard page 2: Trends
- [ ] 11.1 — Station slicer
- [ ] 11.2 — 3-year line chart
- [ ] 11.3 — Threshold lines overlay
- [ ] 11.4 — Year × week heat map (seasonality)

#### Phase 12 — Dashboard page 3: Threshold Analysis
- [ ] 12.1 — Station × threshold matrix
- [ ] 12.2 — Conditional formatting
- [ ] 12.3 — Insight callout box
- [ ] 12.4 — Anchor the "Lobith surprised me" insight to real data

#### Phase 13 — Documentation
- [ ] 13.1 — `README.md` — English main + Dutch summary paragraph
- [ ] 13.2 — Architecture diagram (visual)
- [ ] 13.3 — `docs/data-dictionary.md`
- [ ] 13.4 — `docs/thresholds.md` (sources)
- [ ] 13.5 — Reproduction steps
- [ ] 13.6 — Known limitations
- [ ] 13.7 — Screenshots
- [ ] 13.8 — `docs/lessons-learned.md`:
  - meting vs verwachting discovery
  - lobith.ponton vs lobith.bovenrijn.tolkamer
  - Wadar API migration (Dec 2025)
  - Sluis "boven" vs "beneden" hydrology
  - active ≠ continuously reliable

#### Phase 14 — Publish
- [ ] 14.1 — Power BI Service publish
- [ ] 14.2 — Public link (if possible)
- [ ] 14.3 — Screenshots
- [ ] 14.4 — Embed in README

#### Phase 15 — LinkedIn post
- [ ] 15.1 — Draft post (English + Dutch versions)
- [ ] 15.2 — Dashboard screenshot
- [ ] 15.3 — Hashtags: #Waterbeheer #Rijkswaterstaat #OpenData #PowerBI
- [ ] 15.4 — GitHub repo link
- [ ] 15.5 — Hook story
- [ ] 15.6 — Publish & monitor engagement

### Optional bonus phases (post-MVP)

- 🌟 Meting vs. verwachting comparison page (forecast accuracy visualization)
- 🌟 Forecast error analysis (hourly, seasonal)
- 🌟 Winter flood season (Nov–Mar) highlight page
- 🌟 Upstream-downstream lag analysis (Lobith → Kampen propagation)
- 🌟 Bilingual EN/NL dashboard language toggle

### Time budget (rough)

| Phase | Estimated hours |
|---|---|
| 1.5 (current) | 1 |
| 2 — Multi-station | 2–3 |
| 3 — CSV layer | 1 |
| 4 — 3-year scale | 2–3 |
| 5 — Supabase | 1–2 |
| 6 — Postgres load | 2 |
| 7 — Power BI connect | 1–3 (macOS factor) |
| 8 — DAX | 2 |
| 9 — Thresholds | 2–4 (risk) |
| 10–12 — Dashboard | 4–6 |
| 13 — Docs | 3 |
| 14 — Publish | 1 |
| 15 — LinkedIn | 1 |
| **Total** | **~25–30 hours** |

### Selected stations

| # | Code | Name | River | Region |
|---|---|---|---|---|
| 1 | `lobith.bovenrijn.tolkamer` | Lobith Tolkamer | Bovenrijn | Gelderland |
| 2 | `arnhem.nederrijn` | Arnhem | Nederrijn | Gelderland |
| 3 | `nijmegen.waal` | Nijmegen | Waal | Gelderland |
| 4 | `tiel.waal` | Tiel | Waal | Gelderland |
| 5 | `zutphen.ijssel` | Zutphen | IJssel | Gelderland |
| 6 | `deventer` | Deventer | IJssel | Overijssel |
| 7 | `olst` | Olst | IJssel | Overijssel |
| 8 | `zwolle.ijssel` | Zwolle | IJssel | Overijssel |
| 9 | `kampen.ijssel` | Kampen | IJssel | Overijssel |
| 10 | `maastricht.borgharen.maas.beneden` | Borgharen | Maas | Limburg |
| 11 | `roermond.boven` | Roermond | Maas | Limburg |
| 12 | `venlo` | Venlo | Maas | Limburg |
| 13 | `grave.beneden` | Grave | Maas | Noord-Brabant |

---

## 🇳🇱 NL

### Projectdoel

Een gepubliceerd Power BI-dashboard bouwen dat waterstanden volgt over **5 Nederlandse rivieren** (Bovenrijn, Nederrijn, Waal, IJssel, Maas) op **13 actieve Rijkswaterstaat-meetstations**, met **3 jaar historische data**, en zichtbaar maakt waar kritieke drempelwaarden het vaakst zijn overschreden.

### Techniek

- **Databron**: Rijkswaterstaat WaterWebservices API (nieuwe Wadar-endpoints, live sinds december 2025)
- **Inname**: Python (`requests`, `PyYAML`)
- **Opslag**: PostgreSQL via Supabase free tier
- **Modellering**: SQL (sterschema) + DAX (Power BI-maten)
- **Visualisatie**: Power BI Desktop + Power BI Service
- **Documentatie**: Markdown in `docs/`

### Architechture (target)

### Statusoverzicht

| Fase | Beschrijving | Status |
|---|---|---|
| 0 | Repo-skelet & mappenstructuur | ✅ Klaar |
| 1 | API-verbinding & eerste succesvolle fetch | ✅ Klaar |
| 1.5 | Stationselectie (13 stations, 5 rivieren) | ✅ Klaar |
| 2 | Multi-station fetch met gedeelde module | 🔵 Bezig |
| 3 | CSV-tussenlaag | ⬜ Gepland |
| 4 | Opschalen naar 3 jaar historische data | ⬜ Gepland |
| 5 | Supabase opzet & schema-ontwerp | ⬜ Gepland |
| 6 | CSV → Postgres laden | ⬜ Gepland |
| 7 | Power BI-verbinding | ⬜ Gepland |
| 8 | DAX-maten & datamodel | ⬜ Gepland |
| 9 | Drempelwaarden onderzoek (grootste risico) | ⬜ Gepland |
| 10–12 | Dashboardpagina's (Overzicht, Trends, Drempelanalyse) | ⬜ Gepland |
| 13 | Documentatie | ⬜ Gepland |
| 14 | Power BI Service publicatie | ⬜ Gepland |
| 15 | LinkedIn-post | ⬜ Gepland |

### Belangrijkste leerpunten tot nu toe

- **Wadar-migratie (dec 2025)**: de klassieke WaterWebservices worden eind april 2026 uitgezet. We bouwen direct op de nieuwe API om toekomstbestendig te zijn.
- **`lobith.ponton` valstrik**: dit is een waterkwaliteit-station, geen waterstand-station. Filtering op `GROOTHEIDCODE=WATHTE` is essentieel.
- **Twee parallelle datastromen**: voor elk station levert de API zowel `meting` (echte sensordata) als `verwachting` (modelvoorspellingen). Beide worden bewaard, alleen `meting` standaard getoond.
- **Stuw-stations**: bij sluizen zoals Grave en Roermond meten `boven` en `beneden` totaal verschillende peilen — bewust ontworpen voor waterbeheer.
- **Actief ≠ continu betrouwbaar**: sommige stations rapporteren wel recent maar hebben gaten in hun reeks. Naast laatste meting moet je continuïteit checken.
- **Geografische scope-discipline**: bewust gekozen om Rotterdam (getij-gedomineerd) en Groningen (ander hydrologisch systeem) buiten beschouwing te laten — een smaller verhaal is sterker dan een breder verhaal.

### Geselecteerde stations

(Zie tabel hierboven in de Engelse sectie — codes zijn taal-onafhankelijk.)

### Risico's

1. **`meldpeilen` vinden** — zonder officiële drempelwaarden valt de "Days Above Threshold"-meting weg. Onderzoek nodig in fase 9.
2. **Rijkswaterstaat 160k-metingen-limiet** — bij 3 jaar × 13 stations moeten we mogelijk per jaar chunks ophalen.
3. **API-stabiliteit tijdens migratieperiode** — Rijkswaterstaat verstoort de oude omgeving bewust. Onverwachte downtime mogelijk tot eind april 2026.

---

*This roadmap is a living document. Each completed step should be ticked off; each new discovery should be added to the relevant phase or to `docs/lessons-learned.md`.*

*Deze routekaart is een levend document. Elke voltooide stap wordt afgevinkt; elke nieuwe ontdekking wordt toegevoegd aan de relevante fase of aan `docs/lessons-learned.md`.*