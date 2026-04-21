"""
build_dashboard_data.py — Query Supabase and build docs/index.html.

Reads from fact_measurements + dim_station, produces pre-aggregated JSON,
and injects it into a self-contained HTML dashboard (Chart.js via CDN).

Usage:
    python scripts/build_dashboard_data.py
"""

import os
import json
import decimal
import psycopg2
import psycopg2.extras
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv

SCRIPT_DIR   = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_PATH  = PROJECT_ROOT / "docs" / "index.html"

load_dotenv(PROJECT_ROOT / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

# ── SQL queries ───────────────────────────────────────────────────────

SQL_WATER_LEVELS = """
SELECT
    to_char(date_trunc('month', f.measured_at), 'YYYY-MM') AS month,
    s.name,
    s.river,
    s.code,
    ROUND(AVG(f.value_cm)::numeric, 1)  AS avg_cm,
    ROUND(MAX(f.value_cm)::numeric, 1)  AS max_cm
FROM fact_measurements f
JOIN dim_station s USING (station_id)
WHERE f.proces_type = 'meting'
  AND f.grootheid   = 'WATHTE'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4, 2
"""

SQL_DISCHARGE = """
SELECT
    to_char(date_trunc('month', f.measured_at), 'YYYY-MM') AS month,
    s.name,
    s.river,
    s.code,
    ROUND(AVG(f.value_cm)::numeric, 1)  AS avg_m3s,
    ROUND(MAX(f.value_cm)::numeric, 1)  AS max_m3s
FROM fact_measurements f
JOIN dim_station s USING (station_id)
WHERE f.proces_type = 'meting'
  AND f.grootheid   = 'Q'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4, 2
"""

SQL_THRESHOLD_DAYS = """
SELECT
    s.name,
    s.river,
    s.code,
    EXTRACT(year FROM f.measured_at)::int AS year,
    COUNT(DISTINCT f.measured_at::date)   AS days_above
FROM fact_measurements f
JOIN dim_station s USING (station_id)
WHERE f.proces_type = 'meting'
  AND f.grootheid   = 'WATHTE'
  AND f.value_cm    > s.licht_verhoogd
GROUP BY 1, 2, 3, 4
ORDER BY 3, 4
"""

SQL_SUMMARY = """
SELECT
    COUNT(DISTINCT s.station_id)::int           AS total_stations,
    COUNT(DISTINCT s.river)::int                AS total_rivers,
    COUNT(*)::int                               AS total_measurements,
    MIN(f.measured_at)::date                    AS data_from,
    MAX(f.measured_at)::date                    AS data_to
FROM fact_measurements f
JOIN dim_station s USING (station_id)
WHERE f.grootheid = 'WATHTE' AND f.proces_type = 'meting'
"""

SQL_STATION_SUMMARY = """
SELECT
    s.name,
    s.river,
    s.code,
    ROUND(AVG(f.value_cm)::numeric, 1)  AS avg_cm,
    ROUND(MAX(f.value_cm)::numeric, 1)  AS max_cm,
    s.licht_verhoogd                    AS threshold_cm
FROM fact_measurements f
JOIN dim_station s USING (station_id)
WHERE f.proces_type = 'meting'
  AND f.grootheid   = 'WATHTE'
GROUP BY s.name, s.river, s.code, s.licht_verhoogd
ORDER BY s.river, s.name
"""

# ── HTML template (split at injection point) ─────────────────────────

HTML_BEFORE_DATA = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dutch River Monitor — NL Water Levels</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --bg:       #f1f5f9;
  --surface:  #ffffff;
  --border:   #e2e8f0;
  --text:     #1e293b;
  --muted:    #64748b;
  --accent:   #1d4ed8;
  --header-bg:#0f172a;
  --Bovenrijn:#3b82f6;
  --Waal:     #22c55e;
  --Maas:     #f97316;
  --Nederrijn:#a855f7;
  --IJssel:   #06b6d4;
}
body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; font-size: 14px; }
a { color: var(--accent); }

/* ── Header ── */
header {
  background: var(--header-bg);
  color: #f8fafc;
  padding: 18px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
}
header h1 { font-size: 1.25rem; font-weight: 700; letter-spacing: -0.02em; }
header .subtitle { font-size: 0.8rem; color: #94a3b8; margin-top: 2px; }
.filters { display: flex; gap: 10px; flex-wrap: wrap; }
.filters select {
  background: #1e293b;
  color: #f8fafc;
  border: 1px solid #334155;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 13px;
  cursor: pointer;
}
.filters select:focus { outline: 2px solid var(--accent); }

/* ── KPI cards ── */
.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 12px;
  padding: 20px 24px 0;
}
.kpi-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px 18px;
}
.kpi-card .value { font-size: 1.6rem; font-weight: 700; color: var(--accent); line-height: 1.1; }
.kpi-card .label { font-size: 0.75rem; color: var(--muted); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.05em; }

/* ── Chart grid ── */
.charts-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  padding: 16px 24px;
}
.chart-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 18px;
}
.chart-card.full-width { grid-column: 1 / -1; }
.chart-card h2 { font-size: 0.875rem; font-weight: 600; color: var(--text); margin-bottom: 14px; }
.chart-card .chart-subtitle { font-size: 0.75rem; color: var(--muted); margin-top: -10px; margin-bottom: 14px; }
.chart-wrap { position: relative; height: 240px; }
.chart-card.full-width .chart-wrap { height: 280px; }

/* ── Table ── */
.table-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  margin: 0 24px 24px;
  overflow: hidden;
}
.table-card h2 { font-size: 0.875rem; font-weight: 600; padding: 16px 18px 12px; border-bottom: 1px solid var(--border); }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th {
  background: #f8fafc;
  text-align: left;
  padding: 9px 14px;
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--muted);
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
  border-bottom: 1px solid var(--border);
}
th:hover { background: #f1f5f9; color: var(--text); }
th .sort-icon { margin-left: 4px; opacity: 0.4; }
th.sort-asc .sort-icon, th.sort-desc .sort-icon { opacity: 1; }
td { padding: 9px 14px; border-bottom: 1px solid #f1f5f9; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #f8fafc; }
.river-dot {
  display: inline-block;
  width: 9px; height: 9px;
  border-radius: 50%;
  margin-right: 6px;
  vertical-align: middle;
}
.num { text-align: right; font-variant-numeric: tabular-nums; }

/* ── Legend strip ── */
.legend-strip {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  padding: 0 24px 4px;
}
.legend-item { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.legend-swatch { width: 12px; height: 3px; border-radius: 2px; }

/* ── Footer ── */
footer {
  text-align: center;
  padding: 16px 24px;
  font-size: 0.75rem;
  color: var(--muted);
  border-top: 1px solid var(--border);
  margin-top: 8px;
}

@media (max-width: 640px) {
  .charts-grid { grid-template-columns: 1fr; }
  .kpi-grid { grid-template-columns: repeat(2, 1fr); }
  header { flex-direction: column; align-items: flex-start; }
}
</style>
</head>
<body>

<header>
  <div>
    <h1>Dutch River Monitor</h1>
    <div class="subtitle">Rijkswaterstaat · Water level &amp; discharge · 2023–2026</div>
  </div>
  <div class="filters">
    <select id="riverFilter" onchange="applyFilter()">
      <option value="">All rivers</option>
    </select>
    <select id="stationFilter" onchange="applyFilter()">
      <option value="">All stations</option>
    </select>
  </div>
</header>

<div class="kpi-grid">
  <div class="kpi-card"><div class="value" id="kpi-stations">—</div><div class="label">Stations</div></div>
  <div class="kpi-card"><div class="value" id="kpi-rivers">—</div><div class="label">Rivers</div></div>
  <div class="kpi-card"><div class="value" id="kpi-meas">—</div><div class="label">Readings</div></div>
  <div class="kpi-card"><div class="value" id="kpi-range">—</div><div class="label">Date range</div></div>
</div>

<div class="charts-grid">
  <div class="chart-card full-width">
    <h2>Monthly Average Water Level (cm NAP)</h2>
    <p class="chart-subtitle">WATHTE · meting · per station</p>
    <div class="chart-wrap"><canvas id="levelChart"></canvas></div>
  </div>
  <div class="chart-card">
    <h2>Monthly Average Discharge (m³/s)</h2>
    <p class="chart-subtitle">Q · meting · selected stations</p>
    <div class="chart-wrap"><canvas id="dischargeChart"></canvas></div>
  </div>
  <div class="chart-card">
    <h2>Days Above licht_verhoogd Threshold / Year</h2>
    <p class="chart-subtitle">WATHTE daily max &gt; station threshold</p>
    <div class="chart-wrap"><canvas id="thresholdChart"></canvas></div>
  </div>
</div>

<div class="table-card">
  <h2>Station Summary</h2>
  <div class="table-wrap">
    <table id="stationTable">
      <thead>
        <tr>
          <th onclick="sortTable(0)" data-col="0">Station <span class="sort-icon">↕</span></th>
          <th onclick="sortTable(1)" data-col="1">River <span class="sort-icon">↕</span></th>
          <th onclick="sortTable(2)" data-col="2" class="num">Avg cm NAP <span class="sort-icon">↕</span></th>
          <th onclick="sortTable(3)" data-col="3" class="num">Max cm NAP <span class="sort-icon">↕</span></th>
          <th onclick="sortTable(4)" data-col="4" class="num">Threshold cm <span class="sort-icon">↕</span></th>
        </tr>
      </thead>
      <tbody id="tableBody"></tbody>
    </table>
  </div>
</div>

<footer>
  Data: <a href="https://waterwebservices.rijkswaterstaat.nl" target="_blank" rel="noopener">Rijkswaterstaat WaterWebservices</a> &nbsp;|&nbsp;
  Built: <span id="builtDate"></span> &nbsp;|&nbsp;
  <a href="https://github.com/anatolischejongen/nl-river-levels-powerbi" target="_blank" rel="noopener">GitHub</a>
</footer>

<script>
/* ── Data injected by build_dashboard_data.py ─────────────────────── */
"""

HTML_AFTER_DATA = """\

/* ── River colour palette ─────────────────────────────────────────── */
const RIVER_COLORS = {
  'Bovenrijn': '#3b82f6',
  'Waal':      '#22c55e',
  'Maas':      '#f97316',
  'Nederrijn': '#a855f7',
  'IJssel':    '#06b6d4',
};
function riverColor(river) {
  return RIVER_COLORS[river] || '#94a3b8';
}

/* ── Helpers ──────────────────────────────────────────────────────── */
function unique(arr, key) {
  return [...new Set(arr.map(r => r[key]))].sort();
}
function fmt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('nl-NL');
}

/* ── Chart instances ─────────────────────────────────────────────── */
let levelChart, dischargeChart, thresholdChart;

const CHART_DEFAULTS = {
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
  plugins: {
    legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 11 }, padding: 10 } },
    tooltip: { mode: 'index', intersect: false }
  },
  scales: {
    x: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 }, maxRotation: 45 } },
    y: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } } }
  }
};

/* ── Populate dropdowns ──────────────────────────────────────────── */
function populateDropdowns() {
  const rivers = unique(DATA.water_levels, 'river');
  const rSel = document.getElementById('riverFilter');
  rivers.forEach(r => {
    const o = document.createElement('option');
    o.value = r; o.textContent = r;
    rSel.appendChild(o);
  });
}

function updateStationDropdown(river) {
  const sSel = document.getElementById('stationFilter');
  const prev = sSel.value;
  sSel.innerHTML = '<option value="">All stations</option>';
  const rows = river ? DATA.water_levels.filter(r => r.river === river) : DATA.water_levels;
  const stations = unique(rows, 'name');
  stations.forEach(n => {
    const o = document.createElement('option');
    o.value = n; o.textContent = n;
    sSel.appendChild(o);
  });
  if (stations.includes(prev)) sSel.value = prev;
}

/* ── Water level chart ───────────────────────────────────────────── */
function buildLevelChart(river, station) {
  const filtered = DATA.water_levels.filter(r =>
    (!river || r.river === river) && (!station || r.name === station)
  );
  const months = unique(filtered, 'month');
  const stations = unique(filtered, 'name');

  const datasets = stations.map(name => {
    const stRows = filtered.filter(r => r.name === name);
    const river_ = stRows[0]?.river || '';
    const byMonth = Object.fromEntries(stRows.map(r => [r.month, r.avg_cm]));
    return {
      label: name,
      data: months.map(m => byMonth[m] ?? null),
      borderColor: riverColor(river_),
      backgroundColor: riverColor(river_) + '22',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3,
      spanGaps: false,
    };
  });

  if (levelChart) levelChart.destroy();
  levelChart = new Chart(document.getElementById('levelChart'), {
    type: 'line',
    data: { labels: months, datasets },
    options: { ...CHART_DEFAULTS }
  });
}

/* ── Discharge chart ─────────────────────────────────────────────── */
function buildDischargeChart(river, station) {
  const filtered = DATA.discharge.filter(r =>
    (!river || r.river === river) && (!station || r.name === station)
  );
  const months = unique(DATA.discharge, 'month');
  const stations = unique(filtered, 'name');

  const datasets = stations.map(name => {
    const stRows = filtered.filter(r => r.name === name);
    const river_ = stRows[0]?.river || '';
    const byMonth = Object.fromEntries(stRows.map(r => [r.month, r.avg_m3s]));
    return {
      label: name,
      data: months.map(m => byMonth[m] ?? null),
      borderColor: riverColor(river_),
      backgroundColor: riverColor(river_) + '22',
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.3,
      spanGaps: false,
    };
  });

  const yLabel = 'm³/s';
  if (dischargeChart) dischargeChart.destroy();
  dischargeChart = new Chart(document.getElementById('dischargeChart'), {
    type: 'line',
    data: { labels: months, datasets },
    options: {
      ...CHART_DEFAULTS,
      scales: {
        ...CHART_DEFAULTS.scales,
        y: { ...CHART_DEFAULTS.scales.y, title: { display: true, text: yLabel, font: { size: 10 } } }
      }
    }
  });
}

/* ── Threshold days chart ────────────────────────────────────────── */
function buildThresholdChart(river, station) {
  const filtered = DATA.threshold_days.filter(r =>
    (!river || r.river === river) && (!station || r.name === station)
  );
  const years = [...new Set(filtered.map(r => r.year))].sort();
  const stations = unique(filtered, 'name');

  const datasets = stations.map(name => {
    const stRows = filtered.filter(r => r.name === name);
    const river_ = stRows[0]?.river || '';
    const byYear = Object.fromEntries(stRows.map(r => [r.year, r.days_above]));
    return {
      label: name,
      data: years.map(y => byYear[y] ?? 0),
      backgroundColor: riverColor(river_) + 'cc',
      borderColor: riverColor(river_),
      borderWidth: 1,
    };
  });

  if (thresholdChart) thresholdChart.destroy();
  thresholdChart = new Chart(document.getElementById('thresholdChart'), {
    type: 'bar',
    data: { labels: years, datasets },
    options: {
      ...CHART_DEFAULTS,
      scales: {
        ...CHART_DEFAULTS.scales,
        y: { ...CHART_DEFAULTS.scales.y, title: { display: true, text: 'days', font: { size: 10 } } }
      }
    }
  });
}

/* ── Station table ───────────────────────────────────────────────── */
let tableSortCol = 2, tableSortAsc = false;
let tableData = [];

function buildTable(river, station) {
  tableData = DATA.station_summary.filter(r =>
    (!river || r.river === river) && (!station || r.name === station)
  );
  renderTable();
}

function renderTable() {
  const sorted = [...tableData].sort((a, b) => {
    const cols = ['name', 'river', 'avg_cm', 'max_cm', 'threshold_cm'];
    const k = cols[tableSortCol];
    const va = a[k] ?? '';
    const vb = b[k] ?? '';
    return tableSortAsc ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
  });

  const tbody = document.getElementById('tableBody');
  tbody.innerHTML = sorted.map(r => `
    <tr>
      <td><span class="river-dot" style="background:${riverColor(r.river)}"></span>${r.name}</td>
      <td>${r.river}</td>
      <td class="num">${fmt(r.avg_cm)}</td>
      <td class="num">${fmt(r.max_cm)}</td>
      <td class="num">${fmt(r.threshold_cm)}</td>
    </tr>
  `).join('');

  document.querySelectorAll('th[data-col]').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    const icon = th.querySelector('.sort-icon');
    icon.textContent = '↕';
  });
  const activeTh = document.querySelector(`th[data-col="${tableSortCol}"]`);
  if (activeTh) {
    activeTh.classList.add(tableSortAsc ? 'sort-asc' : 'sort-desc');
    activeTh.querySelector('.sort-icon').textContent = tableSortAsc ? '↑' : '↓';
  }
}

function sortTable(col) {
  if (tableSortCol === col) tableSortAsc = !tableSortAsc;
  else { tableSortCol = col; tableSortAsc = false; }
  renderTable();
}

/* ── KPI cards ───────────────────────────────────────────────────── */
function fillKPIs() {
  const s = DATA.summary;
  document.getElementById('kpi-stations').textContent = s.total_stations;
  document.getElementById('kpi-rivers').textContent   = s.total_rivers;
  document.getElementById('kpi-meas').textContent     = fmt(s.total_measurements);
  const from = (s.data_from || '').slice(0, 4);
  const to   = (s.data_to   || '').slice(0, 4);
  document.getElementById('kpi-range').textContent    = from + ' – ' + to;
  document.getElementById('builtDate').textContent    = DATA.built;
}

/* ── Filter handler ──────────────────────────────────────────────── */
function applyFilter() {
  const river   = document.getElementById('riverFilter').value;
  const station = document.getElementById('stationFilter').value;
  updateStationDropdown(river);
  if (station && !document.querySelector(`#stationFilter option[value="${station}"]`)) {
    document.getElementById('stationFilter').value = '';
  }
  const st = document.getElementById('stationFilter').value;
  buildLevelChart(river, st);
  buildDischargeChart(river, st);
  buildThresholdChart(river, st);
  buildTable(river, st);
}

/* ── Bootstrap ───────────────────────────────────────────────────── */
fillKPIs();
populateDropdowns();
applyFilter();
</script>
</body>
</html>
"""


# ── Helpers ───────────────────────────────────────────────────────────

def run_query(conn, sql):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]


def serialise(obj):
    """JSON-serialise dates/decimals that psycopg2 returns."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Cannot serialise {type(obj)}")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("build_dashboard_data.py — Dutch River Monitor")
    print("=" * 60)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        print("Querying water levels…")
        water_levels = run_query(conn, SQL_WATER_LEVELS)

        print("Querying discharge (Q)…")
        discharge = run_query(conn, SQL_DISCHARGE)

        print("Querying threshold days…")
        threshold_days = run_query(conn, SQL_THRESHOLD_DAYS)

        print("Querying summary stats…")
        summary_rows = run_query(conn, SQL_SUMMARY)
        summary = summary_rows[0] if summary_rows else {}

        print("Querying station summary…")
        station_summary = run_query(conn, SQL_STATION_SUMMARY)
    finally:
        conn.close()

    data = {
        "summary":         summary,
        "water_levels":    water_levels,
        "discharge":       discharge,
        "threshold_days":  threshold_days,
        "station_summary": station_summary,
        "built":           date.today().isoformat(),
    }

    # Size report
    for key in ("water_levels", "discharge", "threshold_days", "station_summary"):
        size_kb = len(json.dumps(data[key], default=serialise)) / 1024
        flag = " ⚠ LARGE" if size_kb > 80 else ""
        print(f"  {key:<20} {size_kb:6.1f} KB  ({len(data[key])} rows){flag}")

    json_str = json.dumps(data, default=serialise, ensure_ascii=False, separators=(",", ":"))
    total_kb = len(json_str) / 1024
    print(f"  {'TOTAL JSON':<20} {total_kb:6.1f} KB")
    if total_kb > 100:
        print("  ⚠  JSON > 100 KB — consider reducing granularity")

    html = HTML_BEFORE_DATA + f"const DATA = {json_str};" + HTML_AFTER_DATA

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"\n✓ Written {len(html) / 1024:.1f} KB → {OUTPUT_PATH}")
    print("\nNext: open docs/index.html in a browser to verify.")


if __name__ == "__main__":
    main()
