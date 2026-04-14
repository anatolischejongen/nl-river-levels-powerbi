"""
inspect_anomalies.py — Investigate unusual row counts from stations.

After fetch_all_stations.py, three stations showed unexpected row counts:
- Arnhem:    974 rows (about half of the usual ~2000)
- Borgharen: 3011 rows (about 1000 more than usual)
- Roermond:  1998 rows (about 13 fewer than usual)

This script re-fetches these three stations and prints the structure
of WaarnemingenLijst — specifically how many entries exist and what
ProcesType each one has — to explain the discrepancies.

This is a one-off diagnostic script. Once findings are documented in
lessons-learned.md, it can be kept as a reference or deleted.
"""

from rws_api import fetch_station_data, get_default_time_range


def inspect_station(station_code):
    """
    Fetch a station and print the structure of WaarnemingenLijst.

    Shows how many entries the station returns and what ProcesType
    each entry has, along with the number of measurements per entry.

    Parameters
    ----------
    station_code : str
        Rijkswaterstaat station code
    """
    print(f"\n{'=' * 60}")
    print(f"STATION: {station_code}")
    print(f"{'=' * 60}")
    
    start_time, end_time = get_default_time_range(days=7)
    
    try:
        response_json = fetch_station_data(station_code, start_time, end_time)
    except Exception as e:
        print(f"  ❌ Fetch failed: {type(e).__name__}: {e}")
        return
    
    waarnemingen = response_json.get("WaarnemingenLijst", [])
    print(f"WaarnemingenLijst entries: {len(waarnemingen)}")
    print()
    
    total_measurements = 0
    
    for i, entry in enumerate(waarnemingen):
        aquo = entry.get("AquoMetadata", {})
        proces_type = aquo.get("ProcesType", "(field not present)")
        
        metingen = entry.get("MetingenLijst", [])
        count = len(metingen)
        total_measurements += count
        
        hoedanigheid = aquo.get("Hoedanigheid", {}).get("Code", "?")
        grootheid = aquo.get("Grootheid", {}).get("Code", "?")
        
        print(f"  Entry {i}:")
        print(f"    ProcesType:   {proces_type}")
        print(f"    Grootheid:    {grootheid}")
        print(f"    Hoedanigheid: {hoedanigheid}")
        print(f"    Measurements: {count:,}")
        print()
    
    print(f"TOTAL measurements across all entries: {total_measurements:,}")


def main():
    print("Inspecting stations with anomalous row counts...")
    print("(all three were previously fetched via fetch_all_stations.py)")
    
    stations_to_inspect = [
        "arnhem.nederrijn",
        "maastricht.borgharen.maas.beneden",
        "roermond.boven",
    ]
    
    for station_code in stations_to_inspect:
        inspect_station(station_code)
    
    print()
    print("=" * 60)
    print("Done. Compare entry counts with fetch_all_stations.py output.")
    print("=" * 60)


if __name__ == "__main__":
    main()