"""
fetch_lobith_real.py — Demo script: fetches Lobith Tolkamer's last 7 days.

This is a sanity-check script that exercises the rws_api module against
a single, well-known station. If this script runs cleanly, the module is
working correctly.
"""

from rws_api import fetch_station_data, parse_response_to_rows, get_default_time_range


STATION_CODE = "lobith.bovenrijn.tolkamer"


def main():
    print(f"Fetching last 7 days for: {STATION_CODE}")
    
    start_time, end_time = get_default_time_range(days=7)
    print(f"Period: {start_time.isoformat()} → {end_time.isoformat()}")
    print()
    
    response_json = fetch_station_data(STATION_CODE, start_time, end_time)
    rows = parse_response_to_rows(response_json, STATION_CODE)
    
    # Quick stats
    meting_count = sum(1 for r in rows if r["proces_type"] == "meting")
    verwachting_count = sum(1 for r in rows if r["proces_type"] == "verwachting")
    
    print(f"Total rows: {len(rows)}")
    print(f"  meting:      {meting_count}")
    print(f"  verwachting: {verwachting_count}")
    print()
    
    if rows:
        print("First row:")
        print(f"  {rows[0]}")
        print("Last row:")
        print(f"  {rows[-1]}")


if __name__ == "__main__":
    main()


# import requests
# from datetime import datetime, timedelta, timezone
# import json

# # This script tests the Rijkswaterstaat OphalenWaarnemingen API for a single station (Lobith)
# def parse_response_to_rows(response_json, station_code):
#     """
#     Rijkswaterstaat OphalenWaarnemingen JSON yanıtını alıp
#     düz satırların listesine çevirir.
    
#     Her satır bir sözlüktür, Postgres'e yazılmaya hazırdır.
#     """
#     rows = []
    
#     waarnemingen_lijst = response_json.get("WaarnemingenLijst", [])
    
#     for entry in waarnemingen_lijst:
#         proces_type = entry.get("AquoMetadata", {}).get("ProcesType", "unknown")
#         metingen_lijst = entry.get("MetingenLijst", [])
        
#         for meting in metingen_lijst:
#             value = meting.get("Meetwaarde", {}).get("Waarde_Numeriek")
#             timestamp = meting.get("Tijdstip")
#             quality_code = meting.get("WaarnemingMetadata", {}).get("Kwaliteitswaardecode")
#             status = meting.get("WaarnemingMetadata", {}).get("Statuswaarde")
            
#             row = {
#                 "station_code": station_code,
#                 "timestamp": timestamp,
#                 "value_cm": value,
#                 "proces_type": proces_type,
#                 "quality_code": quality_code,
#                 "status": status,
#             }
#             rows.append(row)
    
#     return rows

# # === TEST: Let’s retrieve the data for the Lobith station from the API for the last 7 days ===
# URL = "https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"
# STATION_CODE = "lobith.bovenrijn.tolkamer"

# end_time = datetime.now(timezone.utc)
# start_time = end_time - timedelta(days=7)

# TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000+00:00"

# begin_str = start_time.strftime(TIME_FORMAT)
# end_str = end_time.strftime(TIME_FORMAT)

# # JSON body to be sent to the API
# body = {
#     "Locatie": {"Code": STATION_CODE},
#     "AquoPlusWaarnemingMetadata": {
#         "AquoMetadata": {
#             "Compartiment": {"Code": "OW"},
#             "Grootheid": {"Code": "WATHTE"},
#         }
#     },
#     "Periode": {
#         "Begindatumtijd": begin_str,
#         "Einddatumtijd": end_str,
#     },
# }

# print(f"Requesting station: {STATION_CODE}")
# print(f"Period: {begin_str} → {end_str}")
# print(f"Endpoint: {URL}")
# print()

# # Send the POST request to the API
# response = requests.post(URL, json=body, timeout=30)
# print(f"Status code: {response.status_code}")
# print()
# # If the request was successful, 
# # parse the JSON response and print some details
# if response.status_code == 200:
#     data = response.json()

#     print("Top-level keys in response:")
#     print(list(data.keys()))
#     print()

#     print(f"Succesvol field: {data.get('Succesvol')}")
#     print()
#     # Check if WaarnemingenLijst is present 
#     # and print some details about it
#     if "WaarnemingenLijst" in data:
#         waarnemingen = data["WaarnemingenLijst"]
#         print(f"Number of WaarnemingenLijst entries: {len(waarnemingen)}")
#         # If there are entries, 
#         # print the keys of the first one 
#         # and some details about its measurements
#         if waarnemingen:
#             first = waarnemingen[0]
#             print(f"\nFirst entry's top-level keys: {list(first.keys())}")
#             # Check if MetingenLijst is present 
#             # in the first entry and print details
#             if "MetingenLijst" in first:
#                 metingen = first["MetingenLijst"]
#                 print(f"Number of measurements in first entry: {len(metingen)}")
#                 print("\nFirst 3 measurements:")
#                 # Print the first and last 3 measurements in a pretty format
#                 for m in metingen[:3]:
#                     print(json.dumps(m, indent=2))

#                 print("\nLast 3 measurements:")
#                 for m in metingen[-3:]:
#                     print(json.dumps(m, indent=2))

#             # === RESEARCH NOTE: Why does WaarnemingenLijst contain 2 entries? ===
#             # Initial tests revealed that the API returns 2 results even when querying 
#             # a single station. By comparing the AquoMetadata of both entries, we 
#             # found the reason:
#             # - First entry:  ProcesType="meting"     (Actual sensor measurement)
#             # - Second entry: ProcesType="verwachting" (Model forecast/prediction)
#             # Rijkswaterstaat maintains two parallel streams for the same time series: 
#             # real-time measurements and past forecasts—the latter is used to 
#             # validate the model. The parser captures both series.
#             # if len(waarnemingen) > 1:
#             #     second = waarnemingen[1]
#             #     print("\n=== SECOND ENTRY ===")
#             #     print(f"Top-level keys: {list(second.keys())}")
#             #     print("\nAquoMetadata of second entry:")
#             #     print(json.dumps(second.get("AquoMetadata", {}), indent=2))
#             #     print(f"\nNumber of measurements in second entry: {len(second.get('MetingenLijst', []))}")
#             #     if second.get("MetingenLijst"):
#             #         print("\nFirst measurement of second entry:")
#             #         print(json.dumps(second["MetingenLijst"][0], indent=2))
# # === TEST: parser fonksiyonunu çalıştır ===
# # if response.status_code == 200:
# #     parsed_rows = parse_response_to_rows(response.json(), STATION_CODE)
    
# #     print("\n" + "=" * 60)
# #     print("PARSER TEST")
# #     print("=" * 60)
# #     print(f"Toplam satır sayısı: {len(parsed_rows)}")
    
# #     # proces_type'a göre sayalım
# #     meting_count = sum(1 for r in parsed_rows if r["proces_type"] == "meting")
# #     verwachting_count = sum(1 for r in parsed_rows if r["proces_type"] == "verwachting")
# #     unknown_count = sum(1 for r in parsed_rows if r["proces_type"] == "unknown")
    
# #     print(f"  meting:      {meting_count}")
# #     print(f"  verwachting: {verwachting_count}")
# #     print(f"  unknown:     {unknown_count}")
    
# #     print("\nİlk 3 satır:")
# #     for r in parsed_rows[:3]:
# #         print(r)
    
# #     print("\nSon 3 satır:")
# #     for r in parsed_rows[-3:]:
# #         print(r)
