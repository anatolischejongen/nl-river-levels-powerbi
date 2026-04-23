import requests
import json
from datetime import datetime, timedelta

def test_api_single_day():
    #find yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    print(f"Testing API for date: {date_str}")  
    print("Target station: Lobith(LOBI)")

    # API endpoint
    url = "https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"

    # Parameters for the API request
    params = {
        "station": "LOBI",  # Lobith station
        "date": date_str
    }

    try:
        print(f"\nRequesting API endpoint: {url}")
        body = {
            "Locatie": {
                "X": 713748.4,        # Lobith RD koordinatı (yaklaşık)
                "Y": 5755381.4,
                "Code": "LOBI"
            },
            "AquoPlusWaarnemingMetadata": {
                "AquoMetadata": {
                    "Compartiment": {"Code": "OW"},        # Oppervlaktewater
                    "Grootheid": {"Code": "WATHTE"}        # Waterhoogte
                }
            },
            "Periode": {
                "Begindatumtijd": "2026-04-09T00:00:00.000+01:00",
                "Einddatumtijd":  "2026-04-10T00:00:00.000+01:00"
            }
        }

        response = requests.post(url, params=params, timeout=30)

        print(f"Received response with status code: {response.status_code}")
        
        if response.status_code == 200:
            print("API request successful. Processing data...")
            data = response.json()  # Parse the JSON response
            print("API response data:")
            print(json.dumps(data, indent=4)) # Pretty-print the JSON data

        else: 
            print(f"API request failed with status code: {response.status_code}")
            print(f"Response content: {response.text}")

    except Exception as e:
        print(f"An error occurred while making the API request: {e}")   

if __name__ == "__main__":
    test_api_single_day()
                
                
                
                
                
                



