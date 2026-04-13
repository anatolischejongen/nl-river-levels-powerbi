import requests
from datetime import datetime, timedelta

STATION_CODE = 'lobith.ponton'   # ← Adım A'dan gelen gerçek değerle değiştir

url = "https://ddapi20-waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"

# Son 1 gün
end = datetime.now()
start = end - timedelta(days=30)
fmt = "%Y-%m-%dT%H:%M:%S.000+01:00"

body = {
    "Locatie": {
        "Code": STATION_CODE          # SADECE Code, X/Y yok
    },
    "AquoPlusWaarnemingMetadata": {
        "AquoMetadata": {
            "Compartiment": {"Code": "OW"},
            "Grootheid":    {"Code": "WATHTE"}
        }
    },
    "Periode": {
        "Begindatumtijd": start.strftime(fmt),
        "Einddatumtijd":  end.strftime(fmt)
    }
}

response = requests.post(url, json=body, timeout=30)
print("Status:", response.status_code)
print("First 1500 chars:")
print(response.text[:1500])