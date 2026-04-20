"""
rws_api.py — Rijkswaterstaat WaterWebservices API client.

Reusable functions for fetching and parsing measurement data
from Rijkswaterstaat's OphalenWaarnemingen endpoint.

Supports any Grootheid code (WATHTE, DEBIET, STROOMV, …) via the
`grootheid` parameter. All functions default to "WATHTE" so existing
callers continue to work without changes.

This module is imported by other scripts; it is not meant to be run directly.
"""

import requests
from datetime import datetime, timedelta, UTC


# API endpoint - new Wadar service, live since December 2025
API_URL = (
    "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
    "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"
)

# Time format expected by the API (ISO 8601 with milliseconds and timezone)
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000+00:00"

# Default request timeout in seconds
DEFAULT_TIMEOUT = 30


def build_request_body(station_code, start_time, end_time, grootheid="WATHTE"):
    """
    Build the POST request body for OphalenWaarnemingen.

    Parameters
    ----------
    station_code : str
        Rijkswaterstaat station code (e.g. "lobith.bovenrijn.tolkamer")
    start_time : datetime
        Start of the requested period (timezone-aware UTC)
    end_time : datetime
        End of the requested period (timezone-aware UTC)
    grootheid : str, optional
        Aquo grootheid code to request (default: "WATHTE").
        Other valid values: "DEBIET", "STROOMV".

    Returns
    -------
    dict
        Request body ready to be passed to `requests.post(json=...)`
    """
    return {
        "Locatie": {
            "Code": station_code
        },
        "AquoPlusWaarnemingMetadata": {
            "AquoMetadata": {
                "Compartiment": {"Code": "OW"},
                "Grootheid": {"Code": grootheid}
            }
        },
        "Periode": {
            "Begindatumtijd": start_time.strftime(TIME_FORMAT),
            "Einddatumtijd": end_time.strftime(TIME_FORMAT)
        }
    }


def fetch_station_data(station_code, start_time, end_time,
                       grootheid="WATHTE", timeout=DEFAULT_TIMEOUT):
    """
    Fetch measurement data for a single station from Rijkswaterstaat.

    Parameters
    ----------
    station_code : str
        Rijkswaterstaat station code (e.g. "lobith.bovenrijn.tolkamer")
    start_time : datetime
        Start of the requested period (timezone-aware UTC)
    end_time : datetime
        End of the requested period (timezone-aware UTC)
    grootheid : str, optional
        Aquo grootheid code to request (default: "WATHTE").
        Other valid values: "DEBIET", "STROOMV".
    timeout : int, optional
        Request timeout in seconds (default: 30)

    Returns
    -------
    dict
        Parsed JSON response from the API.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-success status code.
    requests.RequestException
        For network errors, timeouts, etc.
    """
    body = build_request_body(station_code, start_time, end_time, grootheid)

    response = requests.post(API_URL, json=body, timeout=timeout)
    response.raise_for_status()

    return response.json()


def parse_response_to_rows(response_json, station_code, grootheid="WATHTE",
                           accepted_hoedanigheid=("NAP",)):
    """
    Convert Rijkswaterstaat OphalenWaarnemingen JSON response to flat rows.

    Walks the nested response structure (WaarnemingenLijst → MetingenLijst)
    and produces one row per measurement. Handles both 'meting' (real sensor
    readings) and 'verwachting' (model forecasts) entries when present.

    By default, only entries with Hoedanigheid "NAP" are accepted. This
    excludes Belgian TAW readings that Rijkswaterstaat publishes for border
    stations like Borgharen — these are valid but belong to a different
    national reference system.

    Note on `grootheid` vs `accepted_hoedanigheid`: grootheid identifies the
    physical quantity being measured (water level, discharge, flow velocity).
    Hoedanigheid identifies the reference datum or unit convention (NAP, TAW).
    For DEBIET and STROOMV the hoedanigheid filter is typically not relevant,
    but is kept consistent with the WATHTE implementation.

    Parameters
    ----------
    response_json : dict
        Parsed JSON response from `fetch_station_data()`
    station_code : str
        Station code to attach to each row (provided externally to ensure
        consistent labeling across the dataset)
    grootheid : str, optional
        Grootheid code to tag on each output row (default: "WATHTE").
        Must match what was requested in `fetch_station_data()`.
    accepted_hoedanigheid : tuple of str, optional
        Which Hoedanigheid (reference datum) codes to accept.
        Default: ("NAP",) — only Dutch Normaal Amsterdams Peil.
        Pass ("NAP", "TAW") to include Belgian TAW readings too.

    Returns
    -------
    list of dict
        Each row contains: station_code, timestamp, value_cm, grootheid,
        proces_type, hoedanigheid, quality_code, status.
        Empty list if no data was found.
    """
    rows = []

    waarnemingen_lijst = response_json.get("WaarnemingenLijst", [])

    for entry in waarnemingen_lijst:
        aquo = entry.get("AquoMetadata", {})
        proces_type = aquo.get("ProcesType", "meting")
        hoedanigheid = aquo.get("Hoedanigheid", {}).get("Code", "?")

        # Skip entries with unwanted reference datum (e.g. Belgian TAW)
        if hoedanigheid not in accepted_hoedanigheid:
            continue

        metingen_lijst = entry.get("MetingenLijst", [])

        for meting in metingen_lijst:
            value = meting.get("Meetwaarde", {}).get("Waarde_Numeriek")
            timestamp = meting.get("Tijdstip")
            quality_code = meting.get("WaarnemingMetadata", {}).get("Kwaliteitswaardecode")
            status = meting.get("WaarnemingMetadata", {}).get("Statuswaarde")

            row = {
                "station_code": station_code,
                "timestamp":    timestamp,
                "value_cm":     value,
                "grootheid":    grootheid,
                "proces_type":  proces_type,
                "hoedanigheid": hoedanigheid,
                "quality_code": quality_code,
                "status":       status,
            }
            rows.append(row)

    return rows


def get_default_time_range(days=7):
    """
    Return a default UTC time range ending now.

    Convenience helper for callers that just want "the last N days".

    Parameters
    ----------
    days : int, optional
        Number of days back from now (default: 7)

    Returns
    -------
    tuple of (datetime, datetime)
        (start_time, end_time) both timezone-aware UTC datetimes.
    """
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=days)
    return start_time, end_time
