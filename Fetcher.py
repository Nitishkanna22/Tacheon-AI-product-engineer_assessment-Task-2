"""
fetcher.py
----------
Responsible ONLY for talking to the Open-Meteo API.
Returns a list of raw record dicts; no transformation happens here.
"""

import logging
import time
from datetime import date
from typing import Any

import requests

from config import (
    GEOCODING_API_URL,
    WEATHER_API_URL,
    HOURLY_VARIABLES,
    REQUEST_TIMEOUT_SECONDS,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
)

logger = logging.getLogger(__name__)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _get_with_retry(url: str, params: dict) -> dict | None:
    """
    GET request with exponential-style retry on transient failures.
    Returns parsed JSON dict on success, None on permanent failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug(f"GET {url}  attempt {attempt}/{MAX_RETRIES}")
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "unknown"
            # 4xx errors are client errors — retrying won't help
            if e.response is not None and 400 <= e.response.status_code < 500:
                logger.error(f"HTTP {status} — client error, not retrying: {e}")
                return None
            logger.warning(f"HTTP {status} on attempt {attempt}: {e}")

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on attempt {attempt}: {e}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt} (limit: {REQUEST_TIMEOUT_SECONDS}s)")

        except requests.exceptions.RequestException as e:
            logger.error(f"Unexpected request error: {e}")
            return None

        if attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.info(f"Retrying in {wait}s …")
            time.sleep(wait)

    logger.error(f"All {MAX_RETRIES} attempts failed for {url}")
    return None


def _geocode_city(city_name: str) -> dict | None:
    """
    Resolve a city name to latitude, longitude, and country via Open-Meteo
    Geocoding API.  Returns a dict with keys: city, latitude, longitude, country.
    """
    params = {"name": city_name, "count": 1, "language": "en", "format": "json"}
    data = _get_with_retry(GEOCODING_API_URL, params)

    if not data or "results" not in data or not data["results"]:
        logger.error(f"Could not geocode city: '{city_name}'")
        return None

    result = data["results"][0]
    logger.info(
        f"Geocoded '{city_name}' → "
        f"lat={result['latitude']:.4f}, lon={result['longitude']:.4f}, "
        f"country={result.get('country', 'unknown')}"
    )
    return {
        "city":      result["name"],
        "latitude":  result["latitude"],
        "longitude": result["longitude"],
        "country":   result.get("country", "unknown"),
    }


def _fetch_hourly_weather(
    city_meta: dict,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch hourly weather for one city and return a flat list of record dicts
    (one dict per hour).
    """
    params = {
        "latitude":       city_meta["latitude"],
        "longitude":      city_meta["longitude"],
        "hourly":         ",".join(HOURLY_VARIABLES),
        "start_date":     start_date.isoformat(),
        "end_date":       end_date.isoformat(),
        "timezone":       "auto",              # returns local time for the location
        "timeformat":     "iso8601",
    }

    data = _get_with_retry(WEATHER_API_URL, params)
    if not data or "hourly" not in data:
        logger.error(f"No hourly data returned for {city_meta['city']}")
        return []

    hourly = data["hourly"]
    timestamps = hourly.get("time", [])

    if not timestamps:
        logger.warning(f"Empty time series for {city_meta['city']}")
        return []

    records = []
    for i, ts in enumerate(timestamps):
        record: dict[str, Any] = {
            "city":      city_meta["city"],
            "country":   city_meta["country"],
            "latitude":  city_meta["latitude"],
            "longitude": city_meta["longitude"],
            "timestamp": ts,
        }
        # Pull each requested variable; use None if missing/short list
        for var in HOURLY_VARIABLES:
            values = hourly.get(var, [])
            record[var] = values[i] if i < len(values) else None

        records.append(record)

    logger.info(f"  {city_meta['city']}: {len(records)} hourly records fetched")
    return records


# ── Public interface ──────────────────────────────────────────────────────────

def fetch_weather_for_cities(
    city_names: list[str],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """
    Geocode each city, then fetch hourly weather.
    Returns all records across all cities as a flat list.
    Skips cities that fail geocoding; continues with remaining cities.
    """
    all_records: list[dict[str, Any]] = []

    for city_name in city_names:
        logger.info(f"Processing city: {city_name}")
        city_meta = _geocode_city(city_name)
        if city_meta is None:
            logger.warning(f"Skipping '{city_name}' due to geocoding failure")
            continue

        records = _fetch_hourly_weather(city_meta, start_date, end_date)
        all_records.extend(records)

    return all_records