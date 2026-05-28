"""
transformer.py
--------------
Takes raw record dicts from fetcher.py and returns a clean,
analytics-ready pandas DataFrame.

Derived fields added here (none of these come from the API directly):
  - feels_like_delta     : difference between apparent and actual temperature
  - heat_index_category  : human-readable comfort label based on apparent temp
  - is_rainy_hour        : boolean flag for precipitation > 0
  - wind_category        : Beaufort-style wind label
  - hour_of_day          : integer 0–23, useful for time-of-day aggregations
  - date                 : date part only, for daily roll-ups
  - ingested_at          : UTC timestamp when this pipeline run processed the row
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── WMO Weather Interpretation Codes → human-readable label ──────────────────
# Source: https://open-meteo.com/en/docs#weathervariables
WMO_CODE_MAP: dict[int, str] = {
    0:  "Clear sky",
    1:  "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    """Convert a value to float, returning default on failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _heat_index_category(apparent_temp: float | None) -> str:
    """
    Bucket apparent temperature into a comfort category.
    Thresholds are approximate and India-climate-aware.
    """
    if apparent_temp is None:
        return "unknown"
    if apparent_temp >= 54:
        return "extreme danger"
    if apparent_temp >= 41:
        return "danger"
    if apparent_temp >= 32:
        return "extreme caution"
    if apparent_temp >= 27:
        return "caution"
    if apparent_temp >= 10:
        return "comfortable"
    return "cold"


def _wind_category(windspeed_kmh: float | None) -> str:
    """Simplified Beaufort wind scale buckets."""
    if windspeed_kmh is None:
        return "unknown"
    if windspeed_kmh < 1:
        return "calm"
    if windspeed_kmh < 20:
        return "light breeze"
    if windspeed_kmh < 40:
        return "moderate breeze"
    if windspeed_kmh < 62:
        return "strong breeze"
    if windspeed_kmh < 89:
        return "near gale"
    return "storm"


def transform_weather_data(raw_records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Full transformation pipeline:
      1. Build DataFrame from raw records
      2. Cast / coerce types
      3. Handle nulls
      4. Add derived / enrichment columns
      5. Rename columns to BigQuery-friendly names
      6. Reorder and return
    """
    if not raw_records:
        logger.warning("transform_weather_data received empty record list")
        return pd.DataFrame()

    logger.info(f"Starting transformation on {len(raw_records)} records")

    df = pd.DataFrame(raw_records)

    # ── 1. Parse timestamps ───────────────────────────────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
    invalid_ts = df["timestamp"].isna().sum()
    if invalid_ts > 0:
        logger.warning(f"Dropped {invalid_ts} rows with unparseable timestamps")
        df = df.dropna(subset=["timestamp"])

    # ── 2. Coerce numeric columns ─────────────────────────────────────────────
    numeric_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "windspeed_10m",
        "apparent_temperature",
        "weathercode",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── 3. Fill / handle nulls ────────────────────────────────────────────────
    # Precipitation: null almost always means 0 (no rain measured)
    df["precipitation"] = df["precipitation"].fillna(0.0)

    null_summary = df[numeric_cols].isna().sum()
    nulls_present = null_summary[null_summary > 0]
    if not nulls_present.empty:
        logger.warning(f"Null values remaining after fill:\n{nulls_present.to_string()}")

    # ── 4. Derived fields ─────────────────────────────────────────────────────

    # 4a. Difference between "feels like" and actual temperature
    df["feels_like_delta"] = (
        df["apparent_temperature"] - df["temperature_2m"]
    ).round(2)

    # 4b. Human-readable heat index / comfort category
    df["heat_index_category"] = df["apparent_temperature"].apply(_heat_index_category)

    # 4c. Boolean rain flag
    df["is_rainy_hour"] = df["precipitation"] > 0

    # 4d. Wind category
    df["wind_category"] = df["windspeed_10m"].apply(_wind_category)

    # 4e. WMO weather code → descriptive label
    df["weathercode"] = df["weathercode"].fillna(-1).astype(int)
    df["weather_description"] = df["weathercode"].map(WMO_CODE_MAP).fillna("unknown")

    # 4f. Time parts — useful for time-of-day and daily aggregations in SQL
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["date"]        = df["timestamp"].dt.date

    # 4g. Pipeline metadata
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    # ── 5. Rename to clean BigQuery column names ──────────────────────────────
    df = df.rename(columns={
        "temperature_2m":       "temperature_c",
        "relative_humidity_2m": "relative_humidity_pct",
        "windspeed_10m":        "windspeed_kmh",
        "apparent_temperature": "apparent_temperature_c",
        "weathercode":          "wmo_weather_code",
    })

    # ── 6. Final column order ─────────────────────────────────────────────────
    ordered_cols = [
        # Identifiers
        "city", "country", "latitude", "longitude",
        # Time
        "timestamp", "date", "hour_of_day",
        # Core weather
        "temperature_c", "apparent_temperature_c", "feels_like_delta",
        "relative_humidity_pct", "precipitation",
        "windspeed_kmh",
        # Categorical / derived
        "wmo_weather_code", "weather_description",
        "heat_index_category", "wind_category", "is_rainy_hour",
        # Metadata
        "ingested_at",
    ]
    # Only include columns that actually exist (guards against API changes)
    final_cols = [c for c in ordered_cols if c in df.columns]
    df = df[final_cols]

    logger.info(
        f"Transformation complete: {len(df)} rows, columns: {list(df.columns)}"
    )
    return df