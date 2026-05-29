"""
config.py
---------
Central configuration for the pipeline.
Change values here rather than touching pipeline logic.
"""

# ── Cities to fetch ───────────────────────────────────────────────────────────
# Open-Meteo resolves city names to lat/lon via the Geocoding API.
CITIES = [
    "Chennai",
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
]

# ── Date range ────────────────────────────────────────────────────────────────
# How many past days to fetch when no --days flag is provided.
DEFAULT_DAYS_BACK = 7

# ── Open-Meteo endpoints ──────────────────────────────────────────────────────
GEOCODING_API_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_API_URL   = "https://api.open-meteo.com/v1/forecast"

# ── Variables to pull from the API ───────────────────────────────────────────
# Full list: https://open-meteo.com/en/docs
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "windspeed_10m",
    "weathercode",
    "apparent_temperature",   # "feels like" temperature
]

# ── BigQuery ──────────────────────────────────────────────────────────────────
# Replace with your actual GCP project ID (found in BigQuery console top-left).
BIGQUERY_PROJECT_ID = "your-gcp-project-id"   # ← CHANGE THIS
BIGQUERY_DATASET_ID = "weather_pipeline"
BIGQUERY_TABLE_ID   = "hourly_observations"

# ── API / network settings ────────────────────────────────────────────────────
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES             = 3
RETRY_BACKOFF_SECONDS   = 5