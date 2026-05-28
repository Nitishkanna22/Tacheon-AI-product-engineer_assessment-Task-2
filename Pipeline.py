"""
pipeline.py
-----------
Open-Meteo Weather Data Pipeline
Fetches hourly weather data for one or more cities, transforms it into an
analytics-ready tabular format, and loads it into BigQuery.

Usage:
    python pipeline.py --cities "Chennai,Mumbai,Delhi" --days 7
    python pipeline.py  # uses defaults from config.py
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

from config import (
    CITIES,
    DEFAULT_DAYS_BACK,
    BIGQUERY_PROJECT_ID,
    BIGQUERY_DATASET_ID,
    BIGQUERY_TABLE_ID,
)
from fetcher import fetch_weather_for_cities
from transformer import transform_weather_data
from loader import load_to_bigquery

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open-Meteo → BigQuery pipeline")
    parser.add_argument(
        "--cities",
        type=str,
        default=None,
        help="Comma-separated city names (overrides config.py). E.g. 'Chennai,Mumbai'",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS_BACK,
        help=f"Number of past days to fetch (default: {DEFAULT_DAYS_BACK})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and transform only; skip BigQuery load. Saves output to output/preview.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    city_list = (
        [c.strip() for c in args.cities.split(",")]
        if args.cities
        else CITIES
    )

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=args.days)

    logger.info("=" * 60)
    logger.info("Pipeline started")
    logger.info(f"Cities    : {city_list}")
    logger.info(f"Date range: {start_date} → {end_date}")
    logger.info(f"Dry run   : {args.dry_run}")
    logger.info("=" * 60)

    # ── Step 1: Fetch ─────────────────────────────────────────────────────────
    logger.info("STEP 1 — Fetching data from Open-Meteo API")
    raw_records = fetch_weather_for_cities(city_list, start_date, end_date)
    if not raw_records:
        logger.error("No data fetched. Aborting pipeline.")
        sys.exit(1)
    logger.info(f"Fetched {len(raw_records)} raw hourly records across {len(city_list)} cities")

    # ── Step 2: Transform ─────────────────────────────────────────────────────
    logger.info("STEP 2 — Transforming and enriching data")
    df: pd.DataFrame = transform_weather_data(raw_records)
    logger.info(f"Transformed dataset: {len(df)} rows × {len(df.columns)} columns")

    if args.dry_run:
        out_path = "output/preview.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"Dry run — data saved to {out_path}. Skipping BigQuery load.")
        logger.info("Pipeline finished (dry run).")
        return

    # ── Step 3: Load ──────────────────────────────────────────────────────────
    logger.info("STEP 3 — Loading data into BigQuery")
    load_to_bigquery(
        df=df,
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        table_id=BIGQUERY_TABLE_ID,
    )

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()