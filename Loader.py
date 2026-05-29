"""
loader.py
---------
Handles writing the transformed DataFrame to BigQuery.

Design decisions:
  - Uses WRITE_APPEND so each pipeline run adds new records (idempotency is
    handled by the SQL query layer or a dedup view, not here).
  - Schema is defined explicitly — never inferred — so field types and
    descriptions are consistent regardless of what data arrives.
  - Dataset is auto-created if it doesn't exist (first-run friendly).
"""

import logging

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig, WriteDisposition

logger = logging.getLogger(__name__)

# ── Explicit BigQuery schema ──────────────────────────────────────────────────
# Defining schema explicitly (not auto-detect) ensures:
#   - Correct types even when a column is all-null in a small batch
#   - Stable column descriptions for downstream users
#   - Predictable behaviour if the API changes a field type

BQ_SCHEMA: list[SchemaField] = [
    SchemaField("city",                   "STRING",    description="City name from geocoding"),
    SchemaField("country",                "STRING",    description="Country name"),
    SchemaField("latitude",               "FLOAT64",   description="City latitude"),
    SchemaField("longitude",              "FLOAT64",   description="City longitude"),
    SchemaField("timestamp",              "TIMESTAMP", description="Observation timestamp (local time)"),
    SchemaField("date",                   "DATE",      description="Date part of observation"),
    SchemaField("hour_of_day",            "INT64",     description="Hour 0–23 for time-of-day aggregations"),
    SchemaField("temperature_c",          "FLOAT64",   description="Air temperature at 2m (°C)"),
    SchemaField("apparent_temperature_c", "FLOAT64",   description="Feels-like temperature (°C)"),
    SchemaField("feels_like_delta",       "FLOAT64",   description="apparent_temperature_c minus temperature_c"),
    SchemaField("relative_humidity_pct",  "FLOAT64",   description="Relative humidity at 2m (%)"),
    SchemaField("precipitation",          "FLOAT64",   description="Precipitation in mm (0 if none)"),
    SchemaField("windspeed_kmh",          "FLOAT64",   description="Wind speed at 10m (km/h)"),
    SchemaField("wmo_weather_code",       "INT64",     description="WMO weather interpretation code"),
    SchemaField("weather_description",    "STRING",    description="Human-readable WMO code label"),
    SchemaField("heat_index_category",    "STRING",    description="Comfort bucket: comfortable / caution / danger etc."),
    SchemaField("wind_category",          "STRING",    description="Beaufort-style wind label"),
    SchemaField("is_rainy_hour",          "BOOL",      description="True if precipitation > 0"),
    SchemaField("ingested_at",            "TIMESTAMP", description="UTC timestamp when this pipeline run ran"),
]


def _ensure_dataset_exists(client: bigquery.Client, dataset_ref: bigquery.DatasetReference) -> None:
    """Create the dataset if it doesn't already exist."""
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_ref.dataset_id}' already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # BigQuery Sandbox default; change if needed
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Created dataset '{dataset_ref.dataset_id}'")


def _prepare_df_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final type coercions before upload.
    BigQuery's Python client is strict about types matching the schema.
    """
    df = df.copy()

    # Ensure timestamp columns are proper datetime objects (not strings)
    for col in ["timestamp", "ingested_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Boolean must be Python bool, not numpy bool_
    if "is_rainy_hour" in df.columns:
        df["is_rainy_hour"] = df["is_rainy_hour"].astype(bool)

    return df


def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> None:
    """
    Load a DataFrame into BigQuery using WRITE_APPEND.
    Raises on failure so the caller (pipeline.py) can handle / log it.
    """
    if df.empty:
        logger.warning("DataFrame is empty — nothing to load into BigQuery")
        return

    logger.info(f"Connecting to BigQuery project: {project_id}")
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        logger.error(
            f"Failed to initialise BigQuery client. "
            f"Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly.\nError: {e}"
        )
        raise

    dataset_ref = client.dataset(dataset_id)
    _ensure_dataset_exists(client, dataset_ref)

    table_ref = dataset_ref.table(table_id)
    full_table = f"{project_id}.{dataset_id}.{table_id}"

    df = _prepare_df_for_bq(df)

    job_config = LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=WriteDisposition.WRITE_APPEND,
        # Sandbox note: streaming inserts are not available in sandbox,
        # but load jobs (used here) work fine.
    )

    logger.info(f"Starting load job → {full_table}  ({len(df)} rows)")
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        table = client.get_table(table_ref)
        logger.info(
            f"Load complete. Table now has {table.num_rows} total rows."
        )

    except GoogleAPIError as e:
        logger.error(f"BigQuery load job failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during BigQuery load: {e}")
        raise