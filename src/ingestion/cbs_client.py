"""Ingest crime data from CBS Open Data (dataset 83648NED)."""

import logging
from pathlib import Path

import cbsodata
import pandas as pd

logger = logging.getLogger(__name__)

DATASET_ID = "83648NED"
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def fetch_crime_data() -> pd.DataFrame:
    """Fetch registered crime data from CBS and return as DataFrame."""
    logger.info("Fetching dataset %s from CBS Open Data...", DATASET_ID)
    raw = cbsodata.get_data(DATASET_ID)
    df = pd.DataFrame(raw)
    logger.info("Fetched %d rows with columns: %s", len(df), list(df.columns))
    return df


def filter_municipalities(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only municipality-level rows (region codes starting with 'GM')."""
    col = "RegioS" if "RegioS" in df.columns else "Regions"
    df[col] = df[col].str.strip()
    mask = df[col].str.startswith("GM")
    filtered = df[mask].copy()
    logger.info("Filtered to %d municipality-level rows (from %d total)", len(filtered), len(df))
    return filtered


def save_raw(df: pd.DataFrame, filename: str = "crime_raw.parquet") -> Path:
    """Save DataFrame as Parquet to the raw data landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    df.to_parquet(path, index=False)
    logger.info("Saved raw crime data to %s (%d rows)", path, len(df))
    return path


def ingest_crime_data() -> Path:
    """Full ingestion pipeline: fetch → filter → save."""
    df = fetch_crime_data()
    df = filter_municipalities(df)
    return save_raw(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_crime_data()
