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


def fetch_region_metadata() -> pd.DataFrame:
    """Fetch region metadata to map region names back to CBS codes (GM0363 etc.)."""
    logger.info("Fetching RegioS metadata for %s...", DATASET_ID)
    meta = cbsodata.get_meta(DATASET_ID, "RegioS")
    df_meta = pd.DataFrame(meta)
    # Build name→code mapping: strip whitespace from Title and Key
    df_meta["Key"] = df_meta["Key"].str.strip()
    df_meta["Title"] = df_meta["Title"].str.strip()
    logger.info("Fetched %d region metadata entries", len(df_meta))
    return df_meta[["Key", "Title"]]


def filter_municipalities(df: pd.DataFrame, region_meta: pd.DataFrame) -> pd.DataFrame:
    """Keep only municipality-level rows using metadata to identify GM codes."""
    col = "RegioS" if "RegioS" in df.columns else "Regions"
    df[col] = df[col].str.strip()

    # Get municipality names from metadata (codes starting with GM)
    gm_meta = region_meta[region_meta["Key"].str.startswith("GM")]
    municipality_names = set(gm_meta["Title"])

    mask = df[col].isin(municipality_names)
    filtered = df[mask].copy()

    # Add the region code column by mapping name → code
    name_to_code = dict(zip(gm_meta["Title"], gm_meta["Key"]))
    filtered["region_code"] = filtered[col].map(name_to_code)

    logger.info("Filtered to %d municipality-level rows (from %d total)", len(filtered), len(df))
    return filtered


def save_raw(df: pd.DataFrame, filename: str = "crime_raw.parquet") -> Path:
    """Save DataFrame as Parquet to the raw data landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    df.to_parquet(path, index=False)
    logger.info("Saved raw crime data to %s (%d rows)", path, len(df))
    return path


def save_region_meta(region_meta: pd.DataFrame, filename: str = "region_meta.parquet") -> Path:
    """Save region metadata as Parquet."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    region_meta.to_parquet(path, index=False)
    logger.info("Saved region metadata to %s", path)
    return path


def ingest_crime_data() -> Path:
    """Full ingestion pipeline: fetch → filter → save."""
    df = fetch_crime_data()
    region_meta = fetch_region_metadata()
    save_region_meta(region_meta)
    df = filter_municipalities(df, region_meta)
    return save_raw(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_crime_data()
