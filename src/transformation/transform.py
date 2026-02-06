"""Transform raw CBS crime data into star-schema fact/dimension tables."""

import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def load_raw_crime_data(path: Path | None = None) -> pd.DataFrame:
    """Load raw crime Parquet from the landing zone."""
    path = path or RAW_DIR / "crime_raw.parquet"
    df = pd.read_parquet(path)
    logger.info("Loaded %d rows from %s", len(df), path)
    return df


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from string columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df


def parse_period(period_code: str) -> int | None:
    """Extract year from CBS period code like '2024JJ00'."""
    match = re.match(r"(\d{4})", period_code)
    return int(match.group(1)) if match else None


def build_dim_periods(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique periods dimension."""
    col = "Perioden" if "Perioden" in df.columns else "Periods"
    periods = df[[col]].drop_duplicates().rename(columns={col: "period_code"})
    periods["year"] = periods["period_code"].apply(parse_period)
    periods = periods.dropna(subset=["year"])
    periods["year"] = periods["year"].astype(int)
    logger.info("Built dim_periods: %d rows", len(periods))
    return periods.reset_index(drop=True)


def build_dim_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique regions dimension using region_code and RegioS (name)."""
    name_col = "RegioS" if "RegioS" in df.columns else "Regions"

    regions = (
        df[["region_code", name_col]]
        .drop_duplicates(subset=["region_code"])
        .rename(columns={name_col: "region_name"})
    )
    logger.info("Built dim_regions: %d rows", len(regions))
    return regions.reset_index(drop=True)


def build_dim_crime_types(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique crime type dimension."""
    code_col = None
    for candidate in ["SoortMisdrijf", "Misdrijf", "CrimeType"]:
        if candidate in df.columns:
            code_col = candidate
            break

    if code_col is None:
        for col in df.columns:
            if "misdrijf" in col.lower() or "crime" in col.lower():
                code_col = col
                break

    if code_col is None:
        raise ValueError(f"Cannot find crime type column in: {list(df.columns)}")

    crime_types = df[[code_col]].drop_duplicates().rename(columns={code_col: "crime_code"})
    crime_types["crime_name"] = crime_types["crime_code"]
    logger.info("Built dim_crime_types: %d rows (source column: %s)", len(crime_types), code_col)
    return crime_types.reset_index(drop=True)


def build_fact_crimes(
    df: pd.DataFrame,
    dim_regions: pd.DataFrame,
    dim_crime_types: pd.DataFrame,
    dim_periods: pd.DataFrame,
) -> pd.DataFrame:
    """Build fact table by joining raw data with dimension IDs."""
    period_col = "Perioden" if "Perioden" in df.columns else "Periods"

    # Find crime type column
    crime_col = None
    for candidate in ["SoortMisdrijf", "Misdrijf", "CrimeType"]:
        if candidate in df.columns:
            crime_col = candidate
            break

    # Map to dimension indices
    region_map = dict(zip(dim_regions["region_code"], dim_regions.index))
    crime_map = dict(zip(dim_crime_types["crime_code"], dim_crime_types.index))
    period_map = dict(zip(dim_periods["period_code"], dim_periods.index))

    fact = df.copy()
    fact["region_id"] = fact["region_code"].map(region_map)
    fact["crime_type_id"] = fact[crime_col].map(crime_map)
    fact["period_id"] = fact[period_col].map(period_map)

    # Use the actual CBS column names for values
    value_col = "TotaalGeregistreerdeMisdrijven_1"
    rate_col = "GeregistreerdeMisdrijvenPer1000Inw_3"

    fact["registered_crimes"] = pd.to_numeric(fact.get(value_col, pd.Series()), errors="coerce")
    fact["registered_crimes_per_1000"] = pd.to_numeric(
        fact.get(rate_col, pd.Series()), errors="coerce"
    )

    result = fact[
        ["region_id", "crime_type_id", "period_id", "registered_crimes", "registered_crimes_per_1000"]
    ].dropna(subset=["region_id", "crime_type_id", "period_id"])

    result["region_id"] = result["region_id"].astype(int)
    result["crime_type_id"] = result["crime_type_id"].astype(int)
    result["period_id"] = result["period_id"].astype(int)

    logger.info("Built fact_crimes: %d rows", len(result))
    return result.reset_index(drop=True)


def transform_all(raw_path: Path | None = None) -> dict[str, pd.DataFrame]:
    """Run all transformations and return dimension + fact DataFrames."""
    df = load_raw_crime_data(raw_path)
    df = clean_columns(df)

    dim_regions = build_dim_regions(df)
    dim_crime_types = build_dim_crime_types(df)
    dim_periods = build_dim_periods(df)
    fact_crimes = build_fact_crimes(df, dim_regions, dim_crime_types, dim_periods)

    return {
        "dim_regions": dim_regions,
        "dim_crime_types": dim_crime_types,
        "dim_periods": dim_periods,
        "fact_crimes": fact_crimes,
    }
