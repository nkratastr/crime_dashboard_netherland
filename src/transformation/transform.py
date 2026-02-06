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
    """Extract unique regions dimension."""
    code_col = "RegioS" if "RegioS" in df.columns else "Regions"
    name_col = "RegioS" if "RegioS" in df.columns else "Regions"

    # CBS data has separate code and name — but in 83648NED the RegioS column
    # contains the code. The name may be in a Title field or we derive it.
    regions = df[[code_col]].drop_duplicates().rename(columns={code_col: "region_code"})

    # Try to get region names from a Title column if present
    if "Title" in df.columns:
        name_map = df.drop_duplicates(subset=[code_col]).set_index(code_col)["Title"].to_dict()
        regions["region_name"] = regions["region_code"].map(name_map)
    else:
        regions["region_name"] = regions["region_code"]

    logger.info("Built dim_regions: %d rows", len(regions))
    return regions.reset_index(drop=True)


def build_dim_crime_types(df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique crime type dimension."""
    # CBS column name for crime type
    code_col = None
    for candidate in ["SoortMisdrijf", "Misdrijf", "CrimeType"]:
        if candidate in df.columns:
            code_col = candidate
            break

    if code_col is None:
        # Fallback: look for any column with 'misdrijf' or 'crime' in name
        for col in df.columns:
            if "misdrijf" in col.lower() or "crime" in col.lower():
                code_col = col
                break

    if code_col is None:
        raise ValueError(f"Cannot find crime type column in: {list(df.columns)}")

    crime_types = df[[code_col]].drop_duplicates().rename(columns={code_col: "crime_code"})
    crime_types["crime_name"] = crime_types["crime_code"]  # Will be enriched later
    logger.info("Built dim_crime_types: %d rows (source column: %s)", len(crime_types), code_col)
    return crime_types.reset_index(drop=True)


def build_fact_crimes(
    df: pd.DataFrame,
    dim_regions: pd.DataFrame,
    dim_crime_types: pd.DataFrame,
    dim_periods: pd.DataFrame,
) -> pd.DataFrame:
    """Build fact table by joining raw data with dimension IDs."""
    code_col = "RegioS" if "RegioS" in df.columns else "Regions"
    period_col = "Perioden" if "Perioden" in df.columns else "Periods"

    # Find crime type column
    crime_col = None
    for candidate in ["SoortMisdrijf", "Misdrijf", "CrimeType"]:
        if candidate in df.columns:
            crime_col = candidate
            break
    if crime_col is None:
        for col in df.columns:
            if "misdrijf" in col.lower() or "crime" in col.lower():
                crime_col = col
                break

    # Find value columns for registered crimes
    value_col = None
    rate_col = None
    for col in df.columns:
        col_lower = col.lower()
        if "geregistreerd" in col_lower or "registered" in col_lower:
            if "relatief" in col_lower or "1000" in col_lower or "per" in col_lower:
                rate_col = col
            else:
                if value_col is None:
                    value_col = col
        elif "totaalmisdrijven" in col_lower.replace(" ", ""):
            value_col = col

    # Build region ID mapping
    region_map = dim_regions.reset_index().set_index("region_code")["index"].to_dict()
    crime_map = dim_crime_types.reset_index().set_index("crime_code")["index"].to_dict()
    period_map = dim_periods.reset_index().set_index("period_code")["index"].to_dict()

    fact = df.copy()
    fact["region_id"] = fact[code_col].map(region_map)
    fact["crime_type_id"] = fact[crime_col].map(crime_map)
    fact["period_id"] = fact[period_col].map(period_map)

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
