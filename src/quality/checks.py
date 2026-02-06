"""Data quality checks for the crime pipeline."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


class QualityCheckError(Exception):
    pass


def check_not_empty(df: pd.DataFrame, name: str) -> None:
    """Verify DataFrame is not empty."""
    if df.empty:
        raise QualityCheckError(f"{name} is empty")
    logger.info("PASS: %s has %d rows", name, len(df))


def check_no_nulls(df: pd.DataFrame, columns: list[str], name: str) -> None:
    """Verify specified columns have no null values."""
    for col in columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise QualityCheckError(f"{name}.{col} has {null_count} null values")
    logger.info("PASS: %s has no nulls in %s", name, columns)


def check_non_negative(df: pd.DataFrame, columns: list[str], name: str) -> None:
    """Verify numeric columns have no negative values (ignoring nulls)."""
    for col in columns:
        negative_count = (df[col].dropna() < 0).sum()
        if negative_count > 0:
            raise QualityCheckError(f"{name}.{col} has {negative_count} negative values")
    logger.info("PASS: %s has no negatives in %s", name, columns)


def check_unique(df: pd.DataFrame, columns: list[str], name: str) -> None:
    """Verify column(s) form a unique key."""
    duplicate_count = df.duplicated(subset=columns).sum()
    if duplicate_count > 0:
        raise QualityCheckError(f"{name} has {duplicate_count} duplicate rows on {columns}")
    logger.info("PASS: %s is unique on %s", name, columns)


def check_referential_integrity(
    fact_df: pd.DataFrame,
    fact_col: str,
    dim_df: pd.DataFrame,
    dim_col: str,
    name: str,
) -> None:
    """Verify all fact foreign keys exist in the dimension table."""
    fact_keys = set(fact_df[fact_col].dropna().unique())
    dim_keys = set(dim_df[dim_col].unique())
    orphans = fact_keys - dim_keys
    if orphans:
        raise QualityCheckError(
            f"{name}: {len(orphans)} orphan keys in fact.{fact_col} not found in dim.{dim_col}"
        )
    logger.info("PASS: %s referential integrity OK (%d keys)", name, len(fact_keys))


def run_all_checks(tables: dict[str, pd.DataFrame]) -> None:
    """Run all quality checks on the transformed tables."""
    dim_regions = tables["dim_regions"]
    dim_crime_types = tables["dim_crime_types"]
    dim_periods = tables["dim_periods"]
    fact_crimes = tables["fact_crimes"]

    # Not empty
    for name, df in tables.items():
        check_not_empty(df, name)

    # No nulls on key columns
    check_no_nulls(dim_regions, ["region_code", "region_name"], "dim_regions")
    check_no_nulls(dim_crime_types, ["crime_code", "crime_name"], "dim_crime_types")
    check_no_nulls(dim_periods, ["period_code", "year"], "dim_periods")
    check_no_nulls(fact_crimes, ["region_id", "crime_type_id", "period_id"], "fact_crimes")

    # Unique keys
    check_unique(dim_regions, ["region_code"], "dim_regions")
    check_unique(dim_crime_types, ["crime_code"], "dim_crime_types")
    check_unique(dim_periods, ["period_code"], "dim_periods")

    # Non-negative values
    check_non_negative(fact_crimes, ["registered_crimes"], "fact_crimes")

    # Referential integrity
    check_referential_integrity(
        fact_crimes, "region_id", dim_regions.reset_index(), "index", "fact→dim_regions"
    )
    check_referential_integrity(
        fact_crimes, "crime_type_id", dim_crime_types.reset_index(), "index", "fact→dim_crime_types"
    )
    check_referential_integrity(
        fact_crimes, "period_id", dim_periods.reset_index(), "index", "fact→dim_periods"
    )

    logger.info("All quality checks passed!")
