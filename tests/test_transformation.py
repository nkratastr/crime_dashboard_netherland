"""Tests for the transformation layer."""

import pandas as pd

from src.transformation.transform import (
    build_dim_crime_types,
    build_dim_periods,
    build_dim_regions,
    clean_columns,
    parse_period,
)


def test_parse_period_standard():
    assert parse_period("2024JJ00") == 2024
    assert parse_period("2019JJ00") == 2019


def test_parse_period_invalid():
    assert parse_period("") is None
    assert parse_period("XXXX") is None


def test_clean_columns_strips_whitespace():
    df = pd.DataFrame({"RegioS": ["  GM0363  ", " PV27 "], "Value": [1, 2]})
    result = clean_columns(df)
    assert result["RegioS"].tolist() == ["GM0363", "PV27"]


def test_build_dim_periods():
    df = pd.DataFrame({"Perioden": ["2020JJ00", "2021JJ00", "2020JJ00"]})
    result = build_dim_periods(df)
    assert len(result) == 2
    assert set(result["year"]) == {2020, 2021}


def test_build_dim_regions():
    df = pd.DataFrame({"RegioS": ["GM0363", "GM0518", "GM0363"]})
    result = build_dim_regions(df)
    assert len(result) == 2


def test_build_dim_crime_types():
    df = pd.DataFrame({"SoortMisdrijf": ["Diefstal", "Fraude", "Diefstal"]})
    result = build_dim_crime_types(df)
    assert len(result) == 2
    assert set(result["crime_code"]) == {"Diefstal", "Fraude"}
