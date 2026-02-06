"""Tests for the data quality checks."""

import pytest
import pandas as pd

from src.quality.checks import (
    QualityCheckError,
    check_no_nulls,
    check_non_negative,
    check_not_empty,
    check_unique,
)


def test_check_not_empty_passes():
    df = pd.DataFrame({"a": [1, 2]})
    check_not_empty(df, "test")


def test_check_not_empty_fails():
    df = pd.DataFrame({"a": []})
    with pytest.raises(QualityCheckError, match="is empty"):
        check_not_empty(df, "test")


def test_check_no_nulls_passes():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    check_no_nulls(df, ["a", "b"], "test")


def test_check_no_nulls_fails():
    df = pd.DataFrame({"a": [1, None]})
    with pytest.raises(QualityCheckError, match="null"):
        check_no_nulls(df, ["a"], "test")


def test_check_non_negative_passes():
    df = pd.DataFrame({"val": [0, 1, 100]})
    check_non_negative(df, ["val"], "test")


def test_check_non_negative_fails():
    df = pd.DataFrame({"val": [1, -5, 3]})
    with pytest.raises(QualityCheckError, match="negative"):
        check_non_negative(df, ["val"], "test")


def test_check_unique_passes():
    df = pd.DataFrame({"code": ["A", "B", "C"]})
    check_unique(df, ["code"], "test")


def test_check_unique_fails():
    df = pd.DataFrame({"code": ["A", "B", "A"]})
    with pytest.raises(QualityCheckError, match="duplicate"):
        check_unique(df, ["code"], "test")
