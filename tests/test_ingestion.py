"""Tests for the ingestion layer."""

from unittest.mock import patch

import pandas as pd

from src.ingestion.cbs_client import filter_municipalities


def test_filter_municipalities_keeps_gm_codes():
    df = pd.DataFrame({
        "RegioS": ["GM0363", "GM0518", "PV27  ", "NL01  "],
        "Value": [100, 200, 300, 400],
    })
    result = filter_municipalities(df)
    assert len(result) == 2
    assert list(result["RegioS"]) == ["GM0363", "GM0518"]


def test_filter_municipalities_empty():
    df = pd.DataFrame({
        "RegioS": ["PV27", "NL01"],
        "Value": [300, 400],
    })
    result = filter_municipalities(df)
    assert result.empty
