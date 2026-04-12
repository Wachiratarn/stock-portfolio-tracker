import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda"))

from prices import fetch_prices


def _make_ohlcv(n: int = 5) -> pd.DataFrame:
    """Single-ticker flat DataFrame — what yf.download returns for one ticker."""
    idx = pd.date_range("2025-01-02", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open":   [100.0] * n,
            "High":   [105.0] * n,
            "Low":    [98.0]  * n,
            "Close":  [102.0] * n,
            "Volume": [1_000_000] * n,
        },
        index=idx,
    )


@patch("prices.yf.download")
def test_fetch_prices_single_ticker_returns_correct_columns(mock_dl):
    mock_dl.return_value = _make_ohlcv(5)
    result = fetch_prices(["AAPL"], period="5d")
    assert list(result.columns) == ["ticker", "date", "open", "high", "low", "close", "volume"]


@patch("prices.yf.download")
def test_fetch_prices_single_ticker_correct_row_count(mock_dl):
    mock_dl.return_value = _make_ohlcv(5)
    result = fetch_prices(["AAPL"], period="5d")
    assert len(result) == 5
    assert (result["ticker"] == "AAPL").all()


@patch("prices.yf.download")
def test_fetch_prices_skips_nan_close_rows(mock_dl):
    df = _make_ohlcv(3)
    df.loc[df.index[1], "Close"] = float("nan")
    mock_dl.return_value = df
    result = fetch_prices(["AAPL"], period="5d")
    assert len(result) == 2


def test_fetch_prices_empty_tickers_returns_empty_df():
    result = fetch_prices([], period="1y")
    assert result.empty


@patch("prices.yf.download")
def test_fetch_prices_volume_is_integer(mock_dl):
    mock_dl.return_value = _make_ohlcv(1)
    result = fetch_prices(["AAPL"], period="1d")
    assert result["volume"].dtype == int
