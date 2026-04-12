import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda"))

from lambda_function import lambda_handler


def _sample_df():
    return pd.DataFrame([
        {"ticker": "AAPL", "date": date(2025, 1, 2), "open": 100.0,
         "high": 105.0, "low": 98.0, "close": 102.0, "volume": 1_000_000}
    ])


@patch("lambda_function.write_parquet", return_value="prices/year=2025/month=04/day=11/prices.parquet")
@patch("lambda_function.fetch_prices", return_value=_sample_df())
@patch("lambda_function.is_first_run", return_value=False)
@patch("lambda_function.read_holdings", return_value=[{"ticker": "AAPL", "qty": 10, "avg_cost": 150.0}])
def test_handler_success(mock_read, mock_first, mock_fetch, mock_write):
    result = lambda_handler({}, None)
    assert result["status"] == "success"
    assert result["rows"] == 1


@patch("lambda_function.read_holdings", return_value=[])
def test_handler_skips_when_no_tickers(mock_read):
    result = lambda_handler({}, None)
    assert result == {"status": "skipped", "reason": "no_tickers"}


@patch("lambda_function.fetch_prices", return_value=pd.DataFrame())
@patch("lambda_function.is_first_run", return_value=False)
@patch("lambda_function.read_holdings", return_value=[{"ticker": "AAPL", "qty": 10, "avg_cost": 150.0}])
def test_handler_skips_on_holiday(mock_read, mock_first, mock_fetch):
    result = lambda_handler({}, None)
    assert result == {"status": "skipped", "reason": "no_data"}


@patch("lambda_function.write_parquet", return_value="prices/year=2025/month=04/day=11/prices.parquet")
@patch("lambda_function.fetch_prices", return_value=_sample_df())
@patch("lambda_function.is_first_run", return_value=True)
@patch("lambda_function.read_holdings", return_value=[{"ticker": "AAPL", "qty": 10, "avg_cost": 150.0}])
def test_handler_uses_1y_on_first_run(mock_read, mock_first, mock_fetch, mock_write):
    lambda_handler({}, None)
    mock_fetch.assert_called_once_with(["AAPL"], period="1y")
