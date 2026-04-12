import pytest
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda"))

from holdings import read_holdings

CSV_CONTENT = "ticker,qty,avg_cost\nAAPL,10,150.00\nMSFT,5,280.00\n"


def _mock_s3(content: str):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: content.encode("utf-8"))
    }
    return mock_s3


@patch("holdings.boto3.client")
def test_read_holdings_returns_list_of_dicts(mock_client):
    mock_client.return_value = _mock_s3(CSV_CONTENT)
    result = read_holdings("my-bucket", "holdings/holdings.csv")
    assert len(result) == 2
    assert result[0] == {"ticker": "AAPL", "qty": 10.0, "avg_cost": 150.00}
    assert result[1] == {"ticker": "MSFT", "qty": 5.0, "avg_cost": 280.00}


@patch("holdings.boto3.client")
def test_read_holdings_strips_whitespace_from_tickers(mock_client):
    mock_client.return_value = _mock_s3("ticker,qty,avg_cost\n AAPL ,10,150.00\n")
    result = read_holdings("my-bucket", "holdings/holdings.csv")
    assert result[0]["ticker"] == "AAPL"


@patch("holdings.boto3.client")
def test_read_holdings_empty_csv_returns_empty_list(mock_client):
    mock_client.return_value = _mock_s3("ticker,qty,avg_cost\n")
    result = read_holdings("my-bucket", "holdings/holdings.csv")
    assert result == []
