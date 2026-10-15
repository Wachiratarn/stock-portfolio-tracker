import pytest
import pandas as pd
import io
from datetime import date
from unittest.mock import patch, MagicMock, call
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda"))

from storage import is_first_run, write_parquet


def _sample_df():
    return pd.DataFrame([
        {
            "ticker": "AAPL", "trade_date": date(2025, 1, 2),
            "open_price": 100.0, "high_price": 105.0, "low_price": 98.0,
            "close_price": 102.0, "volume": 1_000_000,
        }
    ])


@patch("storage.boto3.client")
def test_is_first_run_true_when_no_files(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"KeyCount": 0}
    assert is_first_run("my-bucket") is True


@patch("storage.boto3.client")
def test_is_first_run_false_when_files_exist(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"KeyCount": 3}
    assert is_first_run("my-bucket") is False


@patch("storage.boto3.client")
def test_write_parquet_returns_correct_s3_key(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"KeyCount": 0}
    mock_s3.put_object.return_value = {}

    key = write_parquet(_sample_df(), "my-bucket", run_date=date(2025, 4, 11))
    assert key == "prices/y=2025/m=04/d=11/prices.parquet"


@patch("storage.boto3.client")
def test_write_parquet_calls_put_object(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"KeyCount": 0}
    mock_s3.put_object.return_value = {}

    write_parquet(_sample_df(), "my-bucket", run_date=date(2025, 4, 11))
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["Key"] == "prices/y=2025/m=04/d=11/prices.parquet"


@patch("storage.boto3.client")
def test_write_parquet_skips_existing_partition(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"KeyCount": 1}

    key = write_parquet(_sample_df(), "my-bucket", run_date=date(2025, 4, 11))
    assert key == "prices/y=2025/m=04/d=11/prices.parquet"
    mock_s3.put_object.assert_not_called()


@patch("storage.boto3.client")
def test_write_parquet_empty_df_returns_none(mock_client):
    result = write_parquet(pd.DataFrame(), "my-bucket")
    assert result is None
