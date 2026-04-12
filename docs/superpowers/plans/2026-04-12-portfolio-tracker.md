# Portfolio Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a serverless AWS pipeline that fetches daily US stock prices via yfinance and surfaces them in a 4-page Power BI report with nightly scheduled refresh.

**Architecture:** EventBridge cron triggers a Lambda (Python) function each weekday after US market close. Lambda reads a holdings CSV from S3, fetches OHLCV data from yfinance, and writes date-partitioned Parquet files back to S3. Athena queries those Parquet files via a single DDL-registered table. Power BI connects to Athena via ODBC, builds a star schema, and serves a 4-page report published to Power BI Service.

**Tech Stack:** Python 3.12, yfinance, pandas, pyarrow, boto3, AWS Lambda + S3 + Athena + EventBridge, Power BI Desktop + Service, Simba Athena ODBC driver

---

## File Structure

```
portfolio/
├── lambda/
│   ├── lambda_function.py   # handler — wires read → fetch → write
│   ├── holdings.py          # read_holdings(): reads holdings.csv from S3
│   ├── prices.py            # fetch_prices(): pulls OHLCV from yfinance
│   └── storage.py           # is_first_run(), write_parquet(): S3 Parquet I/O
├── tests/
│   ├── conftest.py          # shared pytest fixtures
│   ├── test_holdings.py
│   ├── test_prices.py
│   └── test_storage.py
├── data/
│   └── holdings.csv         # your actual positions — upload to S3
├── sql/
│   └── create_prices_table.sql  # Athena DDL — run once in console
├── scripts/
│   └── build_layer.sh       # packages Lambda layer zip (run on any OS)
├── requirements.txt         # dev dependencies (pytest, moto, etc.)
└── .gitignore
```

---

## Phase 1 — Python Code

### Task 1: Project scaffolding

**Files:**
- Create: `lambda/` directory
- Create: `tests/` directory
- Create: `data/` directory
- Create: `sql/` directory
- Create: `scripts/` directory
- Create: `.gitignore`
- Create: `requirements.txt`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p lambda tests data sql scripts
```

- [ ] **Step 2: Create `.gitignore`**

```
__pycache__/
*.pyc
.pytest_cache/
layer/
lambda-layer.zip
.env
*.egg-info/
dist/
.venv/
```

- [ ] **Step 3: Create `requirements.txt` (dev only — not deployed to Lambda)**

```
pytest==9.0.3
pytest-cov==7.1.0
moto[s3]==5.1.22
boto3==1.42.84
yfinance==1.2.1
pandas==3.0.2
pyarrow==23.0.1
```

> Note: pandas 3.0 is a major version bump from 2.x. The Lambda code is unaffected, but if any test helper breaks on `freq="B"` in `pd.date_range`, replace it with `freq="bday"` (the new alias in pandas 3.0+).

- [ ] **Step 4: Install dev dependencies**

```bash
pip install -r requirements.txt
```

Expected: all packages install without errors.

- [ ] **Step 5: Verify pytest runs**

```bash
pytest tests/ -v
```

Expected: `no tests ran` — no errors.

- [ ] **Step 6: Commit**

```bash
git add .gitignore requirements.txt
git commit -m "chore: project scaffolding"
```

---

### Task 2: Holdings reader

**Files:**
- Create: `lambda/holdings.py`
- Create: `tests/test_holdings.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_holdings.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_holdings.py -v
```

Expected: `ModuleNotFoundError: No module named 'holdings'`

- [ ] **Step 3: Implement `lambda/holdings.py`**

```python
import boto3
import csv
import io


def read_holdings(bucket: str, key: str) -> list[dict]:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    return [
        {
            "ticker": row["ticker"].strip(),
            "qty": float(row["qty"]),
            "avg_cost": float(row["avg_cost"]),
        }
        for row in reader
    ]
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_holdings.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add lambda/holdings.py tests/test_holdings.py
git commit -m "feat: holdings CSV reader with S3 integration"
```

---

### Task 3: Price fetcher

**Files:**
- Create: `lambda/prices.py`
- Create: `tests/test_prices.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_prices.py`:

```python
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda"))

from prices import fetch_prices


def _make_ohlcv(n: int = 5) -> pd.DataFrame:
    """Single-ticker flat DataFrame — what yf.download returns for one ticker."""
    idx = pd.date_range("2025-01-02", periods=n, freq="bday")
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_prices.py -v
```

Expected: `ModuleNotFoundError: No module named 'prices'`

- [ ] **Step 3: Implement `lambda/prices.py`**

```python
import pandas as pd
import yfinance as yf


def fetch_prices(tickers: list[str], period: str = "1y") -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()

    raw = yf.download(
        tickers,
        period=period,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    rows = []
    for ticker in tickers:
        # yf.download with group_by="ticker" returns MultiIndex columns when
        # multiple tickers are given, and flat columns when only one ticker.
        if isinstance(raw.columns, pd.MultiIndex):
            ticker_df = raw[ticker]
        else:
            ticker_df = raw  # single ticker — flat columns

        for ts, row in ticker_df.iterrows():
            if pd.isna(row["Close"]):
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "date": ts.date(),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                }
            )

    return pd.DataFrame(rows)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_prices.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add lambda/prices.py tests/test_prices.py
git commit -m "feat: yfinance OHLCV fetcher with multi-ticker support"
```

---

### Task 4: Storage module

**Files:**
- Create: `lambda/storage.py`
- Create: `tests/test_storage.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_storage.py`:

```python
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
            "ticker": "AAPL", "date": date(2025, 1, 2),
            "open": 100.0, "high": 105.0, "low": 98.0,
            "close": 102.0, "volume": 1_000_000,
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
    mock_s3.put_object.return_value = {}

    key = write_parquet(_sample_df(), "my-bucket", run_date=date(2025, 4, 11))
    assert key == "prices/year=2025/month=04/day=11/prices.parquet"


@patch("storage.boto3.client")
def test_write_parquet_calls_put_object(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.put_object.return_value = {}

    write_parquet(_sample_df(), "my-bucket", run_date=date(2025, 4, 11))
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["Key"] == "prices/year=2025/month=04/day=11/prices.parquet"


@patch("storage.boto3.client")
def test_write_parquet_empty_df_returns_none(mock_client):
    result = write_parquet(pd.DataFrame(), "my-bucket")
    assert result is None
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_storage.py -v
```

Expected: `ModuleNotFoundError: No module named 'storage'`

- [ ] **Step 3: Implement `lambda/storage.py`**

```python
import io
from datetime import date

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def is_first_run(bucket: str, prefix: str = "prices/") -> bool:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) == 0


def write_parquet(df: pd.DataFrame, bucket: str, run_date: date = None) -> str | None:
    if df.empty:
        return None

    if run_date is None:
        run_date = date.today()

    key = (
        f"prices/"
        f"year={run_date.year}/"
        f"month={run_date.month:02d}/"
        f"day={run_date.day:02d}/"
        f"prices.parquet"
    )

    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    return key
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_storage.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add lambda/storage.py tests/test_storage.py
git commit -m "feat: S3 Parquet writer with first-run detection"
```

---

### Task 5: Lambda handler

**Files:**
- Create: `lambda/lambda_function.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `tests/conftest.py`**

```python
import os

# Provide dummy env vars so lambda_function imports without error
os.environ.setdefault("S3_BUCKET", "test-bucket")
os.environ.setdefault("HOLDINGS_KEY", "holdings/holdings.csv")
```

- [ ] **Step 2: Implement `lambda/lambda_function.py`**

```python
import os

from holdings import read_holdings
from prices import fetch_prices
from storage import is_first_run, write_parquet

BUCKET = os.environ["S3_BUCKET"]
HOLDINGS_KEY = os.environ["HOLDINGS_KEY"]


def lambda_handler(event, context):
    holdings = read_holdings(BUCKET, HOLDINGS_KEY)
    tickers = [h["ticker"] for h in holdings]

    if not tickers:
        print("No tickers in holdings.csv — exiting.")
        return {"status": "skipped", "reason": "no_tickers"}

    period = "1y" if is_first_run(BUCKET) else "2d"
    print(f"Mode: {'backfill' if period == '1y' else 'daily'}. Tickers: {tickers}")

    df = fetch_prices(tickers, period=period)

    if df.empty:
        print("No price data returned (holiday/weekend) — exiting.")
        return {"status": "skipped", "reason": "no_data"}

    key = write_parquet(df, BUCKET)
    print(f"Written: s3://{BUCKET}/{key} ({len(df)} rows)")

    return {"status": "success", "key": key, "rows": len(df)}
```

- [ ] **Step 3: Write integration-style tests for the handler**

Create `tests/test_lambda_function.py`:

```python
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
```

- [ ] **Step 4: Run all tests**

```bash
pytest tests/ -v
```

Expected: `17 passed`

- [ ] **Step 5: Commit**

```bash
git add lambda/lambda_function.py tests/conftest.py tests/test_lambda_function.py
git commit -m "feat: Lambda handler wiring read/fetch/write with first-run detection"
```

---

### Task 6: Data files and SQL

**Files:**
- Create: `data/holdings.csv`
- Create: `sql/create_prices_table.sql`
- Create: `scripts/build_layer.sh`

- [ ] **Step 1: Create `data/holdings.csv` with your actual positions**

```csv
ticker,qty,avg_cost
AAPL,10,150.00
MSFT,5,280.00
NVDA,8,420.00
```

Replace tickers/qty/avg_cost with your real holdings.

- [ ] **Step 2: Create `sql/create_prices_table.sql`**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS prices (
  ticker STRING,
  date   DATE,
  open   DOUBLE,
  high   DOUBLE,
  low    DOUBLE,
  close  DOUBLE,
  volume BIGINT
)
STORED AS PARQUET
LOCATION 's3://your-portfolio-bucket/prices/'
PARTITIONED BY (year STRING, month STRING, day STRING);

-- Run this after creating the table to register existing partitions:
MSCK REPAIR TABLE prices;
```

- [ ] **Step 3: Create `scripts/build_layer.sh`**

This script builds the Lambda layer zip. Must be run on any OS — pip's `--platform` flag downloads Linux binaries regardless.

```bash
#!/bin/bash
set -e

echo "Building Lambda layer..."
rm -rf layer lambda-layer.zip
mkdir -p layer/python

pip install \
  yfinance \
  pandas \
  pyarrow \
  -t layer/python \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --only-binary=:all: \
  --quiet

cd layer && zip -r ../lambda-layer.zip python/
echo "Done: lambda-layer.zip ($(du -sh ../lambda-layer.zip | cut -f1))"
```

- [ ] **Step 4: Make script executable and build the layer**

```bash
chmod +x scripts/build_layer.sh
bash scripts/build_layer.sh
```

Expected: `Done: lambda-layer.zip (~50–80 MB)`

- [ ] **Step 5: Commit**

```bash
git add data/holdings.csv sql/create_prices_table.sql scripts/build_layer.sh
git commit -m "feat: holdings data, Athena DDL, and Lambda layer build script"
```

---

## Phase 2 — AWS Infrastructure

> Manual steps in the AWS Console. No code changes. Verify each step before proceeding.

### Task 7: S3 bucket and IAM role

- [ ] **Step 1: Create S3 bucket**

In AWS Console → S3 → Create bucket:
- Name: `your-portfolio-bucket` (must be globally unique — pick your own name)
- Region: `us-east-1` (keep consistent — Athena works best in us-east-1)
- Block all public access: ON
- Versioning: OFF (not needed)
- Click Create.

- [ ] **Step 2: Upload holdings.csv**

In the bucket → Create folder → name it `holdings` → Upload `data/holdings.csv` into `holdings/`.

Verify: `s3://your-portfolio-bucket/holdings/holdings.csv` exists.

- [ ] **Step 3: Create IAM policy for Lambda**

In AWS Console → IAM → Policies → Create policy → JSON tab:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::your-portfolio-bucket/holdings/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::your-portfolio-bucket/prices/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::your-portfolio-bucket",
      "Condition": {
        "StringLike": {"s3:prefix": ["prices/*"]}
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

Name it: `PortfolioTrackerLambdaPolicy`. Click Create.

- [ ] **Step 4: Create IAM role for Lambda**

In IAM → Roles → Create role:
- Trusted entity: AWS service → Lambda
- Attach policy: `PortfolioTrackerLambdaPolicy`
- Name: `PortfolioTrackerLambdaRole`
- Click Create.

---

### Task 8: Deploy Lambda function

- [ ] **Step 1: Upload Lambda layer**

In AWS Console → Lambda → Layers → Create layer:
- Name: `portfolio-tracker-deps`
- Upload: `lambda-layer.zip`
- Compatible runtimes: Python 3.12
- Compatible architectures: x86_64
- Click Create. Note the Layer ARN.

- [ ] **Step 2: Create Lambda function**

In Lambda → Functions → Create function:
- Author from scratch
- Name: `portfolio-tracker`
- Runtime: Python 3.12
- Architecture: x86_64
- Execution role: Use existing role → `PortfolioTrackerLambdaRole`
- Click Create.

- [ ] **Step 3: Upload function code**

Zip the lambda directory contents (not the directory itself):

```bash
cd lambda && zip -r ../portfolio-tracker.zip . && cd ..
```

In the Lambda function → Code → Upload from → .zip file → upload `portfolio-tracker.zip`.

- [ ] **Step 4: Add the Lambda layer**

In the Lambda function → Code → Scroll to Layers → Add a layer:
- Custom layers → `portfolio-tracker-deps` → latest version
- Click Add.

- [ ] **Step 5: Set environment variables**

In Configuration → Environment variables → Edit:
- `S3_BUCKET` = `your-portfolio-bucket`
- `HOLDINGS_KEY` = `holdings/holdings.csv`
- Click Save.

- [ ] **Step 6: Set timeout**

In Configuration → General configuration → Edit:
- Timeout: 5 min 0 sec
- Memory: 512 MB
- Click Save.

- [ ] **Step 7: Test manually**

In the Lambda function → Test → Create new event:
- Event name: `test`
- Event JSON: `{}`
- Click Test.

Expected response:
```json
{
  "status": "success",
  "key": "prices/year=2026/month=04/day=12/prices.parquet",
  "rows": 250
}
```

Verify the Parquet file appears in S3 under `prices/`.

---

### Task 9: EventBridge cron rule

- [ ] **Step 1: Create EventBridge rule**

In AWS Console → EventBridge → Rules → Create rule:
- Name: `portfolio-tracker-daily`
- Description: `Trigger portfolio Lambda after US market close`
- Rule type: Schedule
- Schedule pattern: Cron expression
- Cron: `30 21 ? * MON-FRI *` (9:30 PM UTC = 4:30 PM ET)
- Click Next.

- [ ] **Step 2: Set target**

- Target type: AWS service → Lambda function
- Function: `portfolio-tracker`
- Click Next → Next → Create rule.

- [ ] **Step 3: Verify rule is enabled**

In EventBridge → Rules → confirm `portfolio-tracker-daily` shows State: **Enabled**.

---

### Task 10: Athena table setup

- [ ] **Step 1: Create Athena results bucket**

Athena needs an S3 location to write query results.

In S3 → Create bucket:
- Name: `your-portfolio-bucket-athena-results`
- Region: `us-east-1`
- Block all public access: ON

- [ ] **Step 2: Configure Athena query result location**

In AWS Console → Athena → Settings → Manage:
- Query result location: `s3://your-portfolio-bucket-athena-results/`
- Click Save.

- [ ] **Step 3: Create the prices table**

In Athena → Query editor, paste the contents of `sql/create_prices_table.sql` (replace `your-portfolio-bucket` with your actual bucket name):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS prices (
  ticker STRING,
  date   DATE,
  open   DOUBLE,
  high   DOUBLE,
  low    DOUBLE,
  close  DOUBLE,
  volume BIGINT
)
STORED AS PARQUET
LOCATION 's3://your-portfolio-bucket/prices/'
PARTITIONED BY (year STRING, month STRING, day STRING);
```

Click Run. Expected: `Query successful`.

- [ ] **Step 4: Register partitions**

In Athena query editor:

```sql
MSCK REPAIR TABLE prices;
```

Click Run. Expected: `Query successful` (may say "Partitions not altered" if only one partition exists — that's fine).

- [ ] **Step 5: Verify data is queryable**

```sql
SELECT ticker, date, close
FROM prices
ORDER BY date DESC
LIMIT 10;
```

Expected: rows returned showing your tickers with recent close prices.

---

## Phase 3 — Power BI

> Manual steps in Power BI Desktop. Requires the pipeline from Phase 2 to be working and data in S3.

### Task 11: Athena ODBC setup

- [ ] **Step 1: Download Simba Athena ODBC driver**

Go to: https://docs.aws.amazon.com/athena/latest/ug/connect-with-odbc.html
Download the Windows 64-bit installer. Run the installer, accept defaults.

- [ ] **Step 2: Create AWS credentials for ODBC**

In AWS Console → IAM → Users → Create user:
- Name: `powerbi-athena-reader`
- Attach policy: create a new inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:ListDatabases",
        "athena:GetDatabase",
        "athena:ListTableMetadata",
        "athena:GetTableMetadata"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::your-portfolio-bucket",
        "arn:aws:s3:::your-portfolio-bucket/*",
        "arn:aws:s3:::your-portfolio-bucket-athena-results",
        "arn:aws:s3:::your-portfolio-bucket-athena-results/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetTables", "glue:GetDatabase", "glue:GetDatabases"],
      "Resource": "*"
    }
  ]
}
```

Create access keys for this user. Save the Access Key ID and Secret Access Key.

- [ ] **Step 3: Configure ODBC DSN**

Open Windows → ODBC Data Sources (64-bit):
- System DSN → Add → Simba Athena ODBC Driver → Finish
- DSN Name: `AthenaPortfolio`
- Region: `us-east-1`
- S3 Output Location: `s3://your-portfolio-bucket-athena-results/`
- Authentication Type: IAM Credentials
- Access Key: *(from Step 2)*
- Secret Key: *(from Step 2)*
- Click Test → Expected: `Connection successful`

---

### Task 12: Connect Power BI and build star schema

- [ ] **Step 1: Connect Power BI to Athena**

Open Power BI Desktop → Get Data → ODBC:
- DSN: `AthenaPortfolio`
- Click OK → Connect

In the Navigator, select `default` → `prices` → Click Load.

- [ ] **Step 2: Load holdings as a local table**

In Power BI → Get Data → Text/CSV → select your local `data/holdings.csv`.
- Ensure columns: `ticker` (text), `qty` (decimal), `avg_cost` (decimal)
- Click Load. Rename this table to `dim_holdings` in the Fields pane.

- [ ] **Step 3: Rename prices table**

In the Fields pane, right-click the Athena `prices` table → Rename → `fact_prices`.

- [ ] **Step 4: Create `dim_date` table using DAX**

In Home → New Table:

```dax
dim_date =
ADDCOLUMNS(
    CALENDAR(DATE(2024, 1, 1), DATE(2027, 12, 31)),
    "Year",        YEAR([Date]),
    "Month",       MONTH([Date]),
    "MonthName",   FORMAT([Date], "MMMM"),
    "Quarter",     QUARTER([Date]),
    "WeekNumber",  WEEKNUM([Date]),
    "IsMonthEnd",  [Date] = EOMONTH([Date], 0)
)
```

- [ ] **Step 5: Set relationships in Model view**

Go to Model view (diagram icon in left sidebar).

Add relationships:
- `fact_prices[ticker]` → `dim_holdings[ticker]` (Many to One, single direction)
- `fact_prices[date]` → `dim_date[Date]` (Many to One, single direction)

---

### Task 13: DAX measures

- [ ] **Step 1: Create all DAX measures in `fact_prices`**

In the Fields pane, select `fact_prices`. For each measure, right-click `fact_prices` → New Measure:

**Market Value:**
```dax
Market Value =
SUMX(
    dim_holdings,
    dim_holdings[qty] *
    CALCULATE(
        LASTNONBLANK(fact_prices[close], 1),
        ALLEXCEPT(fact_prices, fact_prices[ticker])
    )
)
```

**Cost Basis:**
```dax
Cost Basis =
SUMX(dim_holdings, dim_holdings[qty] * dim_holdings[avg_cost])
```

**Unrealised P&L:**
```dax
Unrealised PnL = [Market Value] - [Cost Basis]
```

**Return %:**
```dax
Return % = DIVIDE([Unrealised PnL], [Cost Basis])
```

**Weight %:**
```dax
Weight % =
DIVIDE(
    [Market Value],
    CALCULATE([Market Value], ALL(dim_holdings))
)
```

**Latest Close** (used in Holdings Detail table):
```dax
Latest Close =
CALCULATE(
    LASTNONBLANKVALUE(fact_prices[date], MAX(fact_prices[close])),
    ALLEXCEPT(fact_prices, fact_prices[ticker])
)
```

- [ ] **Step 2: Verify measures in a table visual**

Insert a Table visual. Add fields: `dim_holdings[ticker]`, `[Market Value]`, `[Cost Basis]`, `[Unrealised PnL]`, `[Return %]`. Confirm numbers look correct against your known positions.

---

### Task 14: Page 1 — Overview

- [ ] **Step 1: Rename the default page**

Right-click the default page tab → Rename → `Overview`.

- [ ] **Step 2: Add KPI cards**

Insert 4 Card visuals (one per metric). Set each card's Field to:
- Card 1: `[Market Value]` — Label: "Total Value"
- Card 2: `[Cost Basis]` — Label: "Cost Basis"
- Card 3: `[Unrealised PnL]` — Label: "Unrealised P&L"
- Card 4: `[Return %]` — Label: "Return %" (format as percentage in Format pane)

Arrange cards in a row at the top of the page.

- [ ] **Step 3: Add portfolio weight pie chart**

Insert a Pie Chart visual:
- Legend: `dim_holdings[ticker]`
- Values: `[Weight %]`
- Title: "Portfolio Weight"

- [ ] **Step 4: Add P&L bar chart**

Insert a Clustered Bar Chart visual:
- Y-axis: `dim_holdings[ticker]`
- X-axis: `[Unrealised PnL]`
- Title: "Unrealised P&L by Stock"

- [ ] **Step 5: Add last refreshed card**

Insert a Card visual:
- Field: `MAX(fact_prices[date])`
- Label: "Data as of"
- Format: Short date

---

### Task 15: Page 2 — Performance

- [ ] **Step 1: Add new page**

Click `+` next to the Overview tab → Rename → `Performance`.

- [ ] **Step 2: Add portfolio value line chart**

Insert a Line Chart visual:
- X-axis: `dim_date[Date]`
- Y-axis: `[Market Value]`
- Title: "Portfolio Value Over Time"

- [ ] **Step 3: Add date range slicer**

Insert a Slicer visual:
- Field: `dim_date[Date]`
- In Format pane → Slicer settings → Style: Between (gives a date range picker)

Place the slicer at the top of the page so it filters both charts.

- [ ] **Step 4: Add per-ticker line chart**

Insert a Line Chart visual:
- X-axis: `dim_date[Date]`
- Y-axis: `fact_prices[close]`
- Legend: `fact_prices[ticker]`
- Title: "Price History by Stock"

- [ ] **Step 5: Add return % since purchase bar chart**

Insert a Clustered Bar Chart visual:
- Y-axis: `dim_holdings[ticker]`
- X-axis: `[Return %]`
- Title: "Return % Since Purchase"
- Format X-axis as percentage.

---

### Task 16: Page 3 — Holdings Detail

- [ ] **Step 1: Add new page**

Click `+` → Rename → `Holdings Detail`.

- [ ] **Step 2: Add holdings table**

Insert a Table visual. Add columns in this order:
- `dim_holdings[ticker]`
- `dim_holdings[qty]`
- `dim_holdings[avg_cost]`
- `[Latest Close]` (defined in Task 13)
- `[Market Value]`
- `[Unrealised PnL]`
- `[Return %]`

- [ ] **Step 3: Apply conditional formatting to P&L**

Select the table visual → Format pane → Cell elements → `Unrealised PnL`:
- Font color → Conditional formatting → Rules:
  - If value < 0 → Red (`#FF0000`)
  - If value >= 0 → Green (`#00B050`)

Repeat for `Return %`.

---

### Task 17: Page 4 — Transactions

- [ ] **Step 1: Add new page**

Click `+` → Rename → `Transactions`.

- [ ] **Step 2: Add positions snapshot table**

Insert a Table visual with fields from `dim_holdings`:
- `dim_holdings[ticker]`
- `dim_holdings[qty]`
- `dim_holdings[avg_cost]`

Add a calculated column to `dim_holdings` for total invested:

In Home → New Column (with `dim_holdings` selected):
```dax
Total Invested = dim_holdings[qty] * dim_holdings[avg_cost]
```

Add `dim_holdings[Total Invested]` to the table.

- [ ] **Step 3: Add cumulative cost basis bar chart**

Insert a Clustered Bar Chart:
- Y-axis: `dim_holdings[ticker]`
- X-axis: `dim_holdings[Total Invested]`
- Title: "Total Invested by Stock"

- [ ] **Step 4: Add explanatory text box**

Insert a Text Box:
```
Transactions reflect current holdings snapshot from holdings.csv.
Update holdings.csv and re-upload to S3 after each trade.
```

---

### Task 18: Publish and schedule refresh

- [ ] **Step 1: Save the Power BI file**

File → Save As → `portfolio-tracker.pbix` → save in the `portfolio/` directory.

- [ ] **Step 2: Publish to Power BI Service**

Home → Publish → Sign in to your Power BI account → select `My Workspace` → Click Publish.

Expected: "Publishing succeeded" with a link to open in browser.

- [ ] **Step 3: Configure scheduled refresh**

In Power BI Service (browser):
- Go to My Workspace → Datasets → `portfolio-tracker`
- Settings → Scheduled refresh → On
- Refresh frequency: Daily
- Time: 22:30 UTC (10:30 PM UTC — 1 hour after Lambda runs at 9:30 PM)
- Time zone: UTC
- Click Apply.

- [ ] **Step 4: Set ODBC credentials in Service**

In Dataset settings → Data source credentials → Edit credentials for `AthenaPortfolio` ODBC:
- Authentication method: Basic
- Access Key ID in Username field
- Secret Access Key in Password field
- Click Sign in.

- [ ] **Step 5: Trigger a manual refresh to verify**

In My Workspace → Datasets → `portfolio-tracker` → Refresh now.

Expected: refresh completes within 1–2 minutes. Open the report in browser and confirm data is visible.

- [ ] **Step 6: Final commit**

```bash
git add data/portfolio-tracker.pbix
git commit -m "feat: Power BI report with 4 pages published to Service"
```

---

## Done

At this point:
- Lambda runs every weekday at 9:30 PM UTC, fetches prices, writes Parquet to S3
- Athena queries the data with SQL
- Power BI refreshes nightly at 10:30 PM UTC and shows the 4-page report

**To update your holdings after a trade:**
1. Edit `data/holdings.csv` locally
2. Upload to `s3://your-portfolio-bucket/holdings/holdings.csv` (overwrite)
3. Power BI will pick up the change on the next nightly refresh

**To extend backfill to 5 years:**
1. In `lambda/lambda_function.py`, change `"1y"` to `"5y"` in the `is_first_run` branch
2. Delete all files under `s3://your-portfolio-bucket/prices/` (so `is_first_run` returns True again)
3. Re-zip and re-upload `lambda/` to the Lambda function
4. Trigger a manual test run
