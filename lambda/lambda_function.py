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
