# Portfolio Tracker — Design Spec

**Date:** 2026-04-12  
**Status:** Approved

---

## Overview

A serverless AWS pipeline that fetches daily US stock prices after market close, stores them in S3 as Parquet, queries them via Athena, and visualises them in a 4-page Power BI report with scheduled nightly refresh.

---

## Decisions & Constraints

| Decision | Choice | Rationale |
|---|---|---|
| Market | US stocks only | yfinance native support, no currency conversion needed |
| Data source | yfinance (OHLCV) | Free, reliable for US equities |
| Initial backfill | 1 year | Enough to test; easy to extend to 5y later |
| Holdings config | CSV in S3 | Human-edited after trades, no redeploy needed |
| Price storage format | Parquet | ~10x smaller than CSV, fast Athena queries |
| Pipeline approach | Full stack (Lambda → S3 → Athena → Power BI) | Scalable, ~$0/month, clean scheduled refresh |
| Dividends | Not in scope for now | Easy to add later as a separate S3 table |
| Power BI pages | Overview, Performance, Holdings Detail, Transactions | Core reporting needs without dividend data |

---

## Architecture

Data flows top to bottom, every weekday after US market close:

```
EventBridge (cron)          yfinance (external)
       │                           │
       └──────────┬────────────────┘
                  ▼
          Lambda (Python)
          1. Read holdings.csv from S3
          2. Fetch OHLCV from yfinance
          3. Write Parquet to S3
                  │
                  ▼
            S3 Bucket
          ┌─────────────────────────────┐
          │ holdings/holdings.csv       │  ← human-edited config
          │ prices/year=X/month=X/      │  ← daily Parquet, date-partitioned
          │         day=X/prices.parquet│
          └─────────────────────────────┘
                  │
                  ▼
              Athena
          SQL on Parquet via Glue Catalog
          (registered automatically via CREATE EXTERNAL TABLE)
          Cost: ~$0/month at hobby scale
                  │
                  ▼
            Power BI Desktop
          Athena ODBC → star schema → 4-page report
          Scheduled refresh nightly → published to Service
```

**Key properties:**
- Fully serverless — scales to zero when market is closed
- No servers to manage
- EventBridge retries Lambda up to 2 times on failure
- Lambda is idempotent — safe to re-run on holidays (writes nothing if yfinance returns empty)

---

## AWS Components

### EventBridge
- Cron rule: `cron(30 21 ? * MON-FRI *)` — fires 9:30 PM UTC (4:30 PM ET, after market close)
- Triggers Lambda directly

### Lambda (Python)
- **Runtime:** Python 3.12
- **Timeout:** 5 minutes (well within the 15-min Lambda max)
- **Memory:** 512 MB

**Function structure — `lambda_function.py`:**
```python
def lambda_handler(event, context):
    tickers = read_holdings()        # 1. read CSV from S3
    df      = fetch_prices(tickers)  # 2. pull OHLCV from yfinance
    write_parquet(df)                # 3. write Parquet to S3
```

**First-run vs daily detection:**
- On first run: fetches `period="1y"` for all tickers (full backfill)
- On daily runs: fetches `period="2d"` (just the latest trading day)
- Detection: checks whether any Parquet files already exist in S3

**Environment variables:**
```
S3_BUCKET    = your-portfolio-bucket
HOLDINGS_KEY = holdings/holdings.csv
```

No secrets required — Lambda uses its IAM execution role to access S3.

**Lambda Layer (dependencies):**
- `yfinance`
- `pandas`
- `pyarrow`
- `boto3` (built-in, no layer needed)

**Error handling:**
- yfinance call fails for a ticker → log error, skip that ticker, continue with rest
- S3 write fails → raise exception → EventBridge retries automatically (up to 2x)
- Market holiday / weekend → yfinance returns empty DataFrame → Lambda writes nothing, exits cleanly

### S3 Bucket

Two prefixes:

| Prefix | Format | Written by | Read by |
|---|---|---|---|
| `holdings/holdings.csv` | CSV | User (manually) | Lambda |
| `prices/year=X/month=X/day=X/prices.parquet` | Parquet | Lambda | Athena |

**holdings.csv schema:**
```
ticker, qty, avg_cost
AAPL,   10,  150.00
MSFT,    5,  280.00
NVDA,    8,  420.00
```

**prices Parquet schema:**
```
ticker  STRING
date    DATE
open    DOUBLE
high    DOUBLE
low     DOUBLE
close   DOUBLE
volume  BIGINT
```

### Athena

Table created once via DDL — Glue Catalog registers automatically:

```sql
CREATE EXTERNAL TABLE prices (
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

Cost: $5/TB scanned — entire dataset is a few MB → effectively $0/month.

---

## Power BI

### Star Schema

```
dim_holdings          fact_prices ⭐         dim_date
─────────────         ──────────────         ────────────
ticker (PK)    ──▶    ticker (FK)      ◀──    date (PK)
qty                   date   (FK)             year
avg_cost              open                    month
                      high                    quarter
                      low                     week_number
                      close                   is_month_end
                      volume
```

`dim_date` created in Power BI via DAX `CALENDAR()` function — unlocks time intelligence measures.

### DAX Measures

```
Market Value    = SUMX(dim_holdings, dim_holdings[qty] * RELATED(fact_prices[close]))
Cost Basis      = SUMX(dim_holdings, dim_holdings[qty] * dim_holdings[avg_cost])
Unrealised P&L  = [Market Value] - [Cost Basis]
Return %        = DIVIDE([Unrealised P&L], [Cost Basis])
Weight %        = DIVIDE([Market Value], CALCULATE([Market Value], ALL(dim_holdings)))
```

### Report Pages

**Page 1 — Overview**
- KPI cards: Total Value, Cost Basis, P&L $, Return %
- Pie chart: portfolio weight by ticker
- Bar chart: P&L $ per ticker
- Last refreshed timestamp

**Page 2 — Performance**
- Line chart: portfolio value over time
- Date slicer: 1M / 3M / 6M / 1Y toggle
- Per-ticker line chart (multi-line)
- % return since purchase per ticker

**Page 3 — Holdings Detail**
- Table: ticker · qty · avg cost · current price · market value · P&L $ · P&L %
- Conditional formatting: green/red on P&L columns
- Sortable columns

**Page 4 — Transactions**
- Table: ticker · qty · avg_cost · total invested (qty × avg_cost)
- Running total: cumulative cost basis over time
- Sourced from `holdings.csv` — static snapshot of current positions
- Note: manual update required after each trade; a full trade log is out of scope for now

### Connection & Refresh
- Connect Power BI Desktop to Athena via **Simba Athena ODBC driver** (free download, ~15 min Windows setup)
- Publish report to **Power BI Service** (free tier)
- Scheduled refresh: nightly, after Lambda run completes
- Accessible via browser and Power BI mobile app

---

## Out of Scope (for now)

- Dividends & income tracking (add later: separate `dividends/` S3 table + Lambda fetch)
- Full trade log / transaction history (add later: separate `transactions.csv` with date per trade)
- Multi-user / multi-portfolio support
- S&P 500 benchmark comparison
- Risk & allocation page (beta, correlation, sector concentration)
- ASX or other non-US markets

---

## Future Extensions

All of the following can be added without changing the core architecture:

| Extension | What changes |
|---|---|
| Add dividends | Lambda fetches yfinance dividends → new `dividends/` Parquet table → new Athena table → new Power BI page |
| Extend backfill to 5 years | Change `period="1y"` to `period="5y"` in Lambda, re-run |
| Add transactions log | Add `transactions.csv` to S3 with date per trade |
| Add benchmark (SPY) | Add SPY to holdings.csv with qty=0, filter out of P&L calcs |
| Add more pages to Power BI | Add Power BI pages — no pipeline changes needed |
