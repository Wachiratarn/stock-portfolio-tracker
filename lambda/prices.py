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