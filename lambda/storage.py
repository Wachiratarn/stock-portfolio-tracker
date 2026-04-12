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
