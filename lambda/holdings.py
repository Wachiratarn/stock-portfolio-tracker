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
