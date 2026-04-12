import os

# Provide dummy env vars so lambda_function imports without error
os.environ.setdefault("S3_BUCKET", "test-bucket")
os.environ.setdefault("HOLDINGS_KEY", "holdings/holdings.csv")
