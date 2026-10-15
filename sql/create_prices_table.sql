CREATE EXTERNAL TABLE IF NOT EXISTS prices (
  ticker      STRING,
  trade_date  DATE,
  open_price  DOUBLE,
  high_price  DOUBLE,
  low_price   DOUBLE,
  close_price DOUBLE,
  volume      BIGINT
)
PARTITIONED BY (y STRING, m STRING, d STRING)
STORED AS PARQUET
LOCATION 's3://your-portfolio-bucket/prices/';

-- Run this after creating the table (and after each new day) to register partitions:
MSCK REPAIR TABLE prices;
