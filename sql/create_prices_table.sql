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
