CREATE DATABASE IF NOT EXISTS clusters;

-- Primary ingestion table. One row per (symbol, 1m window, price bucket).
-- ReplacingMergeTree: late-arriving duplicates (from WS retry or
-- cross-region reconciliation) collapse on merge, keeping the latest by
-- ingested_at. The ingest_region column lets the reconciler compare
-- Tokyo vs Singapore writes of the same (exchange, symbol, window, price).
CREATE TABLE IF NOT EXISTS clusters.clusters_1m (
    exchange       LowCardinality(String),
    market_type    Enum8('spot' = 1, 'perp' = 2),
    quote          Enum8('USDT' = 1, 'USDC' = 2),
    symbol         LowCardinality(String),
    window_start   DateTime64(3, 'UTC'),
    price          Int64,
    price_scale    UInt8,
    bid_qty        Int64,
    ask_qty        Int64,
    trades         UInt32,
    qty_scale      UInt8,
    ingest_region  LowCardinality(String),
    ingested_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (exchange, market_type, quote, symbol, window_start, price)
TTL toDateTime(window_start) + INTERVAL 90 DAY TO VOLUME 'cold',
    toDateTime(window_start) + INTERVAL 3 YEAR DELETE
SETTINGS
    storage_policy = 'hot_cold',
    index_granularity = 8192,
    min_bytes_for_wide_part = 10485760;
