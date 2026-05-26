-- Multi-timeframe layout. Каждый TF — отдельная таблица одинаковой
-- схемы. Ingest пишет в каждую TF свой набор closing snapshots
-- (per-symbol aggregator с window_ms = tf_seconds × 1000).
--
-- Cross-region dedup идёт штатно через ReplacingMergeTree(ingested_at)
-- per-table: Tokyo и Singapore оба пишут одно и то же логическое окно,
-- merge оставит самое позднее.
--
-- TTL не задаётся миграцией — его выставляет cluster-ingest на старте
-- per-table через [ingest.retention]. См. migrations/001 и config.rs.
--
-- Эта миграция заменяет VIEW'ы из 002_rollup_views.sql реальными
-- MergeTree-таблицами (с тем же именем). DROP TABLE в CH работает и для
-- VIEW'ов, поэтому безопасно вызывать на свежесозданной БД и на той,
-- где 002 уже применили.

DROP TABLE IF EXISTS clusters.clusters_5m;
DROP TABLE IF EXISTS clusters.clusters_15m;
DROP TABLE IF EXISTS clusters.clusters_1h;
DROP TABLE IF EXISTS clusters.clusters_4h;
DROP TABLE IF EXISTS clusters.clusters_1d;

CREATE TABLE IF NOT EXISTS clusters.clusters_30s (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

-- clusters_1m уже существует (из 001) — пропускаем.

CREATE TABLE IF NOT EXISTS clusters.clusters_5m (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

CREATE TABLE IF NOT EXISTS clusters.clusters_15m (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

CREATE TABLE IF NOT EXISTS clusters.clusters_30m (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

CREATE TABLE IF NOT EXISTS clusters.clusters_1h (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

CREATE TABLE IF NOT EXISTS clusters.clusters_4h (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

CREATE TABLE IF NOT EXISTS clusters.clusters_1d (
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
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, min_bytes_for_wide_part = 10485760;

