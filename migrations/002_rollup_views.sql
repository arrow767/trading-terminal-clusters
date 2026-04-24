-- Rollup access is exposed as plain VIEWs over clusters_1m FINAL.
-- Upside: zero double-count risk from ReplacingMergeTree late-replace +
-- zero maintenance. Downside: long historical 1h/4h/1d scans touch more
-- rows than a pre-materialized table would. When a specific TF's query
-- latency becomes a problem in prod we swap the VIEW for a
-- REFRESHABLE MATERIALIZED VIEW backed by its own MergeTree table — the
-- SQL shape of the SELECT stays identical, so the query-api does not care.

CREATE VIEW IF NOT EXISTS clusters.clusters_5m AS
SELECT
    exchange,
    market_type,
    quote,
    symbol,
    toStartOfInterval(window_start, INTERVAL 5 MINUTE) AS window_start,
    price,
    any(price_scale) AS price_scale,
    sum(bid_qty) AS bid_qty,
    sum(ask_qty) AS ask_qty,
    sum(trades) AS trades,
    any(qty_scale) AS qty_scale,
    any(ingest_region) AS ingest_region
FROM clusters.clusters_1m FINAL
GROUP BY exchange, market_type, quote, symbol, window_start, price;

CREATE VIEW IF NOT EXISTS clusters.clusters_15m AS
SELECT
    exchange,
    market_type,
    quote,
    symbol,
    toStartOfInterval(window_start, INTERVAL 15 MINUTE) AS window_start,
    price,
    any(price_scale) AS price_scale,
    sum(bid_qty) AS bid_qty,
    sum(ask_qty) AS ask_qty,
    sum(trades) AS trades,
    any(qty_scale) AS qty_scale,
    any(ingest_region) AS ingest_region
FROM clusters.clusters_1m FINAL
GROUP BY exchange, market_type, quote, symbol, window_start, price;

CREATE VIEW IF NOT EXISTS clusters.clusters_1h AS
SELECT
    exchange,
    market_type,
    quote,
    symbol,
    toStartOfHour(window_start) AS window_start,
    price,
    any(price_scale) AS price_scale,
    sum(bid_qty) AS bid_qty,
    sum(ask_qty) AS ask_qty,
    sum(trades) AS trades,
    any(qty_scale) AS qty_scale,
    any(ingest_region) AS ingest_region
FROM clusters.clusters_1m FINAL
GROUP BY exchange, market_type, quote, symbol, window_start, price;

CREATE VIEW IF NOT EXISTS clusters.clusters_4h AS
SELECT
    exchange,
    market_type,
    quote,
    symbol,
    toStartOfInterval(window_start, INTERVAL 4 HOUR) AS window_start,
    price,
    any(price_scale) AS price_scale,
    sum(bid_qty) AS bid_qty,
    sum(ask_qty) AS ask_qty,
    sum(trades) AS trades,
    any(qty_scale) AS qty_scale,
    any(ingest_region) AS ingest_region
FROM clusters.clusters_1m FINAL
GROUP BY exchange, market_type, quote, symbol, window_start, price;

CREATE VIEW IF NOT EXISTS clusters.clusters_1d AS
SELECT
    exchange,
    market_type,
    quote,
    symbol,
    toStartOfDay(window_start) AS window_start,
    price,
    any(price_scale) AS price_scale,
    sum(bid_qty) AS bid_qty,
    sum(ask_qty) AS ask_qty,
    sum(trades) AS trades,
    any(qty_scale) AS qty_scale,
    any(ingest_region) AS ingest_region
FROM clusters.clusters_1m FINAL
GROUP BY exchange, market_type, quote, symbol, window_start, price;
