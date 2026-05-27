-- Add per-window OHLC columns to all clusters_<tf> tables.
--
-- Why per-row instead of separate "headers" table:
--   - Single-table read path: cluster_history.rs query stays one SELECT
--     with `any(open) GROUP BY window_start` (each row of a window has
--     the same OHLC value, so `any` is correct).
--   - LowCardinality compression in CH means the repeated OHLC values
--     don't bloat on-disk size meaningfully (typical 5m bar has 30-60
--     buckets — 4 × 8 bytes × 30 = ~1KB redundancy per window, gzip
--     wire-side handles it fine).
--   - Migration safety: ALTER TABLE ... ADD COLUMN ... DEFAULT 0 is
--     online in CH and PRESERVES ALL EXISTING DATA. Legacy rows return 0
--     for these new columns. UI renders open=close=0 as "no candle body"
--     (just heatmap), which is acceptable degradation for pre-migration
--     historical data.
--
-- Run via `clickhouse-client < 004_add_ohlc.sql` or HTTP
-- `POST /?query=<each-statement>` in sequence.

ALTER TABLE clusters.clusters_30s ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_1m  ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_5m  ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_15m ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_30m ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_1h  ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_4h  ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;

ALTER TABLE clusters.clusters_1d  ADD COLUMN IF NOT EXISTS open Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS close Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS high Int64 DEFAULT 0,
                                  ADD COLUMN IF NOT EXISTS low Int64 DEFAULT 0;
