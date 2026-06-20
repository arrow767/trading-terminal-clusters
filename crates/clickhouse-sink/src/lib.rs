//! ClickHouse sink for closing-window cluster snapshots.
//!
//! `rows_from_snapshot` flattens a per-symbol `AnalyticsSnapshot` (the
//! closing snapshot of a 1-minute window, emitted by `Aggregator` on
//! window roll) into one `ClusterRow` per price bucket. `ChWriter`
//! batches those rows and pushes them into ClickHouse via the HTTP
//! interface using `INSERT ... FORMAT JSONEachRow`.
//!
//! Why HTTP + JSONEachRow instead of the native protocol: the rate is
//! modest (a few hundred symbols × tens of price levels per minute is
//! well under 100k rows/min total), and HTTP keeps the dependency tree
//! small — we already pull `reqwest` for exchange REST clients. We can
//! switch to the native protocol later if write throughput becomes a
//! bottleneck.

use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use exchange_core::{AnalyticsSnapshot, MarketType, Quote, SymbolSpec};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

#[derive(Debug, Clone, Serialize)]
pub struct ClusterRow {
    pub exchange: &'static str,
    pub market_type: &'static str,
    pub quote: &'static str,
    pub symbol: String,
    /// Window start as integer ms since epoch — CH parses this directly
    /// into `DateTime64(3, 'UTC')` when input_format_date_time_input_format
    /// is left at its default.
    pub window_start: i64,
    pub price: i64,
    pub price_scale: u8,
    pub bid_qty: i64,
    pub ask_qty: i64,
    pub trades: u32,
    pub qty_scale: u8,
    pub ingest_region: String,
    /// OHLC окна (scaled i64). Повторяется для всех buckets одного
    /// window'а — на write-стороне дешево, ColumnsLowCardinality в CH
    /// хорошо жмёт повторяющиеся значения. Per-window query на read-стороне
    /// делается через `any(open) GROUP BY window_start`. Если миграция
    /// прошла на старой таблице — для legacy строк эти поля = 0
    /// (рендер UI трактует open=close=0 как «нет candle body»).
    pub open: i64,
    pub close: i64,
    pub high: i64,
    pub low: i64,
}

pub fn rows_from_snapshot(
    snap: &AnalyticsSnapshot,
    spec: &SymbolSpec,
    region: &str,
) -> Vec<ClusterRow> {
    let market_type = match spec.market_type {
        MarketType::Spot => "spot",
        MarketType::Perp => "perp",
    };
    let quote = match spec.quote {
        Quote::Usdt => "USDT",
        Quote::Usdc => "USDC",
    };
    let window_start_ms = snap.window_start_ns / 1_000_000;
    snap.clusters
        .iter()
        .map(|b| ClusterRow {
            exchange: spec.exchange.wire_id(),
            market_type,
            quote,
            symbol: spec.symbol.clone(),
            window_start: window_start_ms,
            price: b.price,
            price_scale: spec.price_scale,
            bid_qty: b.bid_qty,
            ask_qty: b.ask_qty,
            trades: b.trades,
            qty_scale: spec.qty_scale,
            ingest_region: region.to_string(),
            // OHLC окна — одинаков для всех buckets этого snapshot'а.
            open: snap.open,
            close: snap.close,
            high: snap.high,
            low: snap.low,
        })
        .collect()
}

/// Serialize a batch of rows in JSONEachRow format: one JSON object per
/// line, no surrounding array, no trailing newline. CH is strict about
/// this — a leading `[` will fail the insert.
pub fn serialize_jsoneachrow(rows: &[ClusterRow]) -> Result<String> {
    let mut out = String::with_capacity(rows.len() * 200);
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let line = serde_json::to_string(row).context("serialize row")?;
        out.push_str(&line);
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct ChWriterConfig {
    pub url: String,
    pub database: String,
    pub table: String,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub request_timeout: Duration,
    /// Hard cap on rows held in the in-memory batch. If ClickHouse is slow/down
    /// and rows pile up past this, the OLDEST are dropped (counted + warned) so
    /// the writer can't grow without bound and OOM the box. The freshest windows
    /// survive; durability for the dropped tail is the WAL's job (when wired).
    pub max_buffer_rows: usize,
}

impl Default for ChWriterConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8123".into(),
            database: "clusters".into(),
            table: "clusters_1m".into(),
            batch_size: 5000,
            flush_interval: Duration::from_secs(2),
            request_timeout: Duration::from_secs(15),
            max_buffer_rows: 300_000, // ~30 MB/writer — absorbs a multi-minute local-CH outage in RAM
        }
    }
}

/// Upper bound on the failure backoff between flush attempts. On repeated CH
/// insert failures the retry interval doubles up to this, so a down ClickHouse
/// is not hammered every `flush_interval` and `recv` stays responsive.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub struct ChWriter {
    cfg: ChWriterConfig,
    client: reqwest::Client,
}

impl ChWriter {
    pub fn new(cfg: ChWriterConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .context("build reqwest client")?;
        Ok(Self { cfg, client })
    }

    pub fn config(&self) -> &ChWriterConfig {
        &self.cfg
    }

    /// Выполнить произвольный DDL-запрос (например, `ALTER TABLE … MODIFY TTL …`)
    /// через тот же HTTP-клиент. Используется ingest-startup'ом чтобы
    /// синхронизировать TTL таблицы с RetentionConfig — менять retention
    /// без миграций / cron'а / ручного захода в ClickHouse.
    ///
    /// На пустую/успешную HTTP-200 ClickHouse возвращает пустое тело —
    /// мы это нормально обрабатываем. На ошибку отдаём детальное тело,
    /// чтобы оператор сразу увидел причину (например `Code: 159, e.what()
    /// = DB::Exception: Cannot parse TTL expression`).
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let url = self.cfg.url.trim_end_matches('/').to_string();
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(sql.to_string())
            .send()
            .await
            .context("ch http send (ddl)")?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(anyhow!("CH HTTP {status} on DDL: {detail}"));
        }
        Ok(())
    }

    /// Pull rows off `rx`, batch them, and POST to ClickHouse. Returns
    /// when the channel closes, after flushing any in-flight batch.
    /// On a flush error, the batch is **kept** rather than dropped — the
    /// next tick will retry. The caller controls retry budget by closing
    /// the channel if the failure is unrecoverable.
    pub async fn run(&self, mut rx: mpsc::Receiver<ClusterRow>) -> Result<WriterStats> {
        let mut batch: Vec<ClusterRow> = Vec::with_capacity(self.cfg.batch_size);
        let mut ticker = interval(self.cfg.flush_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut stats = WriterStats::default();
        // Backoff gate: on CH failure `next_attempt` is pushed into the future so
        // we neither re-POST a doomed insert every flush_interval nor block `recv`
        // on a 15s POST that will fail — recv keeps draining (accumulate + cap).
        // Reset on success.
        let mut next_attempt = Instant::now();
        let mut backoff = self.cfg.flush_interval;
        let mut last_drop_warn: Option<Instant> = None;

        loop {
            tokio::select! {
                biased;
                maybe_row = rx.recv() => {
                    match maybe_row {
                        Some(row) => {
                            batch.push(row);
                            // Bound writer memory: if CH is slow/down and rows pile up,
                            // drop the OLDEST (freshest windows survive) + count/warn.
                            if batch.len() > self.cfg.max_buffer_rows {
                                let drop_n = batch.len() - self.cfg.max_buffer_rows;
                                batch.drain(0..drop_n);
                                stats.rows_dropped += drop_n as u64;
                                let now = Instant::now();
                                if last_drop_warn.map_or(true, |t| now.duration_since(t) >= Duration::from_secs(5)) {
                                    tracing::warn!(
                                        table = %self.cfg.table,
                                        rows_dropped_total = stats.rows_dropped,
                                        "ch writer buffer full (CH slow/down?); dropping oldest rows",
                                    );
                                    last_drop_warn = Some(now);
                                }
                            }
                            if batch.len() >= self.cfg.batch_size && Instant::now() >= next_attempt {
                                self.try_flush(&mut batch, &mut stats, &mut next_attempt, &mut backoff).await;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                self.try_flush(&mut batch, &mut stats, &mut next_attempt, &mut backoff).await;
                            }
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !batch.is_empty() && Instant::now() >= next_attempt {
                        self.try_flush(&mut batch, &mut stats, &mut next_attempt, &mut backoff).await;
                    }
                }
            }
        }

        Ok(stats)
    }

    async fn try_flush(
        &self,
        batch: &mut Vec<ClusterRow>,
        stats: &mut WriterStats,
        next_attempt: &mut Instant,
        backoff: &mut Duration,
    ) {
        match self.flush_once(batch).await {
            Ok(()) => {
                stats.batches_ok += 1;
                stats.rows_ok += batch.len() as u64;
                batch.clear();
                *backoff = self.cfg.flush_interval;
                *next_attempt = Instant::now();
            }
            Err(e) => {
                stats.batches_err += 1;
                tracing::warn!(error = %e, batch_len = batch.len(), "ch insert failed; retry after backoff");
                *backoff = (*backoff * 2).min(BACKOFF_MAX);
                *next_attempt = Instant::now() + *backoff;
            }
        }
    }

    async fn flush_once(&self, batch: &[ClusterRow]) -> Result<()> {
        let body = serialize_jsoneachrow(batch)?;
        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            self.cfg.database, self.cfg.table
        );
        let url = format!(
            "{}/?query={}",
            self.cfg.url.trim_end_matches('/'),
            urlencode(&query)
        );
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .context("ch http send")?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(anyhow!("CH HTTP {status}: {detail}"));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WriterStats {
    pub batches_ok: u64,
    pub batches_err: u64,
    pub rows_ok: u64,
    /// Rows dropped because the batch hit `max_buffer_rows` (CH slow/down).
    pub rows_dropped: u64,
}

/// Minimal application/x-www-form-urlencoded encoder for the query
/// parameter. Avoids pulling `urlencoding` just for this — the input is
/// fixed-shape SQL with predictable characters (spaces, dots).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use exchange_core::{
        AnalyticsSnapshot, ClusterBucket, Exchange, MarketType, Quote, SymbolSpec,
    };

    use super::*;

    fn spec(symbol: &str) -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::BinanceF,
            market_type: MarketType::Perp,
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 2,
            qty_scale: 3,
            tick_size: 10,
            step_size: 1,
        }
    }

    fn snapshot(window_start_ns: i64, buckets: Vec<ClusterBucket>) -> AnalyticsSnapshot {
        AnalyticsSnapshot {
            window_start_ns,
            sequence: 1,
            clusters: buckets,
            ..Default::default()
        }
    }

    #[test]
    fn rows_from_snapshot_one_per_bucket() {
        let s = snapshot(
            60_000_000_000, // 60s in ns → 60_000 ms
            vec![
                ClusterBucket {
                    price: 100,
                    bid_qty: 5,
                    ask_qty: 0,
                    trades: 1,
                },
                ClusterBucket {
                    price: 110,
                    bid_qty: 0,
                    ask_qty: 3,
                    trades: 2,
                },
            ],
        );
        let rows = rows_from_snapshot(&s, &spec("BTCUSDT"), "tokyo");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "BINANCEF");
        assert_eq!(rows[0].market_type, "perp");
        assert_eq!(rows[0].quote, "USDT");
        assert_eq!(rows[0].symbol, "BTCUSDT");
        assert_eq!(rows[0].window_start, 60_000);
        assert_eq!(rows[0].price, 100);
        assert_eq!(rows[0].price_scale, 2);
        assert_eq!(rows[0].bid_qty, 5);
        assert_eq!(rows[0].qty_scale, 3);
        assert_eq!(rows[0].ingest_region, "tokyo");

        assert_eq!(rows[1].price, 110);
        assert_eq!(rows[1].ask_qty, 3);
        assert_eq!(rows[1].trades, 2);
    }

    #[test]
    fn jsoneachrow_format_is_one_object_per_line_no_array() {
        let s = snapshot(
            0,
            vec![
                ClusterBucket {
                    price: 100,
                    bid_qty: 1,
                    ask_qty: 0,
                    trades: 1,
                },
                ClusterBucket {
                    price: 200,
                    bid_qty: 0,
                    ask_qty: 2,
                    trades: 1,
                },
            ],
        );
        let rows = rows_from_snapshot(&s, &spec("BTCUSDT"), "tokyo");
        let body = serialize_jsoneachrow(&rows).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(!body.starts_with('['), "must not be a JSON array");
        assert!(!body.ends_with('\n'), "must not have trailing newline");

        // Each line must be a parseable JSON object.
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v.is_object());
        }
    }

    #[test]
    fn urlencode_handles_sql_punctuation() {
        let q = "INSERT INTO clusters.clusters_1m FORMAT JSONEachRow";
        let enc = urlencode(q);
        // Spaces become %20, dots stay literal.
        assert!(enc.contains("INSERT%20INTO"));
        assert!(enc.contains("clusters.clusters_1m"));
    }

    /// Smoke: ChWriter::run drains a closed channel without panicking,
    /// flushing the residual batch (which may fail since no CH server
    /// is up — `try_flush` swallows the error and bumps batches_err).
    #[tokio::test]
    async fn run_drains_on_close_and_records_stats() {
        let cfg = ChWriterConfig {
            url: "http://127.0.0.1:1".into(), // unreachable
            batch_size: 100,
            flush_interval: Duration::from_millis(50),
            request_timeout: Duration::from_millis(200),
            ..Default::default()
        };
        let writer = ChWriter::new(cfg).unwrap();
        let (tx, rx) = mpsc::channel(8);
        let s = snapshot(
            0,
            vec![ClusterBucket {
                price: 1,
                bid_qty: 1,
                ask_qty: 0,
                trades: 1,
            }],
        );
        let rows = rows_from_snapshot(&s, &spec("BTCUSDT"), "tokyo");
        for r in rows {
            tx.send(r).await.unwrap();
        }
        drop(tx);
        let stats = writer.run(rx).await.unwrap();
        assert_eq!(stats.batches_ok, 0);
        assert!(stats.batches_err >= 1);
        let _ = Arc::new(stats); // keep
    }
}
