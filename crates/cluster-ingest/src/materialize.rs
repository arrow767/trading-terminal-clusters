//! Periodic materializer: rolls up `clusters_1m` → `clusters_{5m,15m,30m,1h}`
//! for the major venues (Binance/OKX/Bybit) so the range API can read those
//! TFs as a direct table scan instead of an on-read rollup (≈2× faster on the
//! heavy long-TF requests). 4h/1d stay rollup-on-read (cheap enough, and
//! keeping their open window materialized would cost continuous CH work).
//!
//! Cost shape: a one-time backfill of the retention window (heavy, ~minutes,
//! runs in ClickHouse — NOT ingest RAM), then a cheap 60s refresh that
//! re-rolls only the recent window (open + just-closed bars, catching late
//! updates). ReplacingMergeTree(ingested_at) makes re-inserts idempotent
//! (a fuller rollup of the same (window, price) replaces the older one).
//!
//! Until the backfill finishes, `CLUSTERS_MATERIALIZED_READY` stays false and
//! the range API falls back to rollup — so a fresh deploy never serves a
//! half-filled table.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use tokio::sync::watch;

/// Venues whose long TFs we materialize. MUST match
/// `cluster_api::cluster_history::MATERIALIZED_EXCHANGES`.
const EXCHANGES_SQL: &str = "'BINANCE','BINANCEF','OKX','OKXF','BYBIT','BYBITF'";
/// TFs we materialize (5m/15m/30m/1h). MUST match the range handler.
const TFS: [u32; 4] = [300, 900, 1800, 3600];
/// Backfill depth — matches clusters_1m retention (7d), so a direct table read
/// covers all available history (no rollup-fallback / UNION needed).
const BACKFILL_DAYS: i64 = 7;
const REFRESH_SECS: u64 = 60;

fn table_for(tf: u32) -> &'static str {
    match tf {
        300 => "clusters_5m",
        900 => "clusters_15m",
        1800 => "clusters_30m",
        3600 => "clusters_1h",
        _ => "clusters_1m",
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn run_materializer(
    ch_url: String,
    ch_db: String,
    ch_user: String,
    ch_password: String,
    mut shutdown: watch::Receiver<bool>,
) {
    // Dedicated client: the backfill INSERT…SELECT can run for minutes, far
    // beyond the shared REST→CH client's 15s timeout.
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "materialize: client build failed — disabled");
            return;
        }
    };

    // Let discovery + the sink settle before the heavy one-time backfill.
    tokio::time::sleep(Duration::from_secs(20)).await;
    if *shutdown.borrow() {
        return;
    }

    let now = now_ms();
    for &tf in &TFS {
        let from = now - BACKFILL_DAYS * 86_400_000;
        match materialize(&client, &ch_url, &ch_db, &ch_user, &ch_password, tf, from, now).await {
            Ok(()) => tracing::info!(tf, "materialize: backfill done"),
            Err(e) => tracing::warn!(tf, error = %e, "materialize: backfill failed (refresh will retry)"),
        }
        if *shutdown.borrow() {
            return;
        }
    }
    cluster_api::cluster_history::CLUSTERS_MATERIALIZED_READY.store(true, Ordering::Relaxed);
    tracing::info!("materialize: READY — serving materialized 5m/15m/30m/1h for binance/okx/bybit");

    let mut tick = tokio::time::interval(Duration::from_secs(REFRESH_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = tick.tick() => {
                let now = now_ms();
                for &tf in &TFS {
                    // Re-roll only the recent window: open + just-closed bars
                    // (≥2×TF, min 15m) — cheap vs the one-time backfill.
                    let refresh_ms = ((tf as i64) * 2).max(900) * 1000;
                    let from = now - refresh_ms;
                    if let Err(e) = materialize(&client, &ch_url, &ch_db, &ch_user, &ch_password, tf, from, now).await {
                        tracing::warn!(tf, error = %e, "materialize: refresh failed");
                    }
                }
            }
        }
    }
    tracing::info!("materialize: shutdown");
}

/// One rollup INSERT…SELECT for a TF over `[from_ms, to_ms)`. `from` is aligned
/// down to the TF boundary so no rolled window is partial.
async fn materialize(
    client: &reqwest::Client,
    ch_url: &str,
    ch_db: &str,
    ch_user: &str,
    ch_password: &str,
    tf: u32,
    from_ms: i64,
    to_ms: i64,
) -> Result<()> {
    let tf_ms = (tf as i64) * 1000;
    let from_aligned = (from_ms / tf_ms) * tf_ms;
    let table = table_for(tf);
    let sql = format!(
        "INSERT INTO {db}.{table} \
         (exchange,market_type,quote,symbol,window_start,price,price_scale,bid_qty,ask_qty,trades,qty_scale,ingest_region,open,close,high,low) \
         SELECT b.exchange,b.market_type,b.quote,b.symbol,b.rw,b.price,b.ps,b.bid_qty,b.ask_qty,b.trades,b.qs,'rollup',w.o,w.c,w.h,w.l \
         FROM ( \
           SELECT exchange,market_type,quote,symbol, \
             toStartOfInterval(window_start, INTERVAL {tf} SECOND, 'UTC') AS rw, price, \
             sum(bid_qty) AS bid_qty, sum(ask_qty) AS ask_qty, sum(trades) AS trades, \
             any(price_scale) AS ps, any(qty_scale) AS qs \
           FROM {db}.clusters_1m FINAL \
           WHERE exchange IN ({ex}) \
             AND window_start >= fromUnixTimestamp64Milli({from}) AND window_start < fromUnixTimestamp64Milli({to}) \
           GROUP BY exchange,market_type,quote,symbol,rw,price \
         ) AS b \
         INNER JOIN ( \
           SELECT exchange,market_type,quote,symbol, \
             toStartOfInterval(window_start, INTERVAL {tf} SECOND, 'UTC') AS rw, \
             argMin(open,window_start) AS o, argMax(close,window_start) AS c, max(high) AS h, min(low) AS l \
           FROM {db}.clusters_1m FINAL \
           WHERE exchange IN ({ex}) \
             AND window_start >= fromUnixTimestamp64Milli({from}) AND window_start < fromUnixTimestamp64Milli({to}) \
           GROUP BY exchange,market_type,quote,symbol,rw \
         ) AS w \
         ON b.exchange=w.exchange AND b.market_type=w.market_type AND b.quote=w.quote AND b.symbol=w.symbol AND b.rw=w.rw",
        db = ch_db, table = table, tf = tf, ex = EXCHANGES_SQL, from = from_aligned, to = to_ms,
    );
    // Bound CH resource use so materialization never starves live inserts/reads.
    let url = format!(
        "{}/?max_threads=4&max_execution_time=600",
        ch_url.trim_end_matches('/')
    );
    let mut req = client.post(&url).body(sql);
    if !ch_user.is_empty() {
        req = req.basic_auth(ch_user, Some(ch_password));
    }
    let resp = req.send().await.context("materialize POST")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("CH {status}: {}", body.chars().take(300).collect::<String>());
    }
    Ok(())
}
