use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clickhouse_sink::{rows_from_snapshot, ChWriter, ChWriterConfig};
use cluster_engine::{run_aggregator, Aggregator, ClusterBus};
use exchange_binance::{BinanceFuturesInfo, BinanceFuturesWs};
use exchange_core::{ClusterFrame, ExchangeInfo, SymbolKey, SymbolSpec, TradePrint};
use tokio::sync::mpsc;

mod binance_session;
use binance_session::{run_session, SymbolRoute};

const TRADE_CHANNEL_BOUND: usize = 4_096;
const CH_CHANNEL_BOUND: usize = 16_384;
const AGG_TICK_INTERVAL: Duration = Duration::from_millis(100);
const WINDOW_MS: i64 = 60_000;
const DIFF_INTERVAL_MS: i64 = 200;
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(500);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-ingest");

    let region = std::env::var("INGEST_REGION").unwrap_or_else(|_| "tokyo".into());
    let top_n: usize = std::env::var("INGEST_TOP_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let ch_url = std::env::var("CH_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());

    let specs = fetch_top_n(top_n).await?;
    tracing::info!(count = specs.len(), region = %region, "binance perp ingest starting");

    let bus = Arc::new(ClusterBus::new());
    let (ch_tx, ch_rx) = mpsc::channel(CH_CHANNEL_BOUND);

    let ch_writer = ChWriter::new(ChWriterConfig {
        url: ch_url,
        ..ChWriterConfig::default()
    })
    .context("build ChWriter")?;
    let writer_handle = tokio::spawn(async move {
        match ch_writer.run(ch_rx).await {
            Ok(stats) => tracing::info!(?stats, "ch writer ended"),
            Err(e) => tracing::error!(error = %e, "ch writer crashed"),
        }
    });

    let mut routes: Vec<SymbolRoute> = Vec::with_capacity(specs.len());
    for spec in &specs {
        let key = SymbolKey::new(spec.exchange, spec.market_type, spec.symbol.as_str());

        let (trade_tx, trade_rx) = mpsc::channel::<TradePrint>(TRADE_CHANNEL_BOUND);
        let agg = Aggregator::new(key.clone(), spec.tick_size, WINDOW_MS, DIFF_INTERVAL_MS);
        let bus_for_agg = Arc::clone(&bus);
        tokio::spawn(async move {
            run_aggregator(agg, trade_rx, bus_for_agg, AGG_TICK_INTERVAL).await;
        });

        spawn_snapshot_to_ch(&bus, &key, spec.clone(), region.clone(), ch_tx.clone());

        routes.push(SymbolRoute {
            spec: spec.clone(),
            sink: trade_tx,
        });
    }
    // Drop our own ch_tx so once every per-symbol fan-out task exits the
    // writer's receiver closes and writer_handle returns cleanly.
    drop(ch_tx);

    let connector = BinanceFuturesWs::new();
    let session_loop = tokio::spawn(async move {
        let mut backoff = RECONNECT_BACKOFF_MIN;
        loop {
            match run_session(&connector, &routes, WS_CONNECT_TIMEOUT).await {
                Ok(stats) => {
                    tracing::info!(?stats, "session ended; reconnecting");
                    backoff = RECONNECT_BACKOFF_MIN;
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?backoff, "session error; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                }
            }
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received; shutting down");
        }
        r = writer_handle => {
            tracing::warn!(result = ?r, "writer task ended unexpectedly");
        }
    }
    session_loop.abort();
    Ok(())
}

async fn fetch_top_n(n: usize) -> Result<Vec<SymbolSpec>> {
    let info = BinanceFuturesInfo::new();
    let all = info
        .fetch_symbols()
        .await
        .map_err(|e| anyhow!("fetch_symbols: {e}"))?;
    if all.is_empty() {
        return Err(anyhow!("binance returned zero tradeable perps"));
    }
    // exchangeInfo doesn't carry volume — for the MVP we just take the
    // first N (alphabetical-ish from the API). A later slice will rank by
    // 24h notional from `/fapi/v1/ticker/24hr`.
    Ok(all.into_iter().take(n.max(1)).collect())
}

fn spawn_snapshot_to_ch(
    bus: &Arc<ClusterBus>,
    key: &SymbolKey,
    spec: SymbolSpec,
    region: String,
    ch_tx: mpsc::Sender<clickhouse_sink::ClusterRow>,
) {
    let mut sub = bus.subscribe(key);
    tokio::spawn(async move {
        loop {
            match sub.recv().await {
                Ok(ClusterFrame::Snapshot(s)) => {
                    let rows = rows_from_snapshot(&s, &spec, &region);
                    for row in rows {
                        if ch_tx.send(row).await.is_err() {
                            return;
                        }
                    }
                }
                Ok(ClusterFrame::Diff(_)) => {
                    // Diffs are streamed live to clients via gRPC (next
                    // slice). They are not persisted — only the closing
                    // snapshot of a window goes to ClickHouse.
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(lagged = n, symbol = %spec.symbol, "ch fanout lagged behind bus");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}
