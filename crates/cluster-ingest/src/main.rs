use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clickhouse_sink::{ChWriter, ChWriterConfig};
use cluster_engine::ClusterBus;
use exchange_binance::BinanceFuturesInfo;
use tokio::sync::{mpsc, watch};

mod binance_session;
mod binance_supervisor;
mod config;

use binance_supervisor::BinanceSupervisor;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-ingest");

    let cfg = match std::env::var("INGEST_CONFIG").ok().map(PathBuf::from) {
        Some(path) => {
            tracing::info!(path = %path.display(), "loading config");
            Config::load(&path)?
        }
        None => {
            tracing::info!("INGEST_CONFIG not set; using defaults");
            Config::default()
        }
    };

    // Env-var overrides for the most common dev knobs.
    let mut ingest = cfg.ingest;
    if let Ok(region) = std::env::var("INGEST_REGION") {
        ingest.region = region;
    }
    if let Ok(url) = std::env::var("CH_URL") {
        ingest.clickhouse_url = url;
    }
    let mut binance_cfg = ingest.exchanges.binance_perp.clone().unwrap_or_default();
    if let Ok(top_n_str) = std::env::var("INGEST_TOP_N") {
        if let Ok(n) = top_n_str.parse() {
            binance_cfg.top_n = Some(n);
        }
    }

    let bus = Arc::new(ClusterBus::new());
    let (ch_tx, ch_rx) = mpsc::channel(ingest.ch_channel_bound);

    let ch_writer = ChWriter::new(ChWriterConfig {
        url: ingest.clickhouse_url.clone(),
        ..ChWriterConfig::default()
    })
    .context("build ChWriter")?;
    let writer_handle = tokio::spawn(async move {
        match ch_writer.run(ch_rx).await {
            Ok(stats) => tracing::info!(?stats, "ch writer ended"),
            Err(e) => tracing::error!(error = %e, "ch writer crashed"),
        }
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let supervisor_task = if binance_cfg.enabled {
        let info = Arc::new(BinanceFuturesInfo::new());
        let supervisor = BinanceSupervisor::new(
            info,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            binance_cfg,
            ch_tx.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }))
    } else {
        tracing::info!("binance_perp disabled in config; supervisor not started");
        None
    };

    // Drop the local copy so once supervisor's spawn_symbol clones are
    // also dropped (during shutdown), the writer's receiver closes and
    // writer_handle returns cleanly.
    drop(ch_tx);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received; signalling shutdown");
        }
        r = writer_handle => {
            tracing::warn!(result = ?r, "writer task ended unexpectedly");
        }
    }

    let _ = shutdown_tx.send(true);
    if let Some(t) = supervisor_task {
        let _ = t.await;
    }

    Ok(())
}
