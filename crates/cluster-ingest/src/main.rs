use anyhow::Result;

// `binance_session` is exercised by integration tests now and will be wired
// into the orchestration layer in the next slice (pool + reconnect).
#[allow(dead_code)]
mod binance_session;

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-ingest");
    tracing::info!("cluster-ingest phase 1 — binance perp session ready, orchestration TBD");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    Ok(())
}
