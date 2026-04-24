use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-ingest");
    tracing::info!("cluster-ingest phase 0 skeleton — exchange adapters not wired yet");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    Ok(())
}
