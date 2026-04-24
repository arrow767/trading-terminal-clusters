use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-api");
    tracing::info!("cluster-api phase 0 skeleton — http server not wired yet");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    Ok(())
}
