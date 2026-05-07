use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-api");
    tracing::info!(
        "cluster-api binary stub: live streaming runs inside cluster-ingest; \
         this binary will host the historical query (REST/CH) endpoints in a later slice."
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");
    Ok(())
}
