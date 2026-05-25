//! Reference gRPC client for the ClusterStream service. Run a
//! `cluster-ingest` instance, then point this CLI at its `grpc_listen`
//! address — it subscribes to one symbol and prints every Snapshot/Diff
//! frame as it arrives.
//!
//! Doubles as the canonical example for whoever writes the
//! `ClusterRemoteClient` integration in fat-terminal:
//! 1. open ClusterStreamClient::connect
//! 2. send SubscribeRequest with the symbol(s) of interest
//! 3. consume the bidirectional `Streaming<Frame>`
//! 4. on the wire, prices and quantities are scaled i64 — multiply by
//!    10^-spec.price_scale / 10^-spec.qty_scale to render
//!
//! Env knobs:
//!   CLUSTER_GRPC=http://127.0.0.1:50051
//!   CLUSTER_TOKEN=<bearer>  Bearer-токен для auth-protected серверов.
//!                           На локальном dev без auth — оставь пустым.
//!   EXCHANGE=BINANCEF       (Exchange::wire_id())
//!   MARKET_TYPE=PERP        ("PERP" or "SPOT")
//!   SYMBOL=BTCUSDT,ETHUSDT  (comma-separated, all on the same exchange)
//!   INTERVAL=60             Таймфрейм в секундах: 30, 60, 300, 900, 3600, 14400, 86400.
//!                           Должен совпадать с одним из `timeframes_secs` сервера.

use anyhow::{anyhow, Context, Result};
use cluster_api::proto;
use cluster_api::proto::cluster_stream_client::ClusterStreamClient;

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint =
        std::env::var("CLUSTER_GRPC").unwrap_or_else(|_| "http://127.0.0.1:50051".into());
    let exchange = std::env::var("EXCHANGE").unwrap_or_else(|_| "BINANCEF".into());
    let market_type = std::env::var("MARKET_TYPE").unwrap_or_else(|_| "PERP".into());
    let symbols_csv = std::env::var("SYMBOL").unwrap_or_else(|_| "BTCUSDT".into());
    let symbols: Vec<proto::SymbolKey> = symbols_csv
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| proto::SymbolKey {
            exchange: exchange.clone(),
            market_type: market_type.clone(),
            symbol: s.to_string(),
        })
        .collect();
    if symbols.is_empty() {
        return Err(anyhow!("no symbols specified"));
    }

    let interval_seconds: u32 = std::env::var("INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    println!(
        "connecting to {endpoint}; subscribing to {}/{} {:?} @ {}s (gzip)",
        exchange,
        market_type,
        symbols.iter().map(|s| &s.symbol).collect::<Vec<_>>(),
        interval_seconds
    );

    let token = std::env::var("CLUSTER_TOKEN").ok().filter(|t| !t.is_empty());

    // gzip — снижает per-frame size на типичных снапшотах (много i64)
    // в 3-5 раз. Сервер уже передаёт gzip; клиент должен явно сказать
    // что принимает (accept_compressed) и хочет слать сжатое (send_compressed,
    // мелочь для subscribe-запроса, но включаем для симметрии).
    let mut client = ClusterStreamClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect to {endpoint}"))?
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

    let mut req = tonic::Request::new(proto::SubscribeRequest {
        symbols,
        interval_seconds,
    });
    if let Some(t) = &token {
        let header = format!("Bearer {t}");
        req.metadata_mut().insert(
            "authorization",
            header
                .parse()
                .context("invalid CLUSTER_TOKEN (must be ASCII)")?,
        );
    }
    let mut stream = client
        .subscribe(req)
        .await
        .context("Subscribe call rejected")?
        .into_inner();

    while let Some(frame) = stream.message().await.context("recv frame")? {
        let key = frame.key.unwrap_or_default();
        match frame.body {
            Some(proto::frame::Body::Snapshot(s)) => {
                println!(
                    "[{}] SNAPSHOT window={} seq={} buckets={}",
                    key.symbol,
                    s.window_start_ns,
                    s.sequence,
                    s.clusters.len()
                );
            }
            Some(proto::frame::Body::Diff(d)) => {
                println!(
                    "[{}] DIFF window={} seq={} upserts={} removes={}",
                    key.symbol,
                    d.window_start_ns,
                    d.sequence,
                    d.upserts.len(),
                    d.removes.len()
                );
            }
            None => {
                eprintln!("[{}] frame with no body — protocol violation", key.symbol);
            }
        }
    }
    println!("server closed stream; exiting");
    Ok(())
}
