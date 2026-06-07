//! Aster (asterdex.com) `/ws` SUBSCRIBE session — SHARDED across connections.
//!
//! Aster — клон Binance, поэтому envelope трейдов и парсер те же
//! (`BinanceFuturesTradeParser` принимает raw `/ws` aggTrade). Два отличия
//! от `binance_session`, продиктованные лимитами Aster `/ws`:
//!   1. Один SUBSCRIBE с >~100 params → close `3001` (illegal request).
//!   2. >~N каналов на ОДИН коннект → close `3003` (channels exceeds limit).
//! Поэтому дробим universe на ШАРДЫ ≤100 символов и держим по одному
//! WS-коннекту на шард (futures perp ~455 → ~5 коннектов). Каждый шард сам
//! реконнектится; route-change/shutdown отменяет весь набор (futures не
//! spawn'ятся — они часть этого future, см. supervisor select).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_aster::AsterWs;
use exchange_binance::BinanceFuturesTradeParser;
use exchange_core::{SymbolSpec, WsConnector};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

/// Символов на один WS-коннект. Под лимитом Aster `/ws` (≥120 ok, <455).
const SHARD_SIZE: usize = 100;

pub async fn run_session(
    connector: &AsterWs,
    routes: &[SymbolRoute],
    connect_timeout: Duration,
) -> Result<SessionStats> {
    if routes.is_empty() {
        return Err(anyhow!("run_session called with no routes"));
    }

    let shards: Vec<Vec<SymbolRoute>> = routes.chunks(SHARD_SIZE).map(|c| c.to_vec()).collect();
    tracing::info!(
        symbols = routes.len(),
        shards = shards.len(),
        "aster session: sharding across WS connections"
    );

    // Один reconnect-loop на шард, все конкурентно как ЧАСТЬ этого future
    // (не tokio::spawn — иначе route-change их не отменит). join_all не
    // вернётся (шарды крутятся вечно), пока supervisor не отменит future.
    let futs = shards.iter().enumerate().map(|(idx, shard)| {
        let shard = shard.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_one_shard(connector, &shard, connect_timeout, idx).await {
                    Ok(()) => backoff = Duration::from_millis(500),
                    Err(e) => {
                        tracing::warn!(shard = idx, error = %e, "aster shard error; reconnecting");
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    });
    futures_util::future::join_all(futs).await;
    Ok(SessionStats::default())
}

/// One sharded WS connection: connect, subscribe (single ≤SHARD_SIZE frame),
/// pump trades until disconnect/error.
async fn run_one_shard(
    connector: &AsterWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    let url = connector.ws_url();
    let connect = tokio_tungstenite::connect_async(url);
    let (ws, _resp) = tokio::time::timeout(connect_timeout, connect)
        .await
        .with_context(|| format!("ws connect timeout to {url}"))?
        .with_context(|| format!("ws connect to {url}"))?;
    let (mut sink, mut stream) = ws.split();

    // ≤SHARD_SIZE → ровно один subscribe-фрейм (под лимитом 3001).
    let spec_refs: Vec<&SymbolSpec> = shard.iter().map(|r| &r.spec).collect();
    for payload in connector.subscribe_payloads_batched(&spec_refs) {
        sink.send(Message::Text(payload))
            .await
            .context("aster: send subscribe")?;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "aster shard subscribed");

    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    for r in shard {
        routing.insert(r.spec.symbol.to_ascii_uppercase(), r.clone());
    }

    let parser = BinanceFuturesTradeParser;
    let mut stats = SessionStats::default();

    loop {
        match stream.next().await {
            None => break,
            Some(Ok(Message::Text(t))) => handle_payload(&parser, &routing, t.as_bytes(), &mut stats),
            Some(Ok(Message::Binary(b))) => handle_payload(&parser, &routing, &b, &mut stats),
            Some(Ok(Message::Ping(p))) => {
                sink.send(Message::Pong(p)).await.context("send pong")?;
            }
            Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
            Some(Ok(Message::Close(frame))) => {
                tracing::info!(shard = idx, ?frame, "aster ws close received");
                break;
            }
            Some(Err(e)) if is_soft_close(&e) => {
                tracing::info!(shard = idx, error = %e, "aster ws ended (peer hung up)");
                break;
            }
            Some(Err(e)) => return Err(anyhow::Error::from(e).context("aster ws read")),
        }
    }
    Ok(())
}

fn is_soft_close(e: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::Error;
    match e {
        Error::ConnectionClosed | Error::AlreadyClosed => true,
        Error::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::BrokenPipe
        ),
        _ => false,
    }
}

fn handle_payload(
    parser: &BinanceFuturesTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    raw: &[u8],
    stats: &mut SessionStats,
) {
    let v: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "ignoring undecodable aster frame");
            stats.frames_undecodable += 1;
            return;
        }
    };
    let Some(symbol) = parser.peek_symbol(&v) else {
        return;
    };
    let upper = symbol.to_ascii_uppercase();
    let Some(route) = routing.get(&upper) else {
        return;
    };
    let trade = match parser.parse_value(&v, &route.spec) {
        Ok(Some(t)) => t,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %upper, "aster parse_value failed");
            stats.parse_errors += 1;
            return;
        }
    };
    match route.sink.try_send(trade) {
        Ok(()) => stats.trades_emitted += 1,
        Err(mpsc::error::TrySendError::Full(_)) => stats.trades_dropped_backpressure += 1,
        Err(mpsc::error::TrySendError::Closed(_)) => stats.trades_dropped_closed += 1,
    }
}
