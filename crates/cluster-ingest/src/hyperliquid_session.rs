//! Hyperliquid WS session — sharded, subscribe one coin per message.
//!
//! `wss://api.hyperliquid.xyz/ws`; per coin send
//! `{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}`,
//! keepalive `{"method":"ping"}` (~14s; HL drops idle ~30s+). Routing is keyed
//! by the native wire coin (BTC / kPEPE); the route's spec keeps canonical
//! `{COIN}USDC`. Sharded like the other custom sessions (per-shard reconnect
//! loops are part of this future so route-change/shutdown cancels them).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_core::{TradePrint, WsConnector};
use exchange_hyperliquid::{HyperliquidTradeParser, HyperliquidWs};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

const SHARD_SIZE: usize = 250;

pub async fn run_session(
    connector: &HyperliquidWs,
    routes: &[SymbolRoute],
    connect_timeout: Duration,
) -> Result<SessionStats> {
    if routes.is_empty() {
        return Err(anyhow!("run_session called with no routes"));
    }
    let shards: Vec<Vec<SymbolRoute>> = routes.chunks(SHARD_SIZE).map(|c| c.to_vec()).collect();
    tracing::info!(symbols = routes.len(), shards = shards.len(), "hyperliquid session: sharding");
    let futs = shards.iter().enumerate().map(|(idx, shard)| {
        let shard = shard.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_one_shard(connector, &shard, connect_timeout, idx).await {
                    Ok(()) => backoff = Duration::from_millis(500),
                    Err(e) => tracing::warn!(shard = idx, error = %e, "hyperliquid shard error; reconnecting"),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    });
    futures_util::future::join_all(futs).await;
    Ok(SessionStats::default())
}

async fn run_one_shard(
    connector: &HyperliquidWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    let url = connector.ws_url();
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("hyperliquid connect timeout")?
        .context("hyperliquid connect")?;
    let (mut sink, mut stream) = ws.split();

    // routing keyed by native wire coin; subscribe one coin per message.
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    for r in shard {
        routing.insert(connector.native_coin(&r.spec.symbol), r.clone());
    }
    for coin in routing.keys() {
        let msg = serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "trades", "coin": coin },
        });
        sink.send(Message::Text(msg.to_string()))
            .await
            .context("hyperliquid: send subscribe")?;
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "hyperliquid shard subscribed");

    let parser = HyperliquidTradeParser;
    let mut stats = SessionStats::default();
    let mut ping = tokio::time::interval(Duration::from_secs(14));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = ping.tick() => {
                if let Err(e) = sink.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await {
                    return Err(anyhow::Error::from(e).context("hyperliquid: send ping"));
                }
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Text(t))) => handle(&parser, &routing, t.as_bytes(), &mut stats),
                    Some(Ok(Message::Binary(b))) => handle(&parser, &routing, &b, &mut stats),
                    Some(Ok(Message::Ping(p))) => { sink.send(Message::Pong(p)).await.context("send pong")?; }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(f))) => { tracing::info!(shard = idx, ?f, "hyperliquid ws close"); break; }
                    Some(Err(e)) if is_soft_close(&e) => { tracing::info!(shard = idx, error = %e, "hyperliquid ws ended"); break; }
                    Some(Err(e)) => return Err(anyhow::Error::from(e).context("hyperliquid ws read")),
                }
            }
        }
    }
    Ok(())
}

fn handle(
    parser: &HyperliquidTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    raw: &[u8],
    stats: &mut SessionStats,
) {
    let v: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(_) => {
            stats.frames_undecodable += 1;
            return;
        }
    };
    let Some(coin) = parser.peek_symbol(&v) else {
        return; // pong / subscriptionResponse
    };
    let Some(route) = routing.get(coin) else {
        return;
    };
    let trades: Vec<TradePrint> = match parser.parse_value(&v, &route.spec) {
        Ok(ts) => ts,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %coin, "hyperliquid parse_value failed");
            stats.parse_errors += 1;
            return;
        }
    };
    for trade in trades {
        match route.sink.try_send(trade) {
            Ok(()) => stats.trades_emitted += 1,
            Err(mpsc::error::TrySendError::Full(_)) => stats.trades_dropped_backpressure += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => stats.trades_dropped_closed += 1,
        }
    }
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
