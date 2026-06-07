//! MEXC WS session — SHARDED, two protocols by market:
//!   - SPOT: `wss://wbs-api.mexc.com/ws`, subscribe JSON
//!     `{"method":"SUBSCRIPTION","params":["spot@public.aggre.deals.v3.api.pb@100ms@{SYM}"...]}`,
//!     market data arrives as BINARY protobuf, keepalive `{"method":"PING"}`.
//!   - FUTURES: `wss://contract.mexc.com/edge`, `{"method":"sub.deal","param":{"symbol":"BTC_USDT"}}`
//!     per symbol, JSON `push.deal`, keepalive `{"method":"ping"}`.
//! Both shard ≤100 syms/connection (parsing reused via exchange-mexc parsers).
//! Sharded like aster/kucoin: per-shard reconnect loops as part of this future.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_core::{TradePrint, WsConnector};
use exchange_mexc::{MexcFuturesTradeParser, MexcSpotTradeParser, MexcWs};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

const SHARD_SIZE: usize = 100;
const SPOT_PARAMS_PER_MSG: usize = 20;

pub async fn run_session(
    connector: &MexcWs,
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
        futures = connector.is_futures(),
        "mexc session: sharding across WS connections"
    );
    let futs = shards.iter().enumerate().map(|(idx, shard)| {
        let shard = shard.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                let res = if connector.is_futures() {
                    run_futures_shard(connector, &shard, connect_timeout, idx).await
                } else {
                    run_spot_shard(connector, &shard, connect_timeout, idx).await
                };
                if let Err(e) = res {
                    tracing::warn!(shard = idx, error = %e, "mexc shard error; reconnecting");
                } else {
                    backoff = Duration::from_millis(500);
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    });
    futures_util::future::join_all(futs).await;
    Ok(SessionStats::default())
}

// ─── Spot (protobuf) ──────────────────────────────────────────────────────────

async fn run_spot_shard(
    connector: &MexcWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    let url = connector.ws_url();
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("mexc spot connect timeout")?
        .context("mexc spot connect")?;
    let (mut sink, mut stream) = ws.split();

    // routing keyed by canonical (BTCUSDT); spot channel carries canonical.
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    for r in shard {
        routing.insert(connector.venue_symbol(&r.spec.symbol), r.clone());
    }
    let venues: Vec<String> = routing.keys().cloned().collect();
    for chunk in venues.chunks(SPOT_PARAMS_PER_MSG) {
        let params: Vec<String> = chunk
            .iter()
            .map(|s| format!("spot@public.aggre.deals.v3.api.pb@100ms@{s}"))
            .collect();
        let msg = serde_json::json!({ "method": "SUBSCRIPTION", "params": params });
        sink.send(Message::Text(msg.to_string()))
            .await
            .context("mexc spot: send subscribe")?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "mexc spot shard subscribed");

    let parser = MexcSpotTradeParser;
    let mut stats = SessionStats::default();
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = ping.tick() => {
                if let Err(e) = sink.send(Message::Text(r#"{"method":"PING"}"#.to_string())).await {
                    return Err(anyhow::Error::from(e).context("mexc spot: send ping"));
                }
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Binary(b))) => {
                        let Some(sym) = parser.peek_symbol(&b) else { continue };
                        let Some(route) = routing.get(&sym) else { continue };
                        emit(parser.parse_value(&b, &route.spec), route, &sym, &mut stats);
                    }
                    // Text frames are subscribe acks / PONG — not market data.
                    Some(Ok(Message::Text(_))) => {}
                    Some(Ok(Message::Ping(p))) => { sink.send(Message::Pong(p)).await.context("send pong")?; }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(f))) => { tracing::info!(shard = idx, ?f, "mexc spot ws close"); break; }
                    Some(Err(e)) if is_soft_close(&e) => { tracing::info!(shard = idx, error = %e, "mexc spot ws ended"); break; }
                    Some(Err(e)) => return Err(anyhow::Error::from(e).context("mexc spot ws read")),
                }
            }
        }
    }
    Ok(())
}

// ─── Futures (JSON) ───────────────────────────────────────────────────────────

async fn run_futures_shard(
    connector: &MexcWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    let url = connector.ws_url();
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("mexc fut connect timeout")?
        .context("mexc fut connect")?;
    let (mut sink, mut stream) = ws.split();

    // routing keyed by venue (BTC_USDT); push.deal carries the venue symbol.
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    for r in shard {
        routing.insert(connector.venue_symbol(&r.spec.symbol), r.clone());
    }
    for venue in routing.keys() {
        let msg = serde_json::json!({ "method": "sub.deal", "param": { "symbol": venue } });
        sink.send(Message::Text(msg.to_string()))
            .await
            .context("mexc fut: send sub.deal")?;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "mexc futures shard subscribed");

    let parser = MexcFuturesTradeParser;
    let mut stats = SessionStats::default();
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = ping.tick() => {
                if let Err(e) = sink.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await {
                    return Err(anyhow::Error::from(e).context("mexc fut: send ping"));
                }
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Text(t))) => {
                        let v: serde_json::Value = match serde_json::from_slice(t.as_bytes()) {
                            Ok(v) => v,
                            Err(_) => { stats.frames_undecodable += 1; continue; }
                        };
                        let Some(venue) = parser.peek_symbol(&v) else { continue };
                        let Some(route) = routing.get(venue) else { continue };
                        emit(parser.parse_value(&v, &route.spec), route, venue, &mut stats);
                    }
                    Some(Ok(Message::Binary(_))) => {} // compress:false → none expected
                    Some(Ok(Message::Ping(p))) => { sink.send(Message::Pong(p)).await.context("send pong")?; }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(f))) => { tracing::info!(shard = idx, ?f, "mexc fut ws close"); break; }
                    Some(Err(e)) if is_soft_close(&e) => { tracing::info!(shard = idx, error = %e, "mexc fut ws ended"); break; }
                    Some(Err(e)) => return Err(anyhow::Error::from(e).context("mexc fut ws read")),
                }
            }
        }
    }
    Ok(())
}

fn emit(
    parsed: Result<Vec<TradePrint>, exchange_core::ExchangeError>,
    route: &SymbolRoute,
    sym: &str,
    stats: &mut SessionStats,
) {
    let trades = match parsed {
        Ok(ts) => ts,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %sym, "mexc parse_value failed");
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
