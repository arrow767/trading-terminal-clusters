//! BingX WS session — SHARDED, one channel per subscribe message.
//!
//! Transport quirks (vs the other sessions):
//!   - every server data frame is GZIP-compressed BINARY → inflate before JSON;
//!   - heartbeat is JSON `{"ping":"<uuid>","time":...}` → reply `{"pong":...}`;
//!   - subscribe `{"id","reqType":"sub","dataType":"<SYM>@trade"}` per symbol.
//! Swap and spot differ only by URL (`connector.ws_url()`); aggressor polarity
//! is resolved in the parser from `spec.market_type`. Sharded ≤150 syms/conn.

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_bingx::{BingxTradeParser, BingxWs};
use exchange_core::WsConnector;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

const SHARD_SIZE: usize = 150;

pub async fn run_session(
    connector: &BingxWs,
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
        "bingx session: sharding across WS connections"
    );
    let futs = shards.iter().enumerate().map(|(idx, shard)| {
        let shard = shard.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_one_shard(connector, &shard, connect_timeout, idx).await {
                    Ok(()) => backoff = Duration::from_millis(500),
                    Err(e) => tracing::warn!(shard = idx, error = %e, "bingx shard error; reconnecting"),
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
    connector: &BingxWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    let url = connector.ws_url();
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("bingx connect timeout")?
        .context("bingx connect")?;
    let (mut sink, mut stream) = ws.split();

    // routing keyed by canonical (BTCUSDT); the @trade dataType carries the venue
    // symbol (BTC-USDT) which the parser maps back to canonical.
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    for r in shard {
        routing.insert(r.spec.symbol.to_uppercase(), r.clone());
    }
    for r in shard {
        let venue = connector.venue_symbol(&r.spec.symbol);
        let msg = format!(r#"{{"id":"tr","reqType":"sub","dataType":"{venue}@trade"}}"#);
        sink.send(Message::Text(msg))
            .await
            .context("bingx: send subscribe")?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "bingx shard subscribed");

    let parser = BingxTradeParser;
    let mut stats = SessionStats::default();

    loop {
        match stream.next().await {
            None => break,
            Some(Ok(Message::Binary(b))) => {
                let Some(text) = inflate(&b) else {
                    stats.frames_undecodable += 1;
                    continue;
                };
                if let Some(pong) = process_frame(&parser, &routing, &text, &mut stats) {
                    sink.send(Message::Text(pong)).await.context("bingx: send pong")?;
                }
            }
            Some(Ok(Message::Text(t))) => {
                if let Some(pong) = process_frame(&parser, &routing, &t, &mut stats) {
                    sink.send(Message::Text(pong)).await.context("bingx: send pong")?;
                }
            }
            Some(Ok(Message::Ping(p))) => sink.send(Message::Pong(p)).await.context("send pong")?,
            Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
            Some(Ok(Message::Close(f))) => {
                tracing::info!(shard = idx, ?f, "bingx ws close");
                break;
            }
            Some(Err(e)) if is_soft_close(&e) => {
                tracing::info!(shard = idx, error = %e, "bingx ws ended");
                break;
            }
            Some(Err(e)) => return Err(anyhow::Error::from(e).context("bingx ws read")),
        }
    }
    Ok(())
}

/// Parse one (already-inflated) frame. Emits trades to the route sinks and
/// returns `Some(pong_json)` if the frame was a heartbeat (caller must send it).
fn process_frame(
    parser: &BingxTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    text: &str,
    stats: &mut SessionStats,
) -> Option<String> {
    let v: serde_json::Value = match serde_json::from_str(text.trim_start()) {
        Ok(v) => v,
        Err(_) => {
            stats.frames_undecodable += 1;
            return None;
        }
    };
    // Heartbeat: {"ping":"<uuid>","time":"<iso>"} → reply {"pong":...}.
    if let Some(ping) = v.get("ping").and_then(|x| x.as_str()) {
        return Some(match v.get("time").and_then(|x| x.as_str()) {
            Some(t) => format!(r#"{{"pong":"{ping}","time":"{t}"}}"#),
            None => format!(r#"{{"pong":"{ping}"}}"#),
        });
    }
    let sym = parser.peek_symbol(&v)?; // None for ack / error / non-trade frames
    let route = routing.get(&sym)?;
    match parser.parse_value(&v, &route.spec) {
        Ok(trades) => {
            for trade in trades {
                match route.sink.try_send(trade) {
                    Ok(()) => stats.trades_emitted += 1,
                    Err(mpsc::error::TrySendError::Full(_)) => stats.trades_dropped_backpressure += 1,
                    Err(mpsc::error::TrySendError::Closed(_)) => stats.trades_dropped_closed += 1,
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, symbol = %sym, "bingx parse_value failed");
            stats.parse_errors += 1;
        }
    }
    None
}

/// Inflate a gzip-compressed binary WS frame to UTF-8.
fn inflate(data: &[u8]) -> Option<String> {
    let mut d = flate2::read::GzDecoder::new(data);
    let mut s = String::new();
    d.read_to_string(&mut s).ok().map(|_| s)
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
