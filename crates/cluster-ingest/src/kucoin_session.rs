//! KuCoin WS session — bullet-token connect, SHARDED, comma-joined subscribe.
//!
//! Per shard: `POST bullet-public` → `wss://{endpoint}?token=..&connectId=..`,
//! subscribe `/contractMarket/execution:` (futures) / `/market/match:` (spot)
//! with venue symbols comma-joined ≤100/topic, JSON `{"type":"ping"}` heartbeat.
//! KuCoin caps subscriptions per connection (~400, close 509) → shard ≤250.
//! Routing is keyed by VENUE symbol (XBTUSDTM / BTC-USDT); the route's spec
//! keeps the canonical BTCUSDT for storage + contract conversion.
//!
//! Sharded like `aster_session`: per-shard reconnect loops run as part of this
//! future (not spawned) so route-change/shutdown cancels them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_kucoin::{KucoinTradeParser, KucoinWs};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

const SHARD_SIZE: usize = 250; // under KuCoin's ~400/connection cap (code 509)
const SYMS_PER_TOPIC: usize = 100; // comma-joined symbols per subscribe message

pub async fn run_session(
    connector: &KucoinWs,
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
        "kucoin session: sharding across bullet WS connections"
    );

    let futs = shards.iter().enumerate().map(|(idx, shard)| {
        let shard = shard.clone();
        async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match run_one_shard(connector, &shard, connect_timeout, idx).await {
                    Ok(()) => backoff = Duration::from_millis(500),
                    Err(e) => {
                        tracing::warn!(shard = idx, error = %e, "kucoin shard error; reconnecting");
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

fn next_connect_id() -> String {
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{t:016x}{n:x}")
}

async fn run_one_shard(
    connector: &KucoinWs,
    shard: &[SymbolRoute],
    connect_timeout: Duration,
    idx: usize,
) -> Result<()> {
    // 1. Bullet token (one-time, per connection).
    let bullet = connector
        .fetch_bullet()
        .await
        .map_err(|e| anyhow!("kucoin bullet: {e}"))?;
    let sep = if bullet.endpoint.contains('?') { '&' } else { '?' };
    let url = format!(
        "{}{}token={}&connectId={}",
        bullet.endpoint,
        sep,
        bullet.token,
        next_connect_id()
    );

    let connect = tokio_tungstenite::connect_async(url.as_str());
    let (ws, _resp) = tokio::time::timeout(connect_timeout, connect)
        .await
        .context("kucoin ws connect timeout")?
        .context("kucoin ws connect")?;
    let (mut sink, mut stream) = ws.split();

    // 2. Routing keyed by venue symbol; subscribe comma-joined.
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(shard.len());
    let mut venues: Vec<String> = Vec::with_capacity(shard.len());
    for r in shard {
        let venue = connector.venue_symbol(&r.spec.symbol);
        routing.insert(venue.clone(), r.clone());
        venues.push(venue);
    }
    let prefix = connector.exec_topic_prefix();
    let mut sub_id: u64 = 1;
    for chunk in venues.chunks(SYMS_PER_TOPIC) {
        let topic = format!("{}{}", prefix, chunk.join(","));
        let msg = serde_json::json!({
            "id": sub_id.to_string(),
            "type": "subscribe",
            "topic": topic,
            "response": false,
        });
        sink.send(Message::Text(msg.to_string()))
            .await
            .context("kucoin: send subscribe")?;
        sub_id += 1;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tracing::info!(shard = idx, symbols = shard.len(), "kucoin shard subscribed");

    let parser = KucoinTradeParser;
    let mut stats = SessionStats::default();

    // 3. JSON ping a few seconds inside the server's pingInterval.
    let ping_ms = bullet.ping_interval_ms.clamp(5_000, 18_000).saturating_sub(3_000).max(5_000);
    let mut ping_ticker = tokio::time::interval(Duration::from_millis(ping_ms));
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_ticker.reset();

    loop {
        tokio::select! {
            biased;
            _ = ping_ticker.tick() => {
                let ping = serde_json::json!({ "id": sub_id.to_string(), "type": "ping" });
                sub_id += 1;
                if let Err(e) = sink.send(Message::Text(ping.to_string())).await {
                    return Err(anyhow::Error::from(e).context("kucoin: send ping"));
                }
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Text(t))) => handle_payload(&parser, &routing, t.as_bytes(), &mut stats),
                    Some(Ok(Message::Binary(b))) => handle_payload(&parser, &routing, &b, &mut stats),
                    Some(Ok(Message::Ping(p))) => {
                        sink.send(Message::Pong(p)).await.context("send pong")?;
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(shard = idx, ?frame, "kucoin ws close received");
                        break;
                    }
                    Some(Err(e)) if is_soft_close(&e) => {
                        tracing::info!(shard = idx, error = %e, "kucoin ws ended (peer hung up)");
                        break;
                    }
                    Some(Err(e)) => return Err(anyhow::Error::from(e).context("kucoin ws read")),
                }
            }
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
    parser: &KucoinTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    raw: &[u8],
    stats: &mut SessionStats,
) {
    let v: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "ignoring undecodable kucoin frame");
            stats.frames_undecodable += 1;
            return;
        }
    };
    if v.get("type").and_then(|x| x.as_str()) == Some("error") {
        tracing::warn!(frame = %v, "kucoin ws error frame");
        return;
    }
    let Some(venue) = parser.peek_symbol(&v) else {
        return; // pong / welcome / ack / non-trade
    };
    let Some(route) = routing.get(venue) else {
        return;
    };
    let trades = match parser.parse_value(&v, &route.spec) {
        Ok(ts) => ts,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %venue, "kucoin parse_value failed");
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
