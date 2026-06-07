//! Bitget V2 public `trade` WS session loop.
//!
//! Структурно идентичен `okx_session`:
//! 1. **Batched subscribe** — дробим на фреймы (`subscribe_payloads_batched`).
//! 2. **Client-initiated ping** — Bitget рвёт idle через ~30с; шлём текст `ping`
//!    каждые ~15с (Bitget отвечает плоским `pong`).
//! 3. **Batched trades** — `trade` канал шлёт `data: [trade, ...]`.
//! 4. **Topic-routing** — symbol из `arg.instId` (уже канон BTCUSDT).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_bitget::{BitgetTradeParser, BitgetWs};
use exchange_core::{SymbolSpec, WsConnector};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

pub async fn run_session(
    connector: &BitgetWs,
    routes: &[SymbolRoute],
    connect_timeout: Duration,
) -> Result<SessionStats> {
    if routes.is_empty() {
        return Err(anyhow!("run_session called with no routes"));
    }

    let url = connector.ws_url();
    let connect = tokio_tungstenite::connect_async(url);
    let (ws, _resp) = tokio::time::timeout(connect_timeout, connect)
        .await
        .with_context(|| format!("ws connect timeout to {url}"))?
        .with_context(|| format!("ws connect to {url}"))?;
    let (mut sink, mut stream) = ws.split();

    // BATCHED subscribe — каждый чанк отдельным фреймом, С ПАУЗОЙ между ними.
    // Bitget закрывает коннект (3001 / immediate "ws read") если subscribe-
    // фреймы летят слишком быстро. 250мс между фреймами держит нас под
    // rate-limit'ом и позволяет подписать весь universe (800+ символов) на
    // одном сокете без реконнект-цикла.
    let spec_refs: Vec<&SymbolSpec> = routes.iter().map(|r| &r.spec).collect();
    for payload in connector.subscribe_payloads_batched(&spec_refs) {
        sink.send(Message::Text(payload))
            .await
            .context("bitget: send subscribe batch")?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tracing::info!(symbols = routes.len(), "bitget session subscribed");

    // Routing keyed by canonical symbol (spec.symbol канон BTCUSDT,
    // peek_symbol тоже возвращает канон).
    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(routes.len());
    for r in routes {
        routing.insert(r.spec.symbol.to_ascii_uppercase(), r.clone());
    }

    let parser = BitgetTradeParser;
    let mut stats = SessionStats::default();

    let ping_interval_ms = connector.ping_interval_ms();
    let mut ping_ticker =
        tokio::time::interval(Duration::from_millis(ping_interval_ms.max(1_000)));
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ping_text = match connector.ping_payload() {
        exchange_core::PingKind::Text(t) => t,
        _ => "ping",
    };

    loop {
        tokio::select! {
            biased;
            _ = ping_ticker.tick() => {
                if let Err(e) = sink.send(Message::Text(ping_text.to_string())).await {
                    return Err(anyhow::Error::from(e).context("bitget: send ping"));
                }
                stats.pongs_sent += 1;
            }
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Text(t))) => {
                        // Bitget отвечает на наш `ping` плоским `pong` — не JSON.
                        if t.as_str() == "pong" { continue; }
                        handle_payload(&parser, &routing, t.as_bytes(), &mut stats);
                    }
                    Some(Ok(Message::Binary(b))) => {
                        handle_payload(&parser, &routing, &b, &mut stats);
                    }
                    Some(Ok(Message::Ping(p))) => {
                        sink.send(Message::Pong(p)).await.context("send pong")?;
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(?frame, "bitget ws close received");
                        break;
                    }
                    Some(Err(e)) if is_soft_close(&e) => {
                        tracing::info!(error = %e, "bitget ws session ended (peer hung up)");
                        break;
                    }
                    Some(Err(e)) => {
                        return Err(anyhow::Error::from(e).context("bitget ws read"));
                    }
                }
            }
        }
    }

    Ok(stats)
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
    parser: &BitgetTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    raw: &[u8],
    stats: &mut SessionStats,
) {
    let v: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "ignoring undecodable bitget frame");
            stats.frames_undecodable += 1;
            return;
        }
    };
    let Some(symbol) = parser.peek_symbol(&v) else {
        return;
    };
    let Some(route) = routing.get(&symbol) else {
        return;
    };
    let trades = match parser.parse_value(&v, &route.spec) {
        Ok(ts) => ts,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %symbol, "bitget parse_value failed");
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
