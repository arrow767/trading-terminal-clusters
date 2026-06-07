//! Aster (asterdex.com) `/ws` SUBSCRIBE session loop.
//!
//! Aster — клон Binance, поэтому envelope трейдов и парсер те же
//! (`BinanceFuturesTradeParser` принимает raw `/ws` aggTrade). Отличие от
//! `binance_session` — подписка БАТЧАМИ С ПАУЗОЙ: Aster закрывает коннект
//! (close 3001) если SUBSCRIBE содержит >~100 params или фреймы летят
//! слишком быстро. Дробим на ≤100 params/frame и шлём с паузой 250мс.

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

pub async fn run_session(
    connector: &AsterWs,
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

    // BATCHED subscribe с паузой между фреймами (см. модульный комментарий).
    let spec_refs: Vec<&SymbolSpec> = routes.iter().map(|r| &r.spec).collect();
    for payload in connector.subscribe_payloads_batched(&spec_refs) {
        sink.send(Message::Text(payload))
            .await
            .context("aster: send subscribe batch")?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tracing::info!(symbols = routes.len(), "aster session subscribed");

    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(routes.len());
    for r in routes {
        routing.insert(r.spec.symbol.to_ascii_uppercase(), r.clone());
    }

    let parser = BinanceFuturesTradeParser;
    let mut stats = SessionStats::default();

    loop {
        match stream.next().await {
            None => break,
            Some(Ok(Message::Text(t))) => {
                handle_payload(&parser, &routing, t.as_bytes(), &mut stats);
            }
            Some(Ok(Message::Binary(b))) => {
                handle_payload(&parser, &routing, &b, &mut stats);
            }
            Some(Ok(Message::Ping(p))) => {
                sink.send(Message::Pong(p)).await.context("send pong")?;
                stats.pongs_sent += 1;
            }
            Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
            Some(Ok(Message::Close(frame))) => {
                tracing::info!(?frame, "aster ws close received");
                break;
            }
            Some(Err(e)) if is_soft_close(&e) => {
                tracing::info!(error = %e, "aster ws session ended (peer hung up)");
                break;
            }
            Some(Err(e)) => {
                return Err(anyhow::Error::from(e).context("aster ws read"));
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
