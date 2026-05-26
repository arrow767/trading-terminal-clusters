//! Bybit V5 public WS session loop.
//!
//! Отличия от Binance-session:
//! 1. **Batched subscribe**: Bybit лимит 10 args / message. Шлём
//!    N/10 frame'ов на запуск.
//! 2. **Client-initiated ping**: сервер не шлёт WS-ping; мы должны
//!    каждые ~20с отправлять `{"op":"ping"}`. Иначе coercion idle-disconnect.
//! 3. **Batched trades в одном WS-frame'е**: `publicTrade.{sym}` шлёт
//!    массив `data: [trade, trade, ...]`. Парсер возвращает Vec<TradePrint>.
//! 4. **Topic-routing**: trade-symbol извлекается из `topic` field,
//!    не из `data` (структурно отличается от Binance `s` field).
//!
//! Остальное (try_send, lag-counter, soft-close detection) — идентично
//! `binance_session.rs`. Делать общий `session.rs` с
//! polymorphic parser'ом — после того как добавим 3-ю биржу: ещё одно
//! сравнение даст понять, какая абстракция подходит лучше.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_bybit::{BybitTradeParser, BybitWs};
use exchange_core::{SymbolSpec, TradePrint, WsConnector};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::binance_session::{SessionStats, SymbolRoute};

pub async fn run_session(
    connector: &BybitWs,
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

    // BATCHED subscribe — Bybit лимит 10 args/msg. На 200 symbols это 20
    // frames. Шлём подряд; ack'и приходят async, мы их потом скипаем
    // в handle_payload (это просто `topic = null` / `op = subscribe`-ответы).
    let spec_refs: Vec<&SymbolSpec> = routes.iter().map(|r| &r.spec).collect();
    for payload in connector.subscribe_payloads_batched(&spec_refs) {
        sink.send(Message::Text(payload))
            .await
            .context("bybit: send subscribe batch")?;
    }
    tracing::info!(symbols = routes.len(), "bybit session subscribed");

    let mut routing: HashMap<String, SymbolRoute> = HashMap::with_capacity(routes.len());
    for r in routes {
        routing.insert(r.spec.symbol.to_ascii_uppercase(), r.clone());
    }

    let parser = BybitTradeParser;
    let mut stats = SessionStats::default();

    // Client-initiated ping таймер. `MissedTickBehavior::Skip` — если
    // вдруг event-loop отставал, не шлём бэклог ping'ов залпом.
    let ping_interval_ms = connector.ping_interval_ms();
    let mut ping_ticker =
        tokio::time::interval(Duration::from_millis(ping_interval_ms.max(1_000)));
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Первый tick interval'а — мгновенный, шлёт ping сразу при старте.
    // Это нормально: сервер ответит pong, мы только увидим в peek_symbol == None.
    let ping_text = match connector.ping_payload() {
        exchange_core::PingKind::Text(t) => t,
        _ => r#"{"op":"ping"}"#, // fallback — Bybit всегда Text, но защитимся
    };

    loop {
        tokio::select! {
            biased;
            // Client ping
            _ = ping_ticker.tick() => {
                if let Err(e) = sink.send(Message::Text(ping_text.to_string())).await {
                    return Err(anyhow::Error::from(e).context("bybit: send ping"));
                }
                stats.pongs_sent += 1; // переиспользуем счётчик как "наши тексты"
            }
            // Inbound WS frame
            next = stream.next() => {
                match next {
                    None => break,
                    Some(Ok(Message::Text(t))) => {
                        handle_payload(&parser, &routing, t.as_bytes(), &mut stats);
                    }
                    Some(Ok(Message::Binary(b))) => {
                        handle_payload(&parser, &routing, &b, &mut stats);
                    }
                    Some(Ok(Message::Ping(p))) => {
                        // Bybit обычно не шлёт ws-ping, но если вдруг —
                        // ответим pong для совместимости со спецификацией.
                        sink.send(Message::Pong(p)).await.context("send pong")?;
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(?frame, "bybit ws close received");
                        break;
                    }
                    Some(Err(e)) if is_soft_close(&e) => {
                        tracing::info!(error = %e, "bybit ws session ended (peer hung up)");
                        break;
                    }
                    Some(Err(e)) => {
                        return Err(anyhow::Error::from(e).context("bybit ws read"));
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
    parser: &BybitTradeParser,
    routing: &HashMap<String, SymbolRoute>,
    raw: &[u8],
    stats: &mut SessionStats,
) {
    let v: serde_json::Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "ignoring undecodable bybit frame");
            stats.frames_undecodable += 1;
            return;
        }
    };
    let Some(symbol) = parser.peek_symbol(&v) else {
        // subscribe ack / pong / error / heartbeat — не trade.
        return;
    };
    let upper = symbol.to_ascii_uppercase();
    let Some(route) = routing.get(&upper) else {
        // Может произойти на subscribe-ack'е сразу после нашего batch send'а,
        // если Bybit вернёт публичный topic с неподписанным символом
        // (теоретически возможно при route reshuffle). Игнорируем тихо.
        return;
    };
    let trades = match parser.parse_value(&v, &route.spec) {
        Ok(ts) => ts,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %upper, "bybit parse_value failed");
            stats.parse_errors += 1;
            return;
        }
    };
    // Bybit шлёт массив трейдов в одном frame'е — раздаём все за один проход.
    for trade in trades {
        match route.sink.try_send(trade) {
            Ok(()) => stats.trades_emitted += 1,
            Err(mpsc::error::TrySendError::Full(_)) => stats.trades_dropped_backpressure += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => stats.trades_dropped_closed += 1,
        }
    }
}
