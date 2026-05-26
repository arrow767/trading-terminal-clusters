use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use exchange_binance::BinanceFuturesTradeParser;
use exchange_core::{SymbolSpec, TradePrint, WsConnector};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// Routing entry for one subscribed symbol on a Binance session.
///
/// `sink` is the inbound channel of the per-symbol aggregator task. The
/// session uses `try_send` (non-blocking) — if the aggregator is lagging
/// we increment a drop counter rather than stalling the WS read loop,
/// because a stalled read would let the pong deadline expire and Binance
/// would close the connection.
///
/// `Clone` is cheap (Sender is an Arc internally) so the orchestrator
/// can hold the canonical list and pass `&[SymbolRoute]` to each
/// reconnect attempt.
#[derive(Clone)]
pub struct SymbolRoute {
    pub spec: SymbolSpec,
    pub sink: mpsc::Sender<TradePrint>,
}

/// Open one Binance USD-M futures WS session, subscribe to all symbols
/// in `routes`, and pump trades into per-symbol sinks. Returns when the
/// connection closes (cleanly or with error). The caller is responsible
/// for reconnect / backoff / chunking — that orchestration lives in the
/// pool layer (next slice).
///
/// If `connect_timeout` elapses before the TLS+WS handshake completes
/// we give up rather than letting the dial hang indefinitely.
pub async fn run_session(
    connector: &dyn WsConnector,
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

    let spec_refs: Vec<&SymbolSpec> = routes.iter().map(|r| &r.spec).collect();
    let sub_payload = connector.subscribe_payload(&spec_refs);
    sink.send(Message::Text(sub_payload))
        .await
        .context("send subscribe payload")?;
    tracing::info!(symbols = routes.len(), "binance session subscribed");

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
                tracing::info!(?frame, "ws close received");
                break;
            }
            Some(Err(e)) if is_soft_close(&e) => {
                tracing::info!(error = %e, "ws session ended (peer hung up)");
                break;
            }
            Some(Err(e)) => {
                return Err(anyhow::Error::from(e).context("ws read"));
            }
        }
    }

    Ok(stats)
}

/// Treat connection-reset / EOF / aborted as a benign close: many
/// exchanges drop the TCP socket instead of sending a WS Close frame
/// when they decide to recycle a connection (Binance does this on the
/// 24h limit), and on Windows that surfaces as `ConnectionReset`. We
/// don't want the orchestrator to escalate these to a fatal error —
/// just reconnect.
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
            tracing::debug!(error = %e, "ignoring undecodable frame");
            stats.frames_undecodable += 1;
            return;
        }
    };
    let Some(symbol) = parser.peek_symbol(&v) else {
        // Subscription ack, error, heartbeat — not a trade event.
        return;
    };
    let upper = symbol.to_ascii_uppercase();
    let Some(route) = routing.get(&upper) else {
        // Trade for a symbol we did not subscribe to — should not happen
        // in normal operation, but ignore silently.
        return;
    };
    let trade = match parser.parse_value(&v, &route.spec) {
        Ok(Some(t)) => t,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(error = %e, symbol = %upper, "parse_value failed");
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

#[derive(Debug, Default, Clone, Copy)]
pub struct SessionStats {
    pub trades_emitted: u64,
    pub trades_dropped_backpressure: u64,
    pub trades_dropped_closed: u64,
    pub parse_errors: u64,
    pub frames_undecodable: u64,
    pub pongs_sent: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::SocketAddr;

    use exchange_binance::BinanceFuturesWs;
    use exchange_core::{Exchange, MarketType, Quote};
    use tokio::net::TcpListener;

    use super::*;

    fn spec(symbol: &str) -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::BinanceF,
            market_type: MarketType::Perp,
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 2,
            qty_scale: 3,
            tick_size: 10,
            step_size: 1,
        }
    }

    /// Spawn a one-shot WS server that accepts a single client, reads the
    /// subscribe message, then writes the supplied frames and closes.
    async fn run_mock_ws(addr: SocketAddr, listener: TcpListener, frames: Vec<String>) {
        let (sock, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(sock).await.unwrap();
        // Drain subscribe.
        let _sub = ws.next().await.unwrap().unwrap();
        for f in frames {
            ws.send(Message::Text(f)).await.unwrap();
        }
        // Server-initiated ping → exercise pong handling.
        ws.send(Message::Ping(vec![1, 2, 3])).await.unwrap();
        // Wait briefly so client gets the pong out before we close.
        tokio::time::sleep(Duration::from_millis(50)).await;
        ws.close(None).await.ok();
        let _ = addr;
    }

    #[tokio::test]
    async fn end_to_end_routes_trades_to_correct_sink() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{addr}");

        let frames = vec![
            r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"100.00","q":"0.001","T":1700000000000,"m":false}}"#.to_string(),
            r#"{"stream":"ethusdt@aggTrade","data":{"e":"aggTrade","s":"ETHUSDT","a":2,"p":"3000.50","q":"0.500","T":1700000001000,"m":true}}"#.to_string(),
            r#"{"result":null,"id":1}"#.to_string(), // subscription ack — must be ignored
        ];
        let server = tokio::spawn(run_mock_ws(addr, listener, frames));

        let connector = BinanceFuturesWs::with_url(url);
        let (btc_tx, mut btc_rx) = mpsc::channel(8);
        let (eth_tx, mut eth_rx) = mpsc::channel(8);
        let routes = vec![
            SymbolRoute {
                spec: spec("BTCUSDT"),
                sink: btc_tx,
            },
            SymbolRoute {
                spec: spec("ETHUSDT"),
                sink: eth_tx,
            },
        ];

        let stats = run_session(&connector, &routes, Duration::from_secs(2))
            .await
            .unwrap();

        let btc = btc_rx.recv().await.unwrap();
        assert_eq!(btc.trade_id, 1);
        assert_eq!(btc.price, 10_000); // 100.00 * 100
        assert_eq!(btc.qty, 1); // 0.001 * 1000
        assert_eq!(btc.aggressor, exchange_core::AggressorSide::Bid);

        let eth = eth_rx.recv().await.unwrap();
        assert_eq!(eth.trade_id, 2);
        assert_eq!(eth.price, 300_050);
        assert_eq!(eth.aggressor, exchange_core::AggressorSide::Ask);

        assert_eq!(stats.trades_emitted, 2);
        assert_eq!(stats.parse_errors, 0);
        assert_eq!(stats.pongs_sent, 1);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_when_no_routes() {
        let connector = BinanceFuturesWs::with_url("ws://127.0.0.1:1");
        let r = run_session(&connector, &[], Duration::from_millis(100)).await;
        assert!(r.is_err());
    }
}
