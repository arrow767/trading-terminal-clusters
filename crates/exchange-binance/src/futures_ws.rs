use exchange_core::{PingKind, SymbolSpec, WsConnector};

/// USD-M futures combined-stream WebSocket. As of 2026-05 Binance routes
/// streams via prefixed paths: `/market/`, `/public/`, `/private/`.
/// Connections without a routed prefix (the historical
/// `wss://fstream.binance.com/stream`) accept SUBSCRIBE and return an
/// ack, but **silently deliver no market frames** — confirmed empirically
/// on this codebase (ack ok, zero aggTrade frames in 8s).
///
/// Use `/market/stream` for the combined-stream subscribe-method protocol.
/// Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
const DEFAULT_WS_URL: &str = "wss://fstream.binance.com/market/stream";

/// `WsConnector` for Binance USD-M futures combined-stream endpoint.
///
/// Binance futures spec (relevant bits):
/// - Server pings every 3 minutes; client must answer with a pong frame
///   within 10 minutes or the connection is dropped. We never initiate
///   pings ourselves — `ping_payload` is `ServerInitiated`.
/// - One connection holds at most ~1024 streams, but the documented
///   "soft" limit at which behavior gets unreliable is 200; we cap at
///   200 to leave headroom for resubscribe churn.
pub struct BinanceFuturesWs {
    ws_url: String,
}

impl BinanceFuturesWs {
    pub fn new() -> Self {
        Self {
            ws_url: DEFAULT_WS_URL.to_string(),
        }
    }

    pub fn with_url(url: impl Into<String>) -> Self {
        Self { ws_url: url.into() }
    }
}

impl Default for BinanceFuturesWs {
    fn default() -> Self {
        Self::new()
    }
}

impl WsConnector for BinanceFuturesWs {
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String {
        let params: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.symbol.to_lowercase()))
            .collect();
        serde_json::json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        })
        .to_string()
    }

    fn ping_interval_ms(&self) -> u64 {
        180_000
    }

    fn ping_payload(&self) -> PingKind {
        PingKind::ServerInitiated
    }

    fn pong_timeout_ms(&self) -> u64 {
        600_000
    }

    fn max_subscriptions_per_socket(&self) -> usize {
        200
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

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

    #[test]
    fn subscribe_payload_lowercases_and_appends_stream() {
        let ws = BinanceFuturesWs::new();
        let s1 = spec("BTCUSDT");
        let s2 = spec("ETHUSDT");
        let payload = ws.subscribe_payload(&[&s1, &s2]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["method"], "SUBSCRIBE");
        let params = v["params"].as_array().unwrap();
        assert_eq!(params[0], "btcusdt@aggTrade");
        assert_eq!(params[1], "ethusdt@aggTrade");
    }

    #[test]
    fn matches_binance_keepalive_spec() {
        let ws = BinanceFuturesWs::new();
        assert!(ws.ping_interval_ms() < ws.pong_timeout_ms());
        assert!(matches!(ws.ping_payload(), PingKind::ServerInitiated));
    }
}
