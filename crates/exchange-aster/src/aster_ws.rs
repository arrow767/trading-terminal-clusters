//! WsConnector для Aster (asterdex.com) — клон combined-stream Binance.
//!
//! Aster держит legacy-монолитный `/stream` endpoint (Binance Futures его
//! выпилил 2026-04-23). Подписка — тот же `{"method":"SUBSCRIBE",...}`,
//! envelope трейдов — `{"stream":..,"data":{aggTrade}}` (bit-for-bit Binance),
//! поэтому в `cluster-ingest` переиспользуется `SessionFlavor::Binance` +
//! `BinanceFuturesTradeParser` (он принимает и wrapped, и raw aggTrade).
//!
//! Hosts:
//!   - perp: `wss://fstream.asterdex.com/stream`
//!   - spot: `wss://sstream.asterdex.com/stream`
//!
//! Keepalive: сервер шлёт ping'и (как Binance) → `ServerInitiated`; session
//! отвечает pong'ом на каждый входящий ping-frame.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

pub const FUTURES_WS_URL: &str = "wss://fstream.asterdex.com/stream";
pub const SPOT_WS_URL: &str = "wss://sstream.asterdex.com/stream";

pub struct AsterWs {
    ws_url: String,
}

impl AsterWs {
    pub fn futures() -> Self {
        Self {
            ws_url: FUTURES_WS_URL.to_string(),
        }
    }
    pub fn spot() -> Self {
        Self {
            ws_url: SPOT_WS_URL.to_string(),
        }
    }
    pub fn with_url(url: impl Into<String>) -> Self {
        Self { ws_url: url.into() }
    }
}

impl WsConnector for AsterWs {
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
            exchange: Exchange::AsterF,
            market_type: MarketType::Perp,
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 1,
            qty_scale: 3,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn subscribe_payload_lowercases_and_appends_stream() {
        let ws = AsterWs::futures();
        let s1 = spec("BTCUSDT");
        let s2 = spec("ETHUSDT");
        let payload = ws.subscribe_payload(&[&s1, &s2]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["method"], "SUBSCRIBE");
        assert_eq!(v["params"][0], "btcusdt@aggTrade");
        assert_eq!(v["params"][1], "ethusdt@aggTrade");
    }

    #[test]
    fn urls_are_aster_hosts() {
        assert!(AsterWs::futures().ws_url().contains("fstream.asterdex.com"));
        assert!(AsterWs::spot().ws_url().contains("sstream.asterdex.com"));
    }
}
