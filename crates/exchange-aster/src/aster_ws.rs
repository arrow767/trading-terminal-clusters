//! WsConnector для Aster (asterdex.com) — клон Binance.
//!
//! Используем RAW `/ws` endpoint + метод SUBSCRIBE (не combined `/stream`):
//! на Aster futures плоский `/stream`+SUBSCRIBE отдаёт ack, но НЕ шлёт кадры
//! (та же граблю, что Binance Futures чинит через `/market/stream`). А `/ws`
//! доставляет raw-aggTrade и работает на scale. `BinanceFuturesTradeParser`
//! принимает и raw, и wrapped формат, так что парсинг переиспользуется.
//!
//! Aster отклоняет ОДИН SUBSCRIBE с >~100 params (close 3001 "illegal
//! request"), поэтому дробим на батчи и шлём с паузой (см. `aster_session`).
//!
//! Hosts:
//!   - perp: `wss://fstream.asterdex.com/ws`
//!   - spot: `wss://sstream.asterdex.com/ws`

use exchange_core::{PingKind, SymbolSpec, WsConnector};

pub const FUTURES_WS_URL: &str = "wss://fstream.asterdex.com/ws";
pub const SPOT_WS_URL: &str = "wss://sstream.asterdex.com/ws";

/// Aster отклоняет одиночный SUBSCRIBE с >~120 params — держим консервативно.
const MAX_PARAMS_PER_SUBSCRIBE: usize = 100;

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

    /// Дробит symbol-set на batched SUBSCRIBE-фреймы (`@aggTrade`). Session
    /// шлёт каждый отдельным WS-frame'ом с паузой между ними.
    pub fn subscribe_payloads_batched(&self, symbols: &[&SymbolSpec]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        symbols
            .chunks(MAX_PARAMS_PER_SUBSCRIBE)
            .enumerate()
            .map(|(idx, chunk)| {
                let params: Vec<String> = chunk
                    .iter()
                    .map(|s| format!("{}@aggTrade", s.symbol.to_lowercase()))
                    .collect();
                serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": idx + 1,
                })
                .to_string()
            })
            .collect()
    }
}

impl WsConnector for AsterWs {
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String {
        self.subscribe_payloads_batched(symbols)
            .into_iter()
            .next()
            .unwrap_or_else(|| r#"{"method":"SUBSCRIBE","params":[],"id":1}"#.to_string())
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
        1024
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
    fn subscribe_lowercases_and_batches() {
        let ws = AsterWs::futures();
        let specs: Vec<SymbolSpec> = (0..250).map(|i| spec(&format!("S{i}USDT"))).collect();
        let refs: Vec<&SymbolSpec> = specs.iter().collect();
        let payloads = ws.subscribe_payloads_batched(&refs);
        assert_eq!(payloads.len(), 3); // 100 + 100 + 50
        let first: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
        assert_eq!(first["method"], "SUBSCRIBE");
        assert_eq!(first["params"][0], "s0usdt@aggTrade");
        assert_eq!(first["params"].as_array().unwrap().len(), 100);
        let last: serde_json::Value = serde_json::from_str(&payloads[2]).unwrap();
        assert_eq!(last["params"].as_array().unwrap().len(), 50);
    }

    #[test]
    fn urls_are_aster_ws_hosts() {
        assert!(AsterWs::futures().ws_url().contains("fstream.asterdex.com/ws"));
        assert!(AsterWs::spot().ws_url().contains("sstream.asterdex.com/ws"));
    }
}
