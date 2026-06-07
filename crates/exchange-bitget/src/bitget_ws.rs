//! WsConnector для Bitget V2 public.
//!
//! Endpoint (один на spot и perp): `wss://ws.bitget.com/v2/ws/public`.
//!
//! Subscribe envelope (канал `trade`, по instId):
//!   `{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"trade","instId":"BTCUSDT"}, ...]}`
//!   instType = "USDT-FUTURES" (perp) | "SPOT" (spot); instId = canonical symbol.
//!
//! Heartbeat: Bitget рвёт idle через ~30с; клиент шлёт плоский текст `ping`
//! каждые ~15с (Bitget отвечает плоским `pong`). Аналогично OKX.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

pub const WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";

/// Консервативный батч на один subscribe-фрейм.
const MAX_ARGS_PER_SUBSCRIBE: usize = 100;

pub struct BitgetWs {
    is_perp: bool,
}

impl BitgetWs {
    pub fn perp() -> Self {
        Self { is_perp: true }
    }
    pub fn spot() -> Self {
        Self { is_perp: false }
    }

    fn inst_type(&self) -> &'static str {
        if self.is_perp {
            "USDT-FUTURES"
        } else {
            "SPOT"
        }
    }

    /// Дробит symbol-set на batched subscribe-фреймы. Session шлёт КАЖДЫЙ
    /// отдельным WS-frame'ом (см. `bitget_session`).
    pub fn subscribe_payloads_batched(&self, symbols: &[&SymbolSpec]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        let inst_type = self.inst_type();
        symbols
            .chunks(MAX_ARGS_PER_SUBSCRIBE)
            .map(|chunk| {
                let args: Vec<serde_json::Value> = chunk
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "instType": inst_type,
                            "channel": "trade",
                            "instId": s.symbol.to_uppercase(),
                        })
                    })
                    .collect();
                serde_json::json!({ "op": "subscribe", "args": args }).to_string()
            })
            .collect()
    }
}

impl WsConnector for BitgetWs {
    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String {
        self.subscribe_payloads_batched(symbols)
            .into_iter()
            .next()
            .unwrap_or_else(|| r#"{"op":"subscribe","args":[]}"#.to_string())
    }

    fn ping_interval_ms(&self) -> u64 {
        15_000
    }

    fn ping_payload(&self) -> PingKind {
        // Bitget требует плоский текст "ping"; ответит "pong".
        PingKind::Text("ping")
    }

    fn pong_timeout_ms(&self) -> u64 {
        30_000
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

    fn spec(symbol: &str, perp: bool) -> SymbolSpec {
        SymbolSpec {
            exchange: if perp { Exchange::BitgetF } else { Exchange::Bitget },
            market_type: if perp { MarketType::Perp } else { MarketType::Spot },
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 1,
            qty_scale: 3,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn perp_subscribe_uses_futures_insttype() {
        let ws = BitgetWs::perp();
        let s = spec("BTCUSDT", true);
        let payload = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"][0]["instType"], "USDT-FUTURES");
        assert_eq!(v["args"][0]["channel"], "trade");
        assert_eq!(v["args"][0]["instId"], "BTCUSDT");
    }

    #[test]
    fn spot_subscribe_uses_spot_insttype() {
        let ws = BitgetWs::spot();
        let s = spec("ETHUSDC", false);
        let payload = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["args"][0]["instType"], "SPOT");
        assert_eq!(v["args"][0]["instId"], "ETHUSDC");
    }

    #[test]
    fn batches_at_limit() {
        let ws = BitgetWs::perp();
        let specs: Vec<SymbolSpec> = (0..250).map(|i| spec(&format!("S{i}USDT"), true)).collect();
        let refs: Vec<&SymbolSpec> = specs.iter().collect();
        let payloads = ws.subscribe_payloads_batched(&refs);
        assert_eq!(payloads.len(), 3); // 100 + 100 + 50
        let last: serde_json::Value = serde_json::from_str(&payloads[2]).unwrap();
        assert_eq!(last["args"].as_array().unwrap().len(), 50);
    }

    #[test]
    fn ping_is_client_text() {
        let ws = BitgetWs::perp();
        assert!(matches!(ws.ping_payload(), PingKind::Text("ping")));
    }
}
