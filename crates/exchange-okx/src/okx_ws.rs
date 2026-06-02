//! WsConnector для OKX V5.
//!
//! Public endpoint (один на spot и swap):
//!   `wss://ws.okx.com:8443/ws/v5/public`
//!
//! Subscribe envelope (канал `trades`, по instId):
//!   `{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT-SWAP"}, ...]}`
//!
//! Heartbeat: OKX закрывает idle-коннект через 30с без активности. КЛИЕНТ
//! шлёт плоский текст `ping` каждые ~20с (OKX отвечает `pong`). См.
//! [`PingKind::Text`] — отправку делает `cluster-ingest::okx_session`.
//!
//! Batching: один subscribe-фрейм держит много args (лимит OKX по размеру
//! сообщения щедрый), но дробим по `MAX_ARGS_PER_SUBSCRIBE`, чтобы не упереться
//! в 64 KB и для симметрии с Bybit-путём.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

use crate::scale::to_okx_inst_id;

pub const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// Консервативный батч на один subscribe-фрейм.
const MAX_ARGS_PER_SUBSCRIBE: usize = 100;

pub struct OkxWs {
    is_swap: bool,
}

impl OkxWs {
    pub fn swap() -> Self {
        Self { is_swap: true }
    }
    pub fn spot() -> Self {
        Self { is_swap: false }
    }

    /// Дробит symbol-set на batched subscribe-фреймы. Session шлёт КАЖДЫЙ
    /// отдельным WS-frame'ом (см. `okx_session`).
    pub fn subscribe_payloads_batched(&self, symbols: &[&SymbolSpec]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        symbols
            .chunks(MAX_ARGS_PER_SUBSCRIBE)
            .map(|chunk| {
                let args: Vec<serde_json::Value> = chunk
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "channel": "trades",
                            "instId": to_okx_inst_id(&s.symbol, self.is_swap),
                        })
                    })
                    .collect();
                serde_json::json!({ "op": "subscribe", "args": args }).to_string()
            })
            .collect()
    }
}

impl WsConnector for OkxWs {
    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String {
        // Совместимость с trait'ом — только первый чанк. Session обязан
        // использовать `subscribe_payloads_batched` и отправить все.
        self.subscribe_payloads_batched(symbols)
            .into_iter()
            .next()
            .unwrap_or_else(|| r#"{"op":"subscribe","args":[]}"#.to_string())
    }

    fn ping_interval_ms(&self) -> u64 {
        20_000
    }

    fn ping_payload(&self) -> PingKind {
        // OKX требует плоский текст "ping"; ответит "pong".
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

    fn spec(symbol: &str, swap: bool) -> SymbolSpec {
        SymbolSpec {
            exchange: if swap { Exchange::OkxF } else { Exchange::Okx },
            market_type: if swap { MarketType::Perp } else { MarketType::Spot },
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 1,
            qty_scale: 3,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn swap_subscribe_uses_swap_instid() {
        let ws = OkxWs::swap();
        let s = spec("BTCUSDT", true);
        let payload = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"][0]["channel"], "trades");
        assert_eq!(v["args"][0]["instId"], "BTC-USDT-SWAP");
    }

    #[test]
    fn spot_subscribe_uses_spot_instid() {
        let ws = OkxWs::spot();
        let s = spec("ETHUSDC", false);
        let payload = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["args"][0]["instId"], "ETH-USDC");
    }

    #[test]
    fn batches_at_limit() {
        let ws = OkxWs::swap();
        let specs: Vec<SymbolSpec> = (0..250).map(|i| spec(&format!("S{i}USDT"), true)).collect();
        let refs: Vec<&SymbolSpec> = specs.iter().collect();
        let payloads = ws.subscribe_payloads_batched(&refs);
        assert_eq!(payloads.len(), 3); // 100 + 100 + 50
        let last: serde_json::Value = serde_json::from_str(&payloads[2]).unwrap();
        assert_eq!(last["args"].as_array().unwrap().len(), 50);
    }

    #[test]
    fn ping_is_client_text() {
        let ws = OkxWs::swap();
        assert!(matches!(ws.ping_payload(), PingKind::Text("ping")));
        assert_eq!(ws.ping_interval_ms(), 20_000);
    }
}
