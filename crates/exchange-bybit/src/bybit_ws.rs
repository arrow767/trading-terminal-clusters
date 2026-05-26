//! WsConnector для Bybit V5.
//!
//! Public endpoints per-category:
//!   - linear: `wss://stream.bybit.com/v5/public/linear`
//!   - spot:   `wss://stream.bybit.com/v5/public/spot`
//!
//! Subscribe envelope:
//!   `{"op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT", ...]}`
//!
//! Heartbeat: КЛИЕНТ шлёт `{"op":"ping"}` каждые ~20 секунд. Сервер не
//! шлёт серверных WS-ping'ов (в отличие от Binance Futures), и без
//! клиентского ping'а соединение закроется по idle. См.
//! [`PingKind::Text`] — handler в `cluster-ingest::bybit_session`
//! отправляет это сам по таймеру.
//!
//! Subscribe-лимит: 10 args per одного `subscribe` сообщения (Bybit V5
//! docs). Если symbol-set больше 10 — клиент должен бить subscribe на
//! батчи (см. `subscribe_payloads_batched`).

use exchange_core::{PingKind, SymbolSpec, WsConnector};

pub const LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
pub const SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";

/// Bybit-side subscribe limit per message — 10 args. Превышаем → ошибка.
const MAX_ARGS_PER_SUBSCRIBE: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitCategory {
    Linear,
    Spot,
}

pub struct BybitWs {
    ws_url: String,
    category: BybitCategory,
}

impl BybitWs {
    pub fn linear() -> Self {
        Self {
            ws_url: LINEAR_WS_URL.to_string(),
            category: BybitCategory::Linear,
        }
    }
    pub fn spot() -> Self {
        Self {
            ws_url: SPOT_WS_URL.to_string(),
            category: BybitCategory::Spot,
        }
    }
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }
    pub fn category(&self) -> BybitCategory {
        self.category
    }

    /// Бьёт большой symbol-set на batched subscribe messages по
    /// `MAX_ARGS_PER_SUBSCRIBE` штук. Session должен отправить КАЖДЫЙ
    /// payload отдельным WS-frame'ом. Для совместимости с trait'овым
    /// `subscribe_payload(&[&SymbolSpec]) -> String` существует и
    /// сингл-метод ниже (склеивает первый чанк).
    pub fn subscribe_payloads_batched(&self, symbols: &[&SymbolSpec]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        symbols
            .chunks(MAX_ARGS_PER_SUBSCRIBE)
            .enumerate()
            .map(|(idx, chunk)| {
                let args: Vec<String> = chunk
                    .iter()
                    .map(|s| format!("publicTrade.{}", s.symbol))
                    .collect();
                serde_json::json!({
                    "op": "subscribe",
                    "args": args,
                    "req_id": format!("sub-{idx}"),
                })
                .to_string()
            })
            .collect()
    }
}

impl WsConnector for BybitWs {
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String {
        // Совместимость с trait'ом — возвращаем ТОЛЬКО первый чанк.
        // Session должен использовать `subscribe_payloads_batched`
        // и отправить все чанки. Если этого не сделать — подпишемся
        // только на первые 10 символов. Тест проверяет инвариант.
        self.subscribe_payloads_batched(symbols)
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                // 0 symbols → пустой subscribe (server проигнорирует)
                r#"{"op":"subscribe","args":[]}"#.to_string()
            })
    }

    fn ping_interval_ms(&self) -> u64 {
        20_000
    }

    fn ping_payload(&self) -> PingKind {
        // Bybit требует клиент-инициированный текст `{"op":"ping"}`.
        // Сервер ответит `{"op":"pong"}` — session игнорирует pong
        // (только обновляет last-rx таймер).
        PingKind::Text(r#"{"op":"ping"}"#)
    }

    fn pong_timeout_ms(&self) -> u64 {
        // Если не получили pong в течение 30с — считаем коннект stale.
        // (Bybit обычно отвечает быстро; этот таймаут — защита от тихих
        // half-closed соединений.)
        30_000
    }

    fn max_subscriptions_per_socket(&self) -> usize {
        // Bybit V5 docs: на одну connection до ~200 args суммарно
        // (через множественные subscribe-сообщения, кажд. по 10 args).
        // Берём 200 как практический cap. Для больших universe — multi-conn
        // shard'инг (TODO в supervisor).
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
            exchange: Exchange::BybitF,
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
    fn batches_subscribe_at_10_args() {
        let ws = BybitWs::linear();
        let specs: Vec<SymbolSpec> = (0..25).map(|i| spec(&format!("S{i}USDT"))).collect();
        let refs: Vec<&SymbolSpec> = specs.iter().collect();
        let payloads = ws.subscribe_payloads_batched(&refs);
        // 25 / 10 = 3 чанка (10 + 10 + 5).
        assert_eq!(payloads.len(), 3);
        let first: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
        assert_eq!(first["op"], "subscribe");
        assert_eq!(first["args"].as_array().unwrap().len(), 10);
        let last: serde_json::Value = serde_json::from_str(&payloads[2]).unwrap();
        assert_eq!(last["args"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn payload_topic_format_matches_docs() {
        let ws = BybitWs::linear();
        let s = spec("BTCUSDT");
        let payload = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["args"][0], "publicTrade.BTCUSDT");
    }

    #[test]
    fn ping_is_client_text() {
        let ws = BybitWs::linear();
        assert!(matches!(ws.ping_payload(), PingKind::Text(_)));
        assert_eq!(ws.ping_interval_ms(), 20_000);
    }

    #[test]
    fn empty_input_doesnt_panic() {
        let ws = BybitWs::linear();
        let payloads = ws.subscribe_payloads_batched(&[]);
        assert!(payloads.is_empty());
        // single-chunk path returns the fallback payload
        let v: serde_json::Value = serde_json::from_str(&ws.subscribe_payload(&[])).unwrap();
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"].as_array().unwrap().len(), 0);
    }
}
