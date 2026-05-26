use exchange_core::{PingKind, SymbolSpec, WsConnector};

/// Binance **spot** combined-stream WS. Отличия от futures:
/// - Хост: `stream.binance.com:9443` вместо `fstream.binance.com`.
/// - Spot не подвергался той же routed-path миграции что и USD-M futures
///   в 2026-05; `/stream` всё ещё штатный combined-endpoint и доставляет
///   данные нормально (проверено эмпирически).
/// - Server-side ping: на spot чаще, нежели на futures (раз в 20с против
///   3 мин на futures), pong-deadline ~1 минута. Tungstenite авто-pong'ит
///   ping-frame'ы (мы ставим Pong на receive в session-loop), так что
///   разница в кадансе не требует кастомного keepalive.
/// - Подписка: формат идентичен — `{"method":"SUBSCRIBE","params":[...]}`.
const DEFAULT_WS_URL: &str = "wss://stream.binance.com:9443/stream";

pub struct BinanceSpotWs {
    ws_url: String,
}

impl BinanceSpotWs {
    pub fn new() -> Self {
        Self {
            ws_url: DEFAULT_WS_URL.to_string(),
        }
    }

    pub fn with_url(url: impl Into<String>) -> Self {
        Self { ws_url: url.into() }
    }
}

impl Default for BinanceSpotWs {
    fn default() -> Self {
        Self::new()
    }
}

impl WsConnector for BinanceSpotWs {
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

    /// Spot шлёт server-side ping каждые ~20с; pong-deadline 1 минута.
    /// Указываем те же значения для consistency, но runtime не использует
    /// эти поля как трагический deadline — отвечаем на каждый ping pong'ом
    /// в session-loop'е, чего достаточно.
    fn ping_interval_ms(&self) -> u64 {
        20_000
    }

    fn ping_payload(&self) -> PingKind {
        PingKind::ServerInitiated
    }

    fn pong_timeout_ms(&self) -> u64 {
        60_000
    }

    /// Spot тоже комбайн-стрим — рабочий потолок 1024, но у нас тот же
    /// safety-cap, что и для futures (200 streams/socket), чтобы не
    /// упереться в throttle при resubscribe'ах.
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
            exchange: Exchange::Binance,
            market_type: MarketType::Spot,
            quote: Quote::Usdt,
            symbol: symbol.into(),
            price_scale: 8,
            qty_scale: 8,
            tick_size: 1_000_000,
            step_size: 1_000,
        }
    }

    #[test]
    fn subscribe_payload_lowercases() {
        let ws = BinanceSpotWs::new();
        let s = spec("BTCUSDT");
        let p = ws.subscribe_payload(&[&s]);
        let v: serde_json::Value = serde_json::from_str(&p).unwrap();
        assert_eq!(v["method"], "SUBSCRIBE");
        assert_eq!(v["params"][0], "btcusdt@aggTrade");
    }

    #[test]
    fn url_is_spot_endpoint() {
        let ws = BinanceSpotWs::new();
        assert!(ws.ws_url().contains("stream.binance.com:9443/stream"));
        // Ничего «routed» — у spot этого не было:
        assert!(!ws.ws_url().contains("/market/"));
    }
}
