//! Hyperliquid WS connector. Single endpoint `wss://api.hyperliquid.xyz/ws`;
//! subscribe one coin per message `{"method":"subscribe","subscription":
//! {"type":"trades","coin":"BTC"}}`, keepalive `{"method":"ping"}` (~14s).
//! Real connect/subscribe lives in `hyperliquid_session`; this exists for the
//! supervisor's `Arc<dyn WsConnector>` + the canonical→native lookup.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

use crate::scale::get_native;

pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

pub struct HyperliquidWs;

impl HyperliquidWs {
    pub fn new() -> Self {
        Self
    }
    /// Canonical `BTCUSDC` → native wire coin `BTC` (case-preserving via map).
    pub fn native_coin(&self, canonical: &str) -> String {
        get_native(canonical)
    }
}

impl Default for HyperliquidWs {
    fn default() -> Self {
        Self::new()
    }
}

impl WsConnector for HyperliquidWs {
    fn ws_url(&self) -> &str {
        WS_URL
    }
    fn subscribe_payload(&self, _symbols: &[&SymbolSpec]) -> String {
        String::new() // session subscribes one coin per message
    }
    fn ping_interval_ms(&self) -> u64 {
        14_000
    }
    fn ping_payload(&self) -> PingKind {
        PingKind::Text(r#"{"method":"ping"}"#)
    }
    fn pong_timeout_ms(&self) -> u64 {
        30_000
    }
    fn max_subscriptions_per_socket(&self) -> usize {
        1000
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
