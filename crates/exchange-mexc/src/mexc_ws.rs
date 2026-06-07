//! MEXC WS connector (nominal). Spot and futures are DIFFERENT protocols
//! (spot: protobuf on wbs-api; futures: JSON on contract.mexc.com/edge) —
//! the real connect/subscribe/parse logic lives in `mexc_session`, which
//! branches on `is_futures()`. This type exists so the supervisor can hold
//! `Arc<dyn WsConnector>` and downcast.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

use crate::scale::to_mexc_contract_symbol;

pub const SPOT_WS_URL: &str = "wss://wbs-api.mexc.com/ws";
pub const FUTURES_WS_URL: &str = "wss://contract.mexc.com/edge";

pub struct MexcWs {
    is_futures: bool,
}

impl MexcWs {
    pub fn perp() -> Self {
        Self { is_futures: true }
    }
    pub fn spot() -> Self {
        Self { is_futures: false }
    }
    pub fn is_futures(&self) -> bool {
        self.is_futures
    }

    /// Canonical `BTCUSDT` → the symbol form that appears in this venue's WS
    /// messages: futures `BTC_USDT`, spot `BTCUSDT`. Used as the routing key.
    pub fn venue_symbol(&self, canonical: &str) -> String {
        if self.is_futures {
            to_mexc_contract_symbol(canonical)
        } else {
            canonical.to_uppercase()
        }
    }
}

impl WsConnector for MexcWs {
    fn ws_url(&self) -> &str {
        if self.is_futures {
            FUTURES_WS_URL
        } else {
            SPOT_WS_URL
        }
    }
    fn subscribe_payload(&self, _symbols: &[&SymbolSpec]) -> String {
        String::new()
    }
    fn ping_interval_ms(&self) -> u64 {
        15_000
    }
    fn ping_payload(&self) -> PingKind {
        PingKind::ServerInitiated
    }
    fn pong_timeout_ms(&self) -> u64 {
        60_000
    }
    fn max_subscriptions_per_socket(&self) -> usize {
        100
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
