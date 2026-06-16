//! BingX WS connector (nominal). Transport quirks live in `bingx_session`:
//! - separate URL per market (swap-market / market),
//! - every server data frame is GZIP-compressed binary → inflate before JSON,
//! - heartbeat is JSON `{"ping":"<uuid>","time":...}` → reply `{"pong":"<uuid>",...}`,
//! - subscribe one channel per message: `{"id","reqType":"sub","dataType":"<SYM>@trade"}`.

use exchange_core::{PingKind, SymbolSpec, WsConnector};

use crate::scale::to_bingx_symbol;

pub const SWAP_WS_URL: &str = "wss://open-api-swap.bingx.com/swap-market";
pub const SPOT_WS_URL: &str = "wss://open-api-ws.bingx.com/market";

pub struct BingxWs {
    is_futures: bool,
}

impl BingxWs {
    pub fn perp() -> Self {
        Self { is_futures: true }
    }
    pub fn spot() -> Self {
        Self { is_futures: false }
    }
    pub fn is_futures(&self) -> bool {
        self.is_futures
    }
    /// Canonical `BTCUSDT` → BingX wire symbol `BTC-USDT` (used in `dataType`).
    pub fn venue_symbol(&self, canonical: &str) -> String {
        to_bingx_symbol(canonical)
    }
}

impl WsConnector for BingxWs {
    fn ws_url(&self) -> &str {
        if self.is_futures {
            SWAP_WS_URL
        } else {
            SPOT_WS_URL
        }
    }
    fn subscribe_payload(&self, _symbols: &[&SymbolSpec]) -> String {
        String::new() // session subscribes one channel per message
    }
    fn ping_interval_ms(&self) -> u64 {
        0 // server-initiated: BingX pings us, we pong (handled in session)
    }
    fn ping_payload(&self) -> PingKind {
        PingKind::ServerInitiated
    }
    fn pong_timeout_ms(&self) -> u64 {
        60_000
    }
    fn max_subscriptions_per_socket(&self) -> usize {
        150
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
