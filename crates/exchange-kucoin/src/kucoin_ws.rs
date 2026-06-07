//! KuCoin WS connector. Unlike other venues KuCoin has NO static WS URL —
//! the session must first `POST /api/v1/bullet-public` to get a one-time
//! `token` + dynamic `endpoint`, then connect to
//! `wss://{endpoint}?token=..&connectId=..`. This connector exposes
//! `fetch_bullet()` (async) + symbol/topic helpers; `kucoin_session` drives
//! the dynamic connect, sharding, comma-joined subscribe and JSON ping.

use exchange_core::{ExchangeError, PingKind, Result, SymbolSpec, WsConnector};

use crate::instruments_info::{FUTURES_BASE, SPOT_BASE};
use crate::scale::to_kucoin_symbol;

pub struct KucoinBullet {
    pub endpoint: String,
    pub token: String,
    pub ping_interval_ms: u64,
}

pub struct KucoinWs {
    is_futures: bool,
    client: reqwest::Client,
}

impl KucoinWs {
    pub fn perp() -> Self {
        Self::new(true)
    }
    pub fn spot() -> Self {
        Self::new(false)
    }
    fn new(is_futures: bool) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("trading-terminal-clusters/kucoin-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { is_futures, client }
    }

    pub fn is_futures(&self) -> bool {
        self.is_futures
    }

    fn base(&self) -> &'static str {
        if self.is_futures {
            FUTURES_BASE
        } else {
            SPOT_BASE
        }
    }

    /// Subscribe topic prefix; venue symbols are appended comma-joined.
    pub fn exec_topic_prefix(&self) -> &'static str {
        if self.is_futures {
            "/contractMarket/execution:"
        } else {
            "/market/match:"
        }
    }

    /// Canonical `BTCUSDT` → KuCoin venue symbol (`XBTUSDTM` / `BTC-USDT`).
    pub fn venue_symbol(&self, canonical: &str) -> String {
        to_kucoin_symbol(canonical, self.is_futures)
    }

    /// `POST /api/v1/bullet-public` → (endpoint, token, pingInterval). No auth.
    pub async fn fetch_bullet(&self) -> Result<KucoinBullet> {
        let url = format!("{}/api/v1/bullet-public", self.base());
        let resp = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        if !status.is_success() {
            return Err(ExchangeError::Network(format!("bullet HTTP {status}: {body}")));
        }
        let json: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| ExchangeError::Parse(e.to_string()))?;
        if json.get("code").and_then(|x| x.as_str()) != Some("200000") {
            return Err(ExchangeError::Parse(format!("bullet code != 200000: {body}")));
        }
        let data = json
            .get("data")
            .ok_or_else(|| ExchangeError::Parse("bullet: no data".into()))?;
        let token = data
            .get("token")
            .and_then(|x| x.as_str())
            .ok_or_else(|| ExchangeError::Parse("bullet: no token".into()))?
            .to_owned();
        let server = data
            .get("instanceServers")
            .and_then(|x| x.as_array())
            .and_then(|a| a.first())
            .ok_or_else(|| ExchangeError::Parse("bullet: no instanceServers".into()))?;
        let endpoint = server
            .get("endpoint")
            .and_then(|x| x.as_str())
            .ok_or_else(|| ExchangeError::Parse("bullet: no endpoint".into()))?
            .to_owned();
        let ping_interval_ms = server
            .get("pingInterval")
            .and_then(|x| x.as_u64())
            .unwrap_or(18_000);
        Ok(KucoinBullet {
            endpoint,
            token,
            ping_interval_ms,
        })
    }
}

// WsConnector impl is mostly nominal — the supervisor holds Arc<dyn WsConnector>
// and downcasts to KucoinWs; the real connect logic lives in kucoin_session.
impl WsConnector for KucoinWs {
    fn ws_url(&self) -> &str {
        self.base() // informational; session uses fetch_bullet()
    }
    fn subscribe_payload(&self, _symbols: &[&SymbolSpec]) -> String {
        String::new() // session builds comma-joined subscribes per shard
    }
    fn ping_interval_ms(&self) -> u64 {
        15_000
    }
    fn ping_payload(&self) -> PingKind {
        PingKind::Text(r#"{"type":"ping"}"#)
    }
    fn pong_timeout_ms(&self) -> u64 {
        40_000
    }
    fn max_subscriptions_per_socket(&self) -> usize {
        300 // KuCoin closes >~400 with code 509; shard well under
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn topic_and_venue() {
        let f = KucoinWs::perp();
        assert_eq!(f.exec_topic_prefix(), "/contractMarket/execution:");
        assert_eq!(f.venue_symbol("BTCUSDT"), "XBTUSDTM");
        let s = KucoinWs::spot();
        assert_eq!(s.exec_topic_prefix(), "/market/match:");
        assert_eq!(s.venue_symbol("BTCUSDT"), "BTC-USDT");
    }
}
