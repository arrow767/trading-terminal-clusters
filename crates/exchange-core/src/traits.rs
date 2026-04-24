use async_trait::async_trait;
use thiserror::Error;

use crate::types::{SymbolSpec, TradePrint};

#[derive(Debug, Error)]
pub enum ExchangeError {
    #[error("network error: {0}")]
    Network(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("rate limited until epoch_ms={until_ms}: {reason}")]
    RateLimited { until_ms: i64, reason: String },

    #[error("unexpected: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ExchangeError>;

#[async_trait]
pub trait ExchangeInfo: Send + Sync {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>>;
}

#[derive(Debug, Clone)]
pub enum PingKind {
    Protocol,
    Text(&'static str),
    ServerInitiated,
}

pub trait WsConnector: Send + Sync {
    fn ws_url(&self) -> &str;

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String;

    fn ping_interval_ms(&self) -> u64;

    fn ping_payload(&self) -> PingKind;

    fn pong_timeout_ms(&self) -> u64;

    fn max_subscriptions_per_socket(&self) -> usize;
}

pub trait TradeParser: Send + Sync {
    fn parse(&self, raw: &[u8], spec: &SymbolSpec) -> Result<Option<TradePrint>>;
}
