use std::collections::HashMap;

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

/// Ranking of symbols by 24h quote-currency volume — used by the
/// supervisor to pick the top-N most active symbols when `top_n` is
/// configured. Returned values are notional in the quote currency
/// (USDT-margined returns USDT volume, USDC-margined returns USDC).
/// f64 is enough precision for ordering — the value is never used for
/// settlement math.
#[async_trait]
pub trait VolumeRanker: Send + Sync {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>>;
}

#[derive(Debug, Clone)]
pub enum PingKind {
    Protocol,
    Text(&'static str),
    ServerInitiated,
}

pub trait WsConnector: Send + Sync + std::any::Any {
    fn ws_url(&self) -> &str;

    fn subscribe_payload(&self, symbols: &[&SymbolSpec]) -> String;

    fn ping_interval_ms(&self) -> u64;

    fn ping_payload(&self) -> PingKind;

    fn pong_timeout_ms(&self) -> u64;

    fn max_subscriptions_per_socket(&self) -> usize;

    /// Downcast escape-hatch. Нужен для exchange-specific session-
    /// runner'ов, которые принимают конкретный тип connector'а (например
    /// `&BybitWs` для `bybit_session::run_session` — там есть методы
    /// `subscribe_payloads_batched`, отсутствующие в общем trait'е).
    /// Trait-объекты не могут вызвать inherent-методы реализации без
    /// downcast, и без `Self: Sized` ограничения в default impl метод
    /// был бы недоступен через `&dyn WsConnector`. Поэтому делаем
    /// обязательной: каждый impl возвращает `self`.
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait TradeParser: Send + Sync {
    fn parse(&self, raw: &[u8], spec: &SymbolSpec) -> Result<Option<TradePrint>>;
}
