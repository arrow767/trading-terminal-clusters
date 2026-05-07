use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level configuration loaded from `cluster-ingest.toml`. Missing
/// fields fall back to documented defaults so a stripped-down config
/// stays usable in dev. The bin treats `Config::default()` as a valid
/// "no config provided" outcome.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_ch_url")]
    pub clickhouse_url: String,
    #[serde(default = "default_window_ms")]
    pub window_ms: i64,
    #[serde(default = "default_diff_interval_ms")]
    pub diff_interval_ms: i64,
    #[serde(default = "default_agg_tick_ms")]
    pub agg_tick_interval_ms: u64,
    #[serde(default = "default_trade_channel_bound")]
    pub trade_channel_bound: usize,
    #[serde(default = "default_ch_channel_bound")]
    pub ch_channel_bound: usize,
    #[serde(default = "default_grpc_listen")]
    pub grpc_listen: String,
    #[serde(default)]
    pub exchanges: ExchangesConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            clickhouse_url: default_ch_url(),
            window_ms: default_window_ms(),
            diff_interval_ms: default_diff_interval_ms(),
            agg_tick_interval_ms: default_agg_tick_ms(),
            trade_channel_bound: default_trade_channel_bound(),
            ch_channel_bound: default_ch_channel_bound(),
            grpc_listen: default_grpc_listen(),
            exchanges: ExchangesConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExchangesConfig {
    pub binance_perp: Option<BinancePerpConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinancePerpConfig {
    #[serde(default = "true_")]
    pub enabled: bool,

    /// Quote currencies to keep. Anything not in this list is dropped at
    /// the filter stage. Defaults to USDT+USDC.
    #[serde(default = "default_include_quotes")]
    pub include_quotes: Vec<String>,

    /// If non-empty, ONLY these symbols are subscribed (after quote
    /// filter). Use to lock dev to a small fixed set; leave empty in
    /// prod so auto-discovery picks up new listings.
    #[serde(default)]
    pub allow: Vec<String>,

    /// Symbols to skip even if they otherwise match.
    #[serde(default)]
    pub deny: Vec<String>,

    /// If set, truncate the filtered list to this size (used with the
    /// API-default ordering — currently alphabetical-ish from
    /// exchangeInfo). Useful for dev caps; leave unset in prod.
    pub top_n: Option<usize>,

    /// How often to re-fetch exchangeInfo and reconcile the symbol set.
    /// New listings appear at this cadence at worst; defaults to 5 min.
    #[serde(default = "default_discovery_poll_secs")]
    pub discovery_poll_secs: u64,

    #[serde(default = "default_ws_connect_timeout_ms")]
    pub ws_connect_timeout_ms: u64,

    #[serde(default = "default_backoff_min_ms")]
    pub reconnect_backoff_min_ms: u64,

    #[serde(default = "default_backoff_max_ms")]
    pub reconnect_backoff_max_ms: u64,
}

impl Default for BinancePerpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            include_quotes: default_include_quotes(),
            allow: Vec::new(),
            deny: Vec::new(),
            top_n: None,
            discovery_poll_secs: default_discovery_poll_secs(),
            ws_connect_timeout_ms: default_ws_connect_timeout_ms(),
            reconnect_backoff_min_ms: default_backoff_min_ms(),
            reconnect_backoff_max_ms: default_backoff_max_ms(),
        }
    }
}

impl IngestConfig {
    pub fn agg_tick_interval(&self) -> Duration {
        Duration::from_millis(self.agg_tick_interval_ms)
    }
}

impl BinancePerpConfig {
    pub fn discovery_poll(&self) -> Duration {
        Duration::from_secs(self.discovery_poll_secs)
    }
    pub fn ws_connect_timeout(&self) -> Duration {
        Duration::from_millis(self.ws_connect_timeout_ms)
    }
    pub fn backoff_min(&self) -> Duration {
        Duration::from_millis(self.reconnect_backoff_min_ms)
    }
    pub fn backoff_max(&self) -> Duration {
        Duration::from_millis(self.reconnect_backoff_max_ms)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        Self::parse(&s)
    }

    pub fn parse(s: &str) -> Result<Self> {
        toml::from_str(s).context("parse cluster-ingest config")
    }
}

fn default_region() -> String {
    "tokyo".into()
}
fn default_ch_url() -> String {
    "http://127.0.0.1:8123".into()
}
fn default_window_ms() -> i64 {
    60_000
}
fn default_diff_interval_ms() -> i64 {
    200
}
fn default_agg_tick_ms() -> u64 {
    100
}
fn default_trade_channel_bound() -> usize {
    4_096
}
fn default_ch_channel_bound() -> usize {
    16_384
}
fn default_grpc_listen() -> String {
    "127.0.0.1:50051".into()
}
fn default_include_quotes() -> Vec<String> {
    vec!["USDT".into(), "USDC".into()]
}
fn default_discovery_poll_secs() -> u64 {
    300
}
fn default_ws_connect_timeout_ms() -> u64 {
    15_000
}
fn default_backoff_min_ms() -> u64 {
    500
}
fn default_backoff_max_ms() -> u64 {
    30_000
}
fn true_() -> bool {
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_yields_defaults() {
        let c = Config::parse("").unwrap();
        assert_eq!(c.ingest.region, "tokyo");
        assert_eq!(c.ingest.window_ms, 60_000);
        assert!(c.ingest.exchanges.binance_perp.is_none());
    }

    #[test]
    fn binance_perp_section_picked_up() {
        let toml_str = r#"
            [ingest]
            region = "singapore"
            window_ms = 30000

            [ingest.exchanges.binance_perp]
            allow = ["BTCUSDT", "ETHUSDT"]
            top_n = 50
        "#;
        let c = Config::parse(toml_str).unwrap();
        assert_eq!(c.ingest.region, "singapore");
        assert_eq!(c.ingest.window_ms, 30_000);
        let bp = c.ingest.exchanges.binance_perp.unwrap();
        assert_eq!(bp.allow, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(bp.top_n, Some(50));
        // Defaults preserved on unset fields:
        assert!(bp.enabled);
        assert_eq!(bp.include_quotes, vec!["USDT", "USDC"]);
        assert_eq!(bp.discovery_poll_secs, 300);
    }

    #[test]
    fn rejects_unknown_field_in_strict_mode() {
        // Default behavior: unknown fields are accepted (forward-compat).
        // We do not enable deny_unknown_fields, since silently dropping
        // a typo'd field is less bad than rejecting a config a future
        // version of this binary would understand.
        let c = Config::parse(
            r#"[ingest]
            something_new = 42
        "#,
        );
        assert!(c.is_ok());
    }
}
