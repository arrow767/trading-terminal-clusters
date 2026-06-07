//! REST instruments for Hyperliquid perps.
//!
//! `POST https://api.hyperliquid.xyz/info {"type":"meta"}` → `universe[]` with
//! `name` + `szDecimals`. System (canonical) symbol = `{NAME}USDC` (HL perps are
//! USDC-margined). Fixed MAX-precision scale: price_scale = 6 - szDecimals,
//! qty_scale = szDecimals (per prod decision). Native perps only (no HIP-3).

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{set_native, MAX_DECIMALS_PERP};

const HL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

pub struct HyperliquidInstrumentsInfo {
    client: reqwest::Client,
}

impl Default for HyperliquidInstrumentsInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperliquidInstrumentsInfo {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("trading-terminal-clusters/hyperliquid-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { client }
    }
}

#[async_trait]
impl ExchangeInfo for HyperliquidInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let resp = self
            .client
            .post(HL_INFO_URL)
            .json(&serde_json::json!({ "type": "meta" }))
            .send()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        if !status.is_success() {
            return Err(ExchangeError::Network(format!("HTTP {status}: {body}")));
        }
        parse_meta(&body)
    }
}

#[async_trait]
impl VolumeRanker for HyperliquidInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        Ok(HashMap::new()) // unused (top_n=None + rank_by=alphabetical)
    }
}

pub(crate) fn parse_meta(json: &str) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let universe = v
        .get("universe")
        .and_then(|u| u.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing universe".into()))?;
    let mut out = Vec::with_capacity(universe.len());
    for item in universe {
        // Skip delisted perps if flagged.
        if item.get("isDelisted").and_then(|x| x.as_bool()) == Some(true) {
            continue;
        }
        let Some(name) = item.get("name").and_then(|x| x.as_str()) else {
            continue;
        };
        let sz_decimals = item.get("szDecimals").and_then(|x| x.as_u64()).unwrap_or(0) as u8;
        let canonical = format!("{}USDC", name.to_uppercase());
        set_native(&canonical, name);
        let price_scale = MAX_DECIMALS_PERP.saturating_sub(sz_decimals);
        out.push(SymbolSpec {
            exchange: Exchange::Hyperliquid,
            market_type: MarketType::Perp,
            quote: Quote::Usdc,
            symbol: canonical,
            price_scale,
            qty_scale: sz_decimals,
            tick_size: 1,
            step_size: 1,
        });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const META: &str = r#"
    {"universe":[
      {"name":"BTC","szDecimals":5},
      {"name":"SOL","szDecimals":2},
      {"name":"kPEPE","szDecimals":0},
      {"name":"DEAD","szDecimals":2,"isDelisted":true}
    ]}"#;

    #[test]
    fn parses_meta_max_precision() {
        let specs = parse_meta(META).unwrap();
        assert_eq!(specs.len(), 3); // DEAD delisted skipped
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDC").unwrap();
        assert_eq!(btc.exchange, Exchange::Hyperliquid);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdc);
        // price_scale = 6 - 5 = 1; qty_scale = 5
        assert_eq!(btc.price_scale, 1);
        assert_eq!(btc.qty_scale, 5);
        let pepe = specs.iter().find(|s| s.symbol == "KPEPEUSDC").unwrap();
        assert_eq!(pepe.price_scale, 6); // 6 - 0
        assert_eq!(crate::scale::get_native("KPEPEUSDC"), "kPEPE");
    }
}
