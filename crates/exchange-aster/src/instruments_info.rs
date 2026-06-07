//! REST instruments + 24h volume for Aster (asterdex.com).
//!
//! Aster's REST is bit-for-bit Binance (same JSON shape/fields), differing
//! only in hosts and the spot path version:
//!   - perp:  `GET https://fapi.asterdex.com/fapi/v1/exchangeInfo`
//!   - spot:  `GET https://sapi.asterdex.com/api/v1/exchangeInfo`  (v1, not v3)
//!
//! Scale = `count_decimals_trimmed(PRICE_FILTER.tickSize / LOT_SIZE.stepSize)`,
//! exactly like Binance and the terminal live engine — so server prices/qty
//! match byte-for-byte. Linear: no contract multiplier.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{count_decimals_trimmed, parse_scaled};

const FUTURES_BASE_URL: &str = "https://fapi.asterdex.com";
const SPOT_BASE_URL: &str = "https://sapi.asterdex.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsterCategory {
    /// USDT/USDC linear perpetual futures.
    Perp,
    /// Spot pairs (USDT/USDC quote).
    Spot,
}

impl AsterCategory {
    fn is_perp(self) -> bool {
        matches!(self, AsterCategory::Perp)
    }
}

pub struct AsterInstrumentsInfo {
    futures_base_url: String,
    spot_base_url: String,
    category: AsterCategory,
    client: reqwest::Client,
}

impl AsterInstrumentsInfo {
    pub fn new(category: AsterCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("trading-terminal-clusters/aster-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            futures_base_url: FUTURES_BASE_URL.to_string(),
            spot_base_url: SPOT_BASE_URL.to_string(),
            category,
            client,
        }
    }

    fn instruments_url(&self) -> String {
        match self.category {
            AsterCategory::Perp => format!("{}/fapi/v1/exchangeInfo", self.futures_base_url),
            AsterCategory::Spot => format!("{}/api/v1/exchangeInfo", self.spot_base_url),
        }
    }

    fn tickers_url(&self) -> String {
        match self.category {
            AsterCategory::Perp => format!("{}/fapi/v1/ticker/24hr", self.futures_base_url),
            AsterCategory::Spot => format!("{}/api/v1/ticker/24hr", self.spot_base_url),
        }
    }
}

#[async_trait]
impl ExchangeInfo for AsterInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let url = self.instruments_url();
        let resp = self
            .client
            .get(&url)
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
        parse_exchange_info(&body, self.category)
    }
}

#[async_trait]
impl VolumeRanker for AsterInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        let url = self.tickers_url();
        let resp = self
            .client
            .get(&url)
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
        parse_24h_quote_volumes(&body)
    }
}

pub(crate) fn parse_24h_quote_volumes(json: &str) -> Result<HashMap<String, f64>> {
    let arr: Vec<serde_json::Value> =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let mut out = HashMap::with_capacity(arr.len());
    for entry in arr {
        let Some(symbol) = entry.get("symbol").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(qv_str) = entry.get("quoteVolume").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(qv) = qv_str.parse::<f64>() else {
            continue;
        };
        out.insert(symbol.to_uppercase(), qv);
    }
    Ok(out)
}

pub(crate) fn parse_exchange_info(json: &str, category: AsterCategory) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let symbols = v
        .get("symbols")
        .and_then(|s| s.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing 'symbols' array".into()))?;

    let is_perp = category.is_perp();
    let (exchange, market_type) = if is_perp {
        (Exchange::AsterF, MarketType::Perp)
    } else {
        (Exchange::Aster, MarketType::Spot)
    };

    let mut out = Vec::with_capacity(symbols.len());
    for s in symbols {
        if s.get("status").and_then(|v| v.as_str()).unwrap_or("") != "TRADING" {
            continue;
        }
        // Perp: exclude dated/delivery contracts if contractType is present.
        // (Aster may omit it — then everything trading is treated as perp.)
        if is_perp {
            if let Some(ct) = s.get("contractType").and_then(|v| v.as_str()) {
                if ct != "PERPETUAL" {
                    continue;
                }
            }
        } else if let Some(allowed) = s.get("isSpotTradingAllowed").and_then(|v| v.as_bool()) {
            if !allowed {
                continue;
            }
        }

        let quote = match s.get("quoteAsset").and_then(|v| v.as_str()) {
            Some("USDT") => Quote::Usdt,
            Some("USDC") => Quote::Usdc,
            _ => continue,
        };

        let symbol = match s.get("symbol").and_then(|v| v.as_str()) {
            Some(s) => s.to_uppercase(),
            None => continue,
        };

        let filters = match s.get("filters").and_then(|v| v.as_array()) {
            Some(f) => f,
            None => {
                tracing::warn!(%symbol, "aster skip: filters missing");
                continue;
            }
        };
        let mut tick_str = "";
        let mut step_str = "";
        for f in filters {
            match f.get("filterType").and_then(|v| v.as_str()).unwrap_or("") {
                "PRICE_FILTER" => {
                    if let Some(t) = f.get("tickSize").and_then(|v| v.as_str()) {
                        tick_str = t;
                    }
                }
                "LOT_SIZE" => {
                    if let Some(t) = f.get("stepSize").and_then(|v| v.as_str()) {
                        step_str = t;
                    }
                }
                _ => {}
            }
        }
        if tick_str.is_empty() || step_str.is_empty() {
            tracing::warn!(%symbol, "aster skip: missing PRICE_FILTER or LOT_SIZE");
            continue;
        }
        let price_scale = count_decimals_trimmed(tick_str);
        let qty_scale = count_decimals_trimmed(step_str);
        let tick_size = match parse_scaled(tick_str, price_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%symbol, tick = tick_str, error = %e, "aster skip: bad tickSize");
                continue;
            }
        };
        let step_size = match parse_scaled(step_str, qty_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%symbol, step = step_str, error = %e, "aster skip: bad stepSize");
                continue;
            }
        };
        if tick_size <= 0 || step_size <= 0 {
            tracing::warn!(%symbol, "aster skip: zero tick/step");
            continue;
        }

        out.push(SymbolSpec {
            exchange,
            market_type,
            quote,
            symbol,
            price_scale,
            qty_scale,
            tick_size,
            step_size,
        });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const PERP_FIXTURE: &str = r#"
    {
      "symbols": [
        {"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT",
         "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},
        {"symbol":"ETHUSDC","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDC",
         "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]},
        {"symbol":"BTCUSDT_241227","contractType":"CURRENT_QUARTER","status":"TRADING","quoteAsset":"USDT","filters":[]},
        {"symbol":"BTCBUSD","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"BUSD","filters":[]}
      ]
    }"#;

    #[test]
    fn parses_perp_usdt_usdc_filters_dated_and_other_quotes() {
        let specs = parse_exchange_info(PERP_FIXTURE, AsterCategory::Perp).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::AsterF);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.price_scale, 1); // "0.10"
        assert_eq!(btc.tick_size, 1);
        assert_eq!(btc.qty_scale, 3);
    }

    const SPOT_FIXTURE: &str = r#"
    {
      "symbols": [
        {"symbol":"BTCUSDT","status":"TRADING","quoteAsset":"USDT","isSpotTradingAllowed":true,
         "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.00001"}]},
        {"symbol":"LEVUPUSDT","status":"TRADING","quoteAsset":"USDT","isSpotTradingAllowed":false,"filters":[]}
      ]
    }"#;

    #[test]
    fn parses_spot_filters_leveraged() {
        let specs = parse_exchange_info(SPOT_FIXTURE, AsterCategory::Spot).unwrap();
        assert_eq!(specs.len(), 1);
        let btc = &specs[0];
        assert_eq!(btc.exchange, Exchange::Aster);
        assert_eq!(btc.market_type, MarketType::Spot);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 5);
    }

    #[test]
    fn rejects_unparseable() {
        assert!(parse_exchange_info("nope", AsterCategory::Perp).is_err());
        assert!(parse_exchange_info("{}", AsterCategory::Perp).is_err());
    }
}
