//! REST instruments for BingX.
//!   - swap (USDT-M perp): `GET /openApi/swap/v2/quote/contracts` → `data[]`
//!     (`pricePrecision`/`quantityPrecision`/`size`). qty is BASE-asset, no
//!     contract multiplier. tick = 10^-pricePrecision (raw 1 at that scale).
//!   - spot: `GET /openApi/spot/v1/common/symbols` → `data.symbols[]` with
//!     explicit `tickSize`/`stepSize` (JSON numbers).
//! Wrapper `{code,msg,data}`; `status==1` = online. Canonical `BTCUSDT`.
//! Scale math mirrors the live engine (`bingx/rest.rs`) byte-for-byte.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{count_decimals_trimmed, fmt_decimal, parse_scaled};

pub const BINGX_BASE: &str = "https://open-api.bingx.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BingxCategory {
    Swap,
    Spot,
}

impl BingxCategory {
    pub fn is_swap(self) -> bool {
        matches!(self, BingxCategory::Swap)
    }
}

pub struct BingxInstrumentsInfo {
    category: BingxCategory,
    client: reqwest::Client,
}

impl BingxInstrumentsInfo {
    pub fn new(category: BingxCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("trading-terminal-clusters/bingx-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { category, client }
    }

    fn url(&self) -> String {
        match self.category {
            BingxCategory::Swap => format!("{BINGX_BASE}/openApi/swap/v2/quote/contracts"),
            BingxCategory::Spot => format!("{BINGX_BASE}/openApi/spot/v1/common/symbols"),
        }
    }
}

#[async_trait]
impl ExchangeInfo for BingxInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let resp = self
            .client
            .get(self.url())
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
        if self.category.is_swap() {
            parse_swap(&body)
        } else {
            parse_spot(&body)
        }
    }
}

#[async_trait]
impl VolumeRanker for BingxInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        Ok(HashMap::new()) // unused (top_n=None + rank_by=alphabetical)
    }
}

fn quote_of(venue: &str) -> Option<Quote> {
    if venue.ends_with("-USDT") {
        Some(Quote::Usdt)
    } else if venue.ends_with("-USDC") {
        Some(Quote::Usdc)
    } else {
        None
    }
}

/// Extract a number field that BingX may send as a JSON number OR a string.
fn num_str(v: Option<&serde_json::Value>) -> Option<String> {
    match v {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(fmt_decimal(n.as_f64().unwrap_or(0.0))),
        _ => None,
    }
}

fn ok_data(json: &str) -> Result<serde_json::Value> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("unknown");
        return Err(ExchangeError::Parse(format!("BingX API error: {msg}")));
    }
    Ok(v)
}

pub(crate) fn parse_swap(json: &str) -> Result<Vec<SymbolSpec>> {
    let v = ok_data(json)?;
    let list = v
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data array".into()))?;
    let mut out = Vec::with_capacity(list.len());
    for it in list {
        if it.get("status").and_then(|x| x.as_i64()).unwrap_or(0) != 1 {
            continue; // 1 = online
        }
        let venue = match it.get("symbol").and_then(|x| x.as_str()) {
            Some(s) => s.to_uppercase(),
            None => continue,
        };
        let quote = match quote_of(&venue) {
            Some(q) => q,
            None => continue,
        };
        let price_scale = it
            .get("pricePrecision")
            .and_then(|x| x.as_u64())
            .unwrap_or(2)
            .min(18) as u8;
        let qty_scale = it
            .get("quantityPrecision")
            .and_then(|x| x.as_u64())
            .unwrap_or(0)
            .min(18) as u8;
        let step_size = num_str(it.get("size"))
            .and_then(|s| parse_scaled(&s, qty_scale).ok())
            .unwrap_or(1)
            .max(1);
        out.push(SymbolSpec {
            exchange: Exchange::BingxF,
            market_type: MarketType::Perp,
            quote,
            symbol: venue.replace('-', ""),
            price_scale,
            qty_scale,
            tick_size: 1, // 10^-pricePrecision == 1 unit at price_scale
            step_size,
        });
    }
    Ok(out)
}

pub(crate) fn parse_spot(json: &str) -> Result<Vec<SymbolSpec>> {
    let v = ok_data(json)?;
    let list = v
        .get("data")
        .and_then(|d| d.get("symbols"))
        .and_then(|s| s.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data.symbols array".into()))?;
    let mut out = Vec::with_capacity(list.len());
    for it in list {
        if it.get("status").and_then(|x| x.as_i64()).unwrap_or(0) != 1 {
            continue;
        }
        let venue = match it.get("symbol").and_then(|x| x.as_str()) {
            Some(s) => s.to_uppercase(),
            None => continue,
        };
        let quote = match quote_of(&venue) {
            Some(q) => q,
            None => continue,
        };
        let tick_str = num_str(it.get("tickSize")).unwrap_or_else(|| "0.01".into());
        let step_str = num_str(it.get("stepSize")).unwrap_or_else(|| "0.000001".into());
        let price_scale = count_decimals_trimmed(&tick_str);
        let tick_size = parse_scaled(&tick_str, price_scale).unwrap_or(1).max(1);
        let qty_scale = count_decimals_trimmed(&step_str);
        let step_size = parse_scaled(&step_str, qty_scale).unwrap_or(1).max(1);
        out.push(SymbolSpec {
            exchange: Exchange::Bingx,
            market_type: MarketType::Spot,
            quote,
            symbol: venue.replace('-', ""),
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

    const SWAP: &str = r#"{"code":0,"msg":"","data":[
      {"symbol":"BTC-USDT","pricePrecision":1,"quantityPrecision":4,"size":"0.0001","status":1},
      {"symbol":"ETH-USDC","pricePrecision":2,"quantityPrecision":3,"size":"0.001","status":1},
      {"symbol":"OFF-USDT","pricePrecision":2,"quantityPrecision":2,"size":"0.01","status":0},
      {"symbol":"FOO-BUSD","pricePrecision":2,"quantityPrecision":2,"size":"0.01","status":1}
    ]}"#;

    const SPOT: &str = r#"{"code":0,"msg":"","data":{"symbols":[
      {"symbol":"BTC-USDT","tickSize":0.01,"stepSize":0.000001,"status":1},
      {"symbol":"RIVER-USDT","tickSize":0.05,"stepSize":0.01,"status":1},
      {"symbol":"OFF-USDT","tickSize":0.01,"stepSize":0.01,"status":0}
    ]}}"#;

    #[test]
    fn parses_swap_no_contract_mult() {
        let s = parse_swap(SWAP).unwrap();
        assert_eq!(s.len(), 2); // OFF offline + BUSD quote excluded
        let btc = s.iter().find(|x| x.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::BingxF);
        assert_eq!(btc.price_scale, 1);
        assert_eq!(btc.qty_scale, 4);
        assert_eq!(btc.tick_size, 1);
        assert_eq!(btc.step_size, 1); // 0.0001 at scale 4
    }

    #[test]
    fn parses_spot_tick_step() {
        let s = parse_spot(SPOT).unwrap();
        assert_eq!(s.len(), 2);
        let river = s.iter().find(|x| x.symbol == "RIVERUSDT").unwrap();
        assert_eq!(river.exchange, Exchange::Bingx);
        assert_eq!(river.price_scale, 2); // 0.05 → 2 decimals
        assert_eq!(river.tick_size, 5); // 0.05 at scale 2
        assert_eq!(river.qty_scale, 2);
        assert_eq!(river.step_size, 1); // 0.01 at scale 2
    }
}
