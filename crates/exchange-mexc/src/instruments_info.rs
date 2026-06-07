//! REST instruments for MEXC.
//!   - spot:    `GET https://api.mexc.com/api/v3/exchangeInfo`
//!   - futures: `GET https://contract.mexc.com/api/v1/contract/detail`
//!
//! Spot has no PRICE_FILTER/LOT_SIZE: price_scale = `quotePrecision` (tick=1),
//! qty_scale = decimals(`baseSizePrecision`), status `"1"` = trading. Futures
//! qty in CONTRACTS → base via `contractSize` (CT map, perp only); effective
//! lot = volUnit × contractSize. Ported from the live engine.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{
    count_decimals_trimmed, decimal_fraction, fmt_decimal, multiply_decimal_strs, parse_scaled,
    set_ct,
};

pub const SPOT_BASE: &str = "https://api.mexc.com";
pub const FUTURES_BASE: &str = "https://contract.mexc.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MexcCategory {
    Perp,
    Spot,
}

impl MexcCategory {
    pub fn is_perp(self) -> bool {
        matches!(self, MexcCategory::Perp)
    }
}

pub struct MexcInstrumentsInfo {
    category: MexcCategory,
    client: reqwest::Client,
}

impl MexcInstrumentsInfo {
    pub fn new(category: MexcCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("trading-terminal-clusters/mexc-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { category, client }
    }

    fn url(&self) -> String {
        match self.category {
            MexcCategory::Perp => format!("{FUTURES_BASE}/api/v1/contract/detail"),
            MexcCategory::Spot => format!("{SPOT_BASE}/api/v3/exchangeInfo"),
        }
    }
}

#[async_trait]
impl ExchangeInfo for MexcInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let url = self.url();
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
        if self.category.is_perp() {
            parse_futures(&body)
        } else {
            parse_spot(&body)
        }
    }
}

#[async_trait]
impl VolumeRanker for MexcInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        Ok(HashMap::new()) // unused (top_n=None + rank_by=alphabetical)
    }
}

fn quote_of(s: Option<&str>) -> Option<Quote> {
    match s {
        Some("USDT") => Some(Quote::Usdt),
        Some("USDC") => Some(Quote::Usdc),
        _ => None,
    }
}

pub(crate) fn parse_spot(json: &str) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let list = v
        .get("symbols")
        .and_then(|x| x.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing symbols".into()))?;
    let mut out = Vec::with_capacity(list.len());
    for s in list {
        // MEXC spot status "1" = trading.
        if matches!(s.get("status").and_then(|x| x.as_str()), Some(st) if st != "1") {
            continue;
        }
        let quote = match quote_of(s.get("quoteAsset").and_then(|x| x.as_str())) {
            Some(q) => q,
            None => continue,
        };
        let symbol = match s.get("symbol").and_then(|x| x.as_str()) {
            Some(s) => s.to_uppercase(),
            None => continue,
        };
        // price decimals from quotePrecision (int); no PRICE_FILTER on MEXC.
        let Some(price_scale) = s
            .get("quotePrecision")
            .and_then(|x| x.as_u64())
            .map(|n| n.min(255) as u8)
        else {
            continue;
        };
        let step_str = s
            .get("baseSizePrecision")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("1");
        let qty_scale = count_decimals_trimmed(step_str);
        let step_size = parse_scaled(step_str, qty_scale).unwrap_or(1).max(1);
        out.push(SymbolSpec {
            exchange: Exchange::Mexc,
            market_type: MarketType::Spot,
            quote,
            symbol,
            price_scale,
            qty_scale,
            tick_size: 1,
            step_size,
        });
    }
    Ok(out)
}

pub(crate) fn parse_futures(json: &str) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let list = v
        .get("data")
        .and_then(|x| x.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data".into()))?;
    let mut out = Vec::with_capacity(list.len());
    for s in list {
        // state 0 = enabled.
        if s.get("state").and_then(|x| x.as_i64()).unwrap_or(-1) != 0 {
            continue;
        }
        let venue = match s.get("symbol").and_then(|x| x.as_str()) {
            Some(s) => s.to_uppercase(),
            None => continue,
        };
        let quote = if venue.ends_with("_USDT") {
            Quote::Usdt
        } else if venue.ends_with("_USDC") {
            Quote::Usdc
        } else {
            continue;
        };
        let canonical = venue.replace('_', "");

        let price_unit = s.get("priceUnit").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let vol_unit = s.get("volUnit").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let contract_size = s.get("contractSize").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let price_unit_str = fmt_decimal(price_unit, "0.01");
        let vol_unit_str = fmt_decimal(vol_unit, "1");
        let contract_size_str = fmt_decimal(contract_size, "1");

        let price_scale = count_decimals_trimmed(&price_unit_str);
        let tick_size = parse_scaled(&price_unit_str, price_scale).unwrap_or(1).max(1);
        let effective_lot = multiply_decimal_strs(&vol_unit_str, &contract_size_str);
        let qty_scale = count_decimals_trimmed(&effective_lot);
        let step_size = parse_scaled(&effective_lot, qty_scale).unwrap_or(1).max(1);
        let (n, d) = decimal_fraction(&contract_size_str);
        set_ct(&canonical, n, d);

        out.push(SymbolSpec {
            exchange: Exchange::MexcF,
            market_type: MarketType::Perp,
            quote,
            symbol: canonical,
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

    const SPOT_FIXTURE: &str = r#"
    {"symbols":[
      {"symbol":"BTCUSDT","status":"1","quoteAsset":"USDT","quotePrecision":2,"baseSizePrecision":"0.000001"},
      {"symbol":"ETHUSDC","status":"1","quoteAsset":"USDC","quotePrecision":2,"baseSizePrecision":"0.0001"},
      {"symbol":"OLDUSDT","status":"3","quoteAsset":"USDT","quotePrecision":2,"baseSizePrecision":"0.01"},
      {"symbol":"BTCBRL","status":"1","quoteAsset":"BRL","quotePrecision":2,"baseSizePrecision":"0.01"}
    ]}"#;

    #[test]
    fn parses_spot() {
        let specs = parse_spot(SPOT_FIXTURE).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::Mexc);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 6);
        assert_eq!(btc.tick_size, 1);
    }

    const FUT_FIXTURE: &str = r#"
    {"data":[
      {"symbol":"BTC_USDT","state":0,"priceUnit":0.1,"volUnit":1,"contractSize":0.0001},
      {"symbol":"ETH_USDC","state":0,"priceUnit":0.01,"volUnit":1,"contractSize":0.01},
      {"symbol":"OFF_USDT","state":1,"priceUnit":0.1,"volUnit":1,"contractSize":1}
    ]}"#;

    #[test]
    fn parses_futures_contract_mult() {
        let specs = parse_futures(FUT_FIXTURE).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::MexcF);
        assert_eq!(btc.price_scale, 1); // priceUnit 0.1
        assert_eq!(btc.tick_size, 1);
        // effective lot = 1 × 0.0001 = 0.0001 → qty_scale 4
        assert_eq!(btc.qty_scale, 4);
        assert_eq!(crate::scale::get_ct("BTCUSDT"), (1, 10000));
    }
}
