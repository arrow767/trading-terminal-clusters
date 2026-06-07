//! REST instruments for KuCoin V1/V2.
//!
//!   - futures: `GET https://api-futures.kucoin.com/api/v1/contracts/active`
//!   - spot:    `GET https://api.kucoin.com/api/v2/symbols`
//!
//! Scale ported from the live engine: price_scale = decimals(tickSize /
//! priceIncrement); futures qty_scale = decimals(lotSize × multiplier);
//! futures register a contract fraction (`multiplier`) into the CT map so the
//! trade parser converts contracts→base for perp. `XBT`→`BTC` canonicalization.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{
    count_decimals_trimmed, decimal_fraction, multiply_decimal_strs, norm_base, parse_scaled,
    set_ct,
};

pub const SPOT_BASE: &str = "https://api.kucoin.com";
pub const FUTURES_BASE: &str = "https://api-futures.kucoin.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KucoinCategory {
    Perp,
    Spot,
}

impl KucoinCategory {
    pub fn is_perp(self) -> bool {
        matches!(self, KucoinCategory::Perp)
    }
}

pub struct KucoinInstrumentsInfo {
    category: KucoinCategory,
    client: reqwest::Client,
}

impl KucoinInstrumentsInfo {
    pub fn new(category: KucoinCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("trading-terminal-clusters/kucoin-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { category, client }
    }

    fn url(&self) -> String {
        match self.category {
            KucoinCategory::Perp => format!("{FUTURES_BASE}/api/v1/contracts/active"),
            KucoinCategory::Spot => format!("{SPOT_BASE}/api/v2/symbols"),
        }
    }
}

#[async_trait]
impl ExchangeInfo for KucoinInstrumentsInfo {
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
        parse_instruments(&body, self.category)
    }
}

#[async_trait]
impl VolumeRanker for KucoinInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        // Ranking unused in prod (top_n=None + rank_by=alphabetical). Return
        // empty so the supervisor's alphabetical fallback applies if ever set.
        Ok(HashMap::new())
    }
}

fn quote_of(s: Option<&str>) -> Option<Quote> {
    match s {
        Some("USDT") => Some(Quote::Usdt),
        Some("USDC") => Some(Quote::Usdc),
        _ => None,
    }
}

/// KuCoin numeric field — JSON number (futures) or decimal string (spot).
fn num_or_str(v: &serde_json::Value, key: &str) -> Option<String> {
    let f = v.get(key)?;
    if let Some(s) = f.as_str() {
        Some(s.to_owned())
    } else if f.is_number() {
        Some(f.to_string())
    } else {
        None
    }
}

pub(crate) fn parse_instruments(json: &str, category: KucoinCategory) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("code").and_then(|x| x.as_str()) != Some("200000") {
        return Err(ExchangeError::Parse(format!(
            "kucoin code != 200000: {}",
            v.get("msg").and_then(|x| x.as_str()).unwrap_or("?")
        )));
    }
    let list = v
        .get("data")
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data".into()))?;

    let is_perp = category.is_perp();
    let mut out = Vec::with_capacity(list.len());
    for s in list {
        let (symbol, quote, price_scale, qty_scale, tick_size, step_size) = if is_perp {
            // Futures contract.
            if matches!(s.get("status").and_then(|x| x.as_str()), Some(st) if st != "Open") {
                continue;
            }
            let quote = match quote_of(s.get("quoteCurrency").and_then(|x| x.as_str())) {
                Some(q) => q,
                None => continue,
            };
            let base = s.get("baseCurrency").and_then(|x| x.as_str()).unwrap_or("");
            if base.is_empty() {
                continue;
            }
            let quote_str = s.get("quoteCurrency").and_then(|x| x.as_str()).unwrap_or("");
            let canonical = format!("{}{}", norm_base(base), quote_str.to_uppercase());

            let tick_str = num_or_str(s, "tickSize").unwrap_or_else(|| "0.1".into());
            let lot_str = num_or_str(s, "lotSize").unwrap_or_else(|| "1".into());
            let mult_str = num_or_str(s, "multiplier").unwrap_or_else(|| "1".into());

            let price_scale = count_decimals_trimmed(&tick_str);
            let tick_size = match parse_scaled(&tick_str, price_scale) {
                Ok(v) if v > 0 => v,
                _ => {
                    tracing::warn!(%canonical, tick = %tick_str, "kucoin skip: bad tickSize");
                    continue;
                }
            };
            // Base-aligned lot = lotSize × multiplier (matches live engine).
            let effective_lot = multiply_decimal_strs(&lot_str, &mult_str);
            let qty_scale = count_decimals_trimmed(&effective_lot);
            let step_size = parse_scaled(&effective_lot, qty_scale).unwrap_or(1).max(1);
            // WS sizes in contracts → base via multiplier fraction (perp only).
            let (n, d) = decimal_fraction(&mult_str);
            set_ct(&canonical, n, d);

            (canonical, quote, price_scale, qty_scale, tick_size, step_size)
        } else {
            // Spot symbol.
            if matches!(s.get("enableTrading").and_then(|x| x.as_bool()), Some(false)) {
                continue;
            }
            let quote = match quote_of(s.get("quoteCurrency").and_then(|x| x.as_str())) {
                Some(q) => q,
                None => continue,
            };
            let sym = match s.get("symbol").and_then(|x| x.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = sym.replace('-', "").to_uppercase();
            let tick_str = num_or_str(s, "priceIncrement").unwrap_or_else(|| "0.0001".into());
            let step_str = num_or_str(s, "baseIncrement").unwrap_or_else(|| "0.0001".into());
            let price_scale = count_decimals_trimmed(&tick_str);
            let qty_scale = count_decimals_trimmed(&step_str);
            let tick_size = parse_scaled(&tick_str, price_scale).unwrap_or(1).max(1);
            let step_size = parse_scaled(&step_str, qty_scale).unwrap_or(1).max(1);
            (canonical, quote, price_scale, qty_scale, tick_size, step_size)
        };

        let (exchange, market_type) = if is_perp {
            (Exchange::KucoinF, MarketType::Perp)
        } else {
            (Exchange::Kucoin, MarketType::Spot)
        };

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
      "code": "200000",
      "data": [
        {"symbol":"XBTUSDTM","baseCurrency":"XBT","quoteCurrency":"USDT","status":"Open",
         "tickSize":0.1,"lotSize":1,"multiplier":0.001},
        {"symbol":"ETHUSDCM","baseCurrency":"ETH","quoteCurrency":"USDC","status":"Open",
         "tickSize":0.01,"lotSize":1,"multiplier":0.01},
        {"symbol":"DEADUSDTM","baseCurrency":"DEAD","quoteCurrency":"USDT","status":"Closed",
         "tickSize":0.1,"lotSize":1,"multiplier":1}
      ]
    }"#;

    #[test]
    fn parses_futures_xbt_alias_and_contract_mult() {
        let specs = parse_instruments(PERP_FIXTURE, KucoinCategory::Perp).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::KucoinF);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.price_scale, 1); // tick 0.1
        assert_eq!(btc.tick_size, 1);
        // effective lot = 1 × 0.001 = 0.001 → qty_scale 3
        assert_eq!(btc.qty_scale, 3);
        assert_eq!(crate::scale::get_ct("BTCUSDT"), (1, 1000));
    }

    const SPOT_FIXTURE: &str = r#"
    {
      "code": "200000",
      "data": [
        {"symbol":"BTC-USDT","baseCurrency":"BTC","quoteCurrency":"USDT","enableTrading":true,
         "priceIncrement":"0.1","baseIncrement":"0.00000001"},
        {"symbol":"ETH-USDC","baseCurrency":"ETH","quoteCurrency":"USDC","enableTrading":true,
         "priceIncrement":"0.01","baseIncrement":"0.0001"},
        {"symbol":"OLD-USDT","baseCurrency":"OLD","quoteCurrency":"USDT","enableTrading":false,
         "priceIncrement":"0.01","baseIncrement":"0.01"}
      ]
    }"#;

    #[test]
    fn parses_spot_filters_disabled() {
        let specs = parse_instruments(SPOT_FIXTURE, KucoinCategory::Spot).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::Kucoin);
        assert_eq!(btc.price_scale, 1);
        assert_eq!(btc.qty_scale, 8);
    }

    #[test]
    fn rejects_error_code() {
        assert!(parse_instruments(r#"{"code":"400000","data":[]}"#, KucoinCategory::Perp).is_err());
    }
}
