//! REST instruments + 24h volume for Bitget V2.
//!
//! Endpoints:
//!   - perp:  `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES`
//!   - spot:  `GET /api/v2/spot/public/symbols`
//!
//! Scale handling mirrors the terminal live engine
//! (`rust-ws-engine/src/bitget/rest.rs`) so server prices/qty match
//! byte-for-byte at the int64 level:
//!   - perp: `pricePlace`/`volumePlace` ARE decimal counts directly →
//!     price_scale/qty_scale; tick = `priceEndStep` (already in scaled units).
//!   - spot: `pricePrecision`/`quantityPrecision` are decimal counts; tick/step
//!     = 1 (finest granularity), same as the live engine.
//!
//! Bitget USDT-FUTURES are LINEAR (qty already in base coin) — no contract
//! multiplier, unlike OKX/KuCoin swaps.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::parse_scaled;

const DEFAULT_BASE_URL: &str = "https://api.bitget.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitgetCategory {
    /// USDT-margined linear perpetual futures (productType=USDT-FUTURES).
    Perp,
    /// Spot pairs (USDT/USDC quote).
    Spot,
}

impl BitgetCategory {
    fn is_perp(self) -> bool {
        matches!(self, BitgetCategory::Perp)
    }
}

pub struct BitgetInstrumentsInfo {
    base_url: String,
    category: BitgetCategory,
    client: reqwest::Client,
}

impl BitgetInstrumentsInfo {
    pub fn new(category: BitgetCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("trading-terminal-clusters/bitget-0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            category,
            client,
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    fn instruments_url(&self) -> String {
        match self.category {
            BitgetCategory::Perp => format!(
                "{}/api/v2/mix/market/contracts?productType=USDT-FUTURES",
                self.base_url
            ),
            BitgetCategory::Spot => format!("{}/api/v2/spot/public/symbols", self.base_url),
        }
    }

    fn tickers_url(&self) -> String {
        match self.category {
            BitgetCategory::Perp => format!(
                "{}/api/v2/mix/market/tickers?productType=USDT-FUTURES",
                self.base_url
            ),
            BitgetCategory::Spot => format!("{}/api/v2/spot/market/tickers", self.base_url),
        }
    }
}

#[async_trait]
impl ExchangeInfo for BitgetInstrumentsInfo {
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
        parse_instruments(&body, self.category)
    }
}

#[async_trait]
impl VolumeRanker for BitgetInstrumentsInfo {
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
        parse_24h_volumes(&body)
    }
}

fn quote_of(s: Option<&str>) -> Option<Quote> {
    match s {
        Some("USDT") => Some(Quote::Usdt),
        Some("USDC") => Some(Quote::Usdc),
        _ => None,
    }
}

/// Bitget returns numeric config fields as JSON strings.
fn str_field<'a>(item: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    item.get(key).and_then(|x| x.as_str())
}

pub(crate) fn parse_instruments(json: &str, category: BitgetCategory) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let code = v.get("code").and_then(|x| x.as_str()).unwrap_or("");
    if code != "00000" {
        return Err(ExchangeError::Parse(format!(
            "bitget code != 00000: {}",
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
        let symbol = match str_field(s, "symbol") {
            Some(s) => s.to_uppercase(),
            None => continue,
        };
        let quote = match quote_of(str_field(s, "quoteCoin")) {
            Some(q) => q,
            None => continue, // skip non-USDT/USDC
        };

        let (price_scale, qty_scale, tick_size, step_size) = if is_perp {
            // Skip explicitly dead contracts.
            if matches!(str_field(s, "symbolStatus"), Some("off") | Some("maintain")) {
                continue;
            }
            let Some(price_place) = str_field(s, "pricePlace").and_then(|x| x.parse::<u8>().ok())
            else {
                tracing::warn!(%symbol, "bitget skip: bad pricePlace");
                continue;
            };
            let Some(volume_place) = str_field(s, "volumePlace").and_then(|x| x.parse::<u8>().ok())
            else {
                tracing::warn!(%symbol, "bitget skip: bad volumePlace");
                continue;
            };
            // priceEndStep is the tick in scaled units (× 10^pricePlace already).
            let tick_size = str_field(s, "priceEndStep")
                .and_then(|x| x.parse::<i64>().ok())
                .unwrap_or(1)
                .max(1);
            // Qty step from minTradeNum at qty_scale; not used for bucketing,
            // default 1.
            let step_size = str_field(s, "minTradeNum")
                .and_then(|x| parse_scaled(x, volume_place).ok())
                .unwrap_or(1)
                .max(1);
            (price_place, volume_place, tick_size, step_size)
        } else {
            // Spot: skip offline pairs.
            if matches!(str_field(s, "status"), Some("offline")) {
                continue;
            }
            let Some(price_prec) =
                str_field(s, "pricePrecision").and_then(|x| x.parse::<u8>().ok())
            else {
                tracing::warn!(%symbol, "bitget skip: bad pricePrecision");
                continue;
            };
            let Some(qty_prec) =
                str_field(s, "quantityPrecision").and_then(|x| x.parse::<u8>().ok())
            else {
                tracing::warn!(%symbol, "bitget skip: bad quantityPrecision");
                continue;
            };
            // Spot tick/step = 1 raw unit (finest), mirrors the live engine.
            (price_prec, qty_prec, 1, 1)
        };

        let (exchange, market_type) = if is_perp {
            (Exchange::BitgetF, MarketType::Perp)
        } else {
            (Exchange::Bitget, MarketType::Spot)
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

pub(crate) fn parse_24h_volumes(json: &str) -> Result<HashMap<String, f64>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("code").and_then(|x| x.as_str()) != Some("00000") {
        return Err(ExchangeError::Parse("bitget tickers code != 00000".into()));
    }
    let list = v
        .get("data")
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data".into()))?;
    let mut out = HashMap::with_capacity(list.len());
    for entry in list {
        let Some(symbol) = str_field(entry, "symbol") else {
            continue;
        };
        // mix tickers → "quoteVolume"; spot tickers → "quoteVol"; fall back to
        // usdt-denominated volume. Used only for ranking; precision irrelevant.
        let vol = ["quoteVolume", "quoteVol", "usdtVolume", "usdtVol"]
            .iter()
            .find_map(|k| str_field(entry, k))
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        out.insert(symbol.to_uppercase(), vol);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const PERP_FIXTURE: &str = r#"
    {
      "code": "00000",
      "msg": "success",
      "data": [
        {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","pricePlace":"1",
         "priceEndStep":"1","volumePlace":"3","minTradeNum":"0.001","symbolStatus":"normal"},
        {"symbol":"ETHUSDC","baseCoin":"ETH","quoteCoin":"USDC","pricePlace":"2",
         "priceEndStep":"1","volumePlace":"2","minTradeNum":"0.01","symbolStatus":"normal"},
        {"symbol":"DEADUSDT","baseCoin":"DEAD","quoteCoin":"USDT","pricePlace":"2",
         "priceEndStep":"1","volumePlace":"1","minTradeNum":"1","symbolStatus":"off"},
        {"symbol":"BTCBRL","baseCoin":"BTC","quoteCoin":"BRL","pricePlace":"1",
         "priceEndStep":"1","volumePlace":"3","minTradeNum":"0.001","symbolStatus":"normal"}
      ]
    }"#;

    #[test]
    fn parses_perp_usdt_usdc_filters_dead_and_other_quotes() {
        let specs = parse_instruments(PERP_FIXTURE, BitgetCategory::Perp).unwrap();
        // BTCUSDT + ETHUSDC. DEADUSDT off, BTCBRL non-USDT/USDC.
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::BitgetF);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.price_scale, 1);
        assert_eq!(btc.qty_scale, 3);
        assert_eq!(btc.tick_size, 1); // priceEndStep
        assert_eq!(btc.step_size, 1); // 0.001 @ scale 3
        let eth = specs.iter().find(|s| s.symbol == "ETHUSDC").unwrap();
        assert_eq!(eth.quote, Quote::Usdc);
    }

    const SPOT_FIXTURE: &str = r#"
    {
      "code": "00000",
      "data": [
        {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT",
         "pricePrecision":"2","quantityPrecision":"6","status":"online"},
        {"symbol":"ETHUSDC","baseCoin":"ETH","quoteCoin":"USDC",
         "pricePrecision":"2","quantityPrecision":"4","status":"online"},
        {"symbol":"OLDUSDT","baseCoin":"OLD","quoteCoin":"USDT",
         "pricePrecision":"2","quantityPrecision":"2","status":"offline"}
      ]
    }"#;

    #[test]
    fn parses_spot_usdt_usdc_filters_offline() {
        let specs = parse_instruments(SPOT_FIXTURE, BitgetCategory::Spot).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::Bitget);
        assert_eq!(btc.market_type, MarketType::Spot);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 6);
        assert_eq!(btc.tick_size, 1);
        assert_eq!(btc.step_size, 1);
    }

    const TICKERS_FIXTURE: &str = r#"
    {
      "code": "00000",
      "data": [
        {"symbol":"BTCUSDT","quoteVolume":"1234567.5"},
        {"symbol":"ETHUSDT","quoteVol":"9000.0"},
        {"symbol":"BAD"}
      ]
    }"#;

    #[test]
    fn parses_24h_volumes() {
        let m = parse_24h_volumes(TICKERS_FIXTURE).unwrap();
        assert!((m["BTCUSDT"] - 1_234_567.5).abs() < 1e-3);
        assert!((m["ETHUSDT"] - 9000.0).abs() < 1e-3);
        assert_eq!(m["BAD"], 0.0);
    }

    #[test]
    fn rejects_error_code() {
        let bad = r#"{"code":"40034","msg":"err","data":[]}"#;
        assert!(parse_instruments(bad, BitgetCategory::Perp).is_err());
    }
}
