//! REST: `GET /v5/market/instruments-info?category={linear|spot}`.
//!
//! Bybit V5 unified API. Categories мы поддерживаем:
//!   - `linear` → USDT/USDC perpetual (BTCUSDT, BTCPERP, ETHUSDT, …)
//!     - BTCUSDT — USDT-linear ("settleCoin":"USDT")
//!     - BTCPERP — USDC-linear ("settleCoin":"USDC", "symbol":"BTCPERP")
//!   - `spot` → BTCUSDT, ETHUSDT, … (всегда settleCoin = quoteCoin)
//!
//! Из ответа берём:
//!   - `symbol` (UPPER, без разделителя)
//!   - `quoteCoin` → maps to Quote::Usdt/Usdc
//!   - `priceFilter.tickSize` → tick + scale (count_decimals_trimmed)
//!   - `lotSizeFilter.basePrecision` (spot) ИЛИ `lotSizeFilter.qtyStep` (linear)
//!   - `status == "Trading"` (skip preview / delivering / suspended)
//!
//! Скипы пер-символу (graceful, как в exchange-binance):
//! - неподходящая quote → skip без warn (фильтр universe);
//! - parse error tick/step → warn + skip;
//! - status != Trading → skip без warn.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{count_decimals_trimmed, parse_scaled};

const DEFAULT_BASE_URL: &str = "https://api.bybit.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitCategory {
    /// Linear USDT/USDC perpetuals.
    Linear,
    /// Spot pairs (USDT/USDC quote).
    Spot,
}

impl BybitCategory {
    fn as_str(self) -> &'static str {
        match self {
            BybitCategory::Linear => "linear",
            BybitCategory::Spot => "spot",
        }
    }
}

pub struct BybitInstrumentsInfo {
    base_url: String,
    category: BybitCategory,
    client: reqwest::Client,
}

impl BybitInstrumentsInfo {
    pub fn new(category: BybitCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("trading-terminal-clusters/bybit-0.1")
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
}

#[async_trait]
impl ExchangeInfo for BybitInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        // Bybit пагинирует через `cursor` если limit>1000 не задан. На spot
        // ~750 символов, на linear ~500 — limit=1000 берёт всё за раз без
        // пагинации. Если когда-нибудь упрёмся в потолок — добавим loop по
        // `result.nextPageCursor`.
        let url = format!(
            "{}/v5/market/instruments-info?category={}&limit=1000",
            self.base_url,
            self.category.as_str()
        );
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
        parse_instruments_info(&body, self.category)
    }
}

/// Bybit /v5/market/tickers — ranking by `volume24h` (base coin) или `turnover24h`
/// (quote). Мы берём `turnover24h` как notional-volume → сравним с Binance
/// quoteVolume по интенсивности торгов. Базовая монета не годится — BTC
/// и DOGE по объёму в "штуках" одного типа нельзя сравнивать.
#[async_trait]
impl VolumeRanker for BybitInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        let url = format!(
            "{}/v5/market/tickers?category={}",
            self.base_url,
            self.category.as_str()
        );
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
        parse_24h_turnover(&body)
    }
}

pub(crate) fn parse_instruments_info(json: &str, category: BybitCategory) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("retCode").and_then(|x| x.as_i64()) != Some(0) {
        return Err(ExchangeError::Parse(format!(
            "bybit retCode != 0: {}",
            v.get("retMsg").and_then(|x| x.as_str()).unwrap_or("?")
        )));
    }
    let list = v
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing result.list".into()))?;

    let mut out = Vec::with_capacity(list.len());
    for s in list {
        let status = s.get("status").and_then(|x| x.as_str()).unwrap_or("");
        if status != "Trading" {
            continue;
        }
        let symbol = match s.get("symbol").and_then(|x| x.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        // Quote: для linear надо смотреть `quoteCoin` (USDT или USDC).
        // Для USDC-linear символ выглядит как "BTCPERP" — `quoteCoin` всё
        // равно "USDC", это и важно. Для spot — `quoteCoin` стандарт.
        let quote = match s.get("quoteCoin").and_then(|x| x.as_str()) {
            Some("USDT") => Quote::Usdt,
            Some("USDC") => Quote::Usdc,
            _ => continue, // skip остальные пары (BTC, ETH, EUR, ...)
        };

        let price_filter = s.get("priceFilter");
        let tick_str = price_filter
            .and_then(|p| p.get("tickSize"))
            .and_then(|x| x.as_str())
            .unwrap_or("");
        // Spot и linear имеют РАЗНЫЕ поля для qty step:
        //   spot:   lotSizeFilter.basePrecision (string)
        //   linear: lotSizeFilter.qtyStep      (string)
        let lot = s.get("lotSizeFilter");
        let step_str = match category {
            BybitCategory::Spot => lot
                .and_then(|l| l.get("basePrecision"))
                .and_then(|x| x.as_str())
                .unwrap_or(""),
            BybitCategory::Linear => lot
                .and_then(|l| l.get("qtyStep"))
                .and_then(|x| x.as_str())
                .unwrap_or(""),
        };
        if tick_str.is_empty() || step_str.is_empty() {
            tracing::warn!(symbol = %symbol, ?category, "bybit skip: missing tick/step");
            continue;
        }

        let price_scale = count_decimals_trimmed(tick_str);
        let qty_scale = count_decimals_trimmed(step_str);
        let tick_size = match parse_scaled(tick_str, price_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(symbol = %symbol, tick = tick_str, error = %e, "bybit skip: bad tickSize");
                continue;
            }
        };
        let step_size = match parse_scaled(step_str, qty_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(symbol = %symbol, step = step_str, error = %e, "bybit skip: bad stepSize");
                continue;
            }
        };
        if tick_size <= 0 || step_size <= 0 {
            tracing::warn!(symbol = %symbol, "bybit skip: zero tick/step");
            continue;
        }

        let (exchange, market_type) = match category {
            BybitCategory::Spot => (Exchange::Bybit, MarketType::Spot),
            BybitCategory::Linear => (Exchange::BybitF, MarketType::Perp),
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

pub(crate) fn parse_24h_turnover(json: &str) -> Result<HashMap<String, f64>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("retCode").and_then(|x| x.as_i64()) != Some(0) {
        return Err(ExchangeError::Parse("bybit tickers retCode != 0".into()));
    }
    let list = v
        .get("result")
        .and_then(|r| r.get("list"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing result.list".into()))?;
    let mut out = HashMap::with_capacity(list.len());
    for entry in list {
        let Some(symbol) = entry.get("symbol").and_then(|x| x.as_str()) else {
            continue;
        };
        let Some(turnover_str) = entry.get("turnover24h").and_then(|x| x.as_str()) else {
            continue;
        };
        let Ok(turnover) = turnover_str.parse::<f64>() else {
            continue;
        };
        out.insert(symbol.to_string(), turnover);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const LINEAR_FIXTURE: &str = r#"
    {
      "retCode": 0,
      "retMsg": "OK",
      "result": {
        "category": "linear",
        "list": [
          {
            "symbol": "BTCUSDT",
            "status": "Trading",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "priceFilter": {"minPrice": "0.10", "maxPrice": "999999", "tickSize": "0.10"},
            "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"}
          },
          {
            "symbol": "BTCPERP",
            "status": "Trading",
            "baseCoin": "BTC",
            "quoteCoin": "USDC",
            "settleCoin": "USDC",
            "priceFilter": {"tickSize": "0.10"},
            "lotSizeFilter": {"qtyStep": "0.001"}
          },
          {
            "symbol": "DELAYED",
            "status": "PreLaunch",
            "quoteCoin": "USDT",
            "priceFilter": {"tickSize": "0.01"},
            "lotSizeFilter": {"qtyStep": "1"}
          },
          {
            "symbol": "BTCBUSD",
            "status": "Trading",
            "quoteCoin": "BUSD",
            "priceFilter": {"tickSize": "0.1"},
            "lotSizeFilter": {"qtyStep": "0.001"}
          }
        ]
      }
    }"#;

    #[test]
    fn parses_linear_usdt_and_usdc_perp_filters_others() {
        let specs = parse_instruments_info(LINEAR_FIXTURE, BybitCategory::Linear).unwrap();
        // BTCUSDT(USDT) + BTCPERP(USDC). DELAYED skip (status), BTCBUSD skip (quote).
        assert_eq!(specs.len(), 2);
        let usdt = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(usdt.exchange, Exchange::BybitF);
        assert_eq!(usdt.market_type, MarketType::Perp);
        assert_eq!(usdt.quote, Quote::Usdt);
        assert_eq!(usdt.price_scale, 1); // "0.10" → 1 dec
        assert_eq!(usdt.tick_size, 1); // 0.10 × 10 = 1
        let usdc = specs.iter().find(|s| s.symbol == "BTCPERP").unwrap();
        assert_eq!(usdc.quote, Quote::Usdc);
        assert_eq!(usdc.market_type, MarketType::Perp);
    }

    const SPOT_FIXTURE: &str = r#"
    {
      "retCode": 0,
      "result": {
        "category": "spot",
        "list": [
          {
            "symbol": "BTCUSDT",
            "status": "Trading",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "priceFilter": {"tickSize": "0.01"},
            "lotSizeFilter": {"basePrecision": "0.000001"}
          },
          {
            "symbol": "ETHUSDC",
            "status": "Trading",
            "baseCoin": "ETH",
            "quoteCoin": "USDC",
            "priceFilter": {"tickSize": "0.01"},
            "lotSizeFilter": {"basePrecision": "0.0001"}
          }
        ]
      }
    }"#;

    #[test]
    fn parses_spot_usdt_usdc() {
        let specs = parse_instruments_info(SPOT_FIXTURE, BybitCategory::Spot).unwrap();
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::Bybit);
        assert_eq!(btc.market_type, MarketType::Spot);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 6); // "0.000001" → 6 dec
    }

    const TURNOVER_FIXTURE: &str = r#"
    {
      "retCode": 0,
      "result": {
        "category": "linear",
        "list": [
          {"symbol": "BTCUSDT", "turnover24h": "1234567890.5"},
          {"symbol": "ETHUSDT", "turnover24h": "45000000.0"},
          {"symbol": "BAD"}
        ]
      }
    }"#;

    #[test]
    fn parses_24h_turnover_skips_malformed() {
        let m = parse_24h_turnover(TURNOVER_FIXTURE).unwrap();
        assert_eq!(m.len(), 2);
        assert!((m["BTCUSDT"] - 1_234_567_890.5).abs() < 1e-3);
    }

    #[test]
    fn rejects_non_zero_retcode() {
        let bad = r#"{"retCode": 10001, "retMsg": "err", "result": {"list": []}}"#;
        assert!(parse_instruments_info(bad, BybitCategory::Linear).is_err());
    }
}
