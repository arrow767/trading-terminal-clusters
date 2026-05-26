use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::parse_scaled;

const DEFAULT_BASE_URL: &str = "https://api.binance.com";

/// REST client for Binance **spot** `exchangeInfo` (+ 24hr ticker for
/// volume-ranking). Mirrors `BinanceFuturesInfo` in shape but speaks the
/// `/api/v3/...` endpoints — separate host (`api.binance.com` not
/// `fapi.binance.com`), different schema (no `contractType`, simpler
/// quote/base layout).
///
/// Filters: only `TRADING` status, and only `USDT` or `USDC` quote — same
/// scope as the futures adapter. Symbols come back as e.g. "BTCUSDT" and
/// "ETHUSDC" (no separator). Non-target quotes (BUSD, FDUSD, etc.) are
/// silently skipped.
pub struct BinanceSpotInfo {
    base_url: String,
    client: reqwest::Client,
}

impl BinanceSpotInfo {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("trading-terminal-clusters/0.1")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            client,
        }
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        let mut me = Self::new();
        me.base_url = base_url.into();
        me
    }
}

impl Default for BinanceSpotInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExchangeInfo for BinanceSpotInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let url = format!("{}/api/v3/exchangeInfo", self.base_url);
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
        parse_exchange_info(&body)
    }
}

#[async_trait]
impl VolumeRanker for BinanceSpotInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        let url = format!("{}/api/v3/ticker/24hr", self.base_url);
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
        out.insert(symbol.to_string(), qv);
    }
    Ok(out)
}

pub(crate) fn parse_exchange_info(json: &str) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let symbols = v
        .get("symbols")
        .and_then(|s| s.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing 'symbols' array".into()))?;

    let mut out = Vec::with_capacity(symbols.len());
    for s in symbols {
        // На spot нет `contractType`; вместо него фильтруем по status.
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "TRADING" {
            continue;
        }

        let quote = match s.get("quoteAsset").and_then(|v| v.as_str()) {
            Some("USDT") => Quote::Usdt,
            Some("USDC") => Quote::Usdc,
            _ => continue,
        };

        // На spot тоже бывают leveraged-токены (UPUSDT, DOWNUSDT — Binance
        // деприкейтил, но всякое возможно). Фильтруем по `isSpotTradingAllowed`
        // если поле есть; для бар-обычного spot оно true.
        if let Some(v) = s.get("isSpotTradingAllowed").and_then(|v| v.as_bool()) {
            if !v {
                continue;
            }
        }

        let symbol = s
            .get("symbol")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExchangeError::Parse("symbol missing".into()))?
            .to_string();

        // Spot exchangeInfo: `baseAssetPrecision`/`quoteAssetPrecision` —
        // максимальная precision; реальные шаги в PRICE_FILTER/LOT_SIZE.
        let price_scale = s
            .get("quoteAssetPrecision")
            .or_else(|| s.get("quotePrecision"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: quote precision missing")))?
            as u8;
        let qty_scale = s
            .get("baseAssetPrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: baseAssetPrecision missing")))?
            as u8;

        let filters = s
            .get("filters")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: filters missing")))?;
        let mut tick_size: i64 = 0;
        let mut step_size: i64 = 0;
        for f in filters {
            match f.get("filterType").and_then(|v| v.as_str()).unwrap_or("") {
                "PRICE_FILTER" => {
                    if let Some(t) = f.get("tickSize").and_then(|v| v.as_str()) {
                        tick_size = parse_scaled(t, price_scale)?;
                    }
                }
                "LOT_SIZE" => {
                    if let Some(t) = f.get("stepSize").and_then(|v| v.as_str()) {
                        step_size = parse_scaled(t, qty_scale)?;
                    }
                }
                _ => {}
            }
        }
        if tick_size <= 0 || step_size <= 0 {
            tracing::warn!(symbol = %symbol, "spot skip: missing PRICE_FILTER or LOT_SIZE");
            continue;
        }

        out.push(SymbolSpec {
            exchange: Exchange::Binance,
            market_type: MarketType::Spot,
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

    const FIXTURE: &str = r#"
    {
      "timezone": "UTC",
      "symbols": [
        {
          "symbol": "BTCUSDT",
          "status": "TRADING",
          "baseAsset": "BTC",
          "quoteAsset": "USDT",
          "baseAssetPrecision": 8,
          "quoteAssetPrecision": 8,
          "isSpotTradingAllowed": true,
          "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.00001000"}
          ]
        },
        {
          "symbol": "ETHUSDC",
          "status": "TRADING",
          "baseAsset": "ETH",
          "quoteAsset": "USDC",
          "baseAssetPrecision": 8,
          "quoteAssetPrecision": 8,
          "isSpotTradingAllowed": true,
          "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.0001"}
          ]
        },
        {
          "symbol": "BTCBUSD",
          "status": "TRADING",
          "quoteAsset": "BUSD",
          "baseAssetPrecision": 8,
          "quoteAssetPrecision": 8,
          "isSpotTradingAllowed": true,
          "filters": []
        },
        {
          "symbol": "LEVUPUSDT",
          "status": "TRADING",
          "quoteAsset": "USDT",
          "baseAssetPrecision": 8,
          "quoteAssetPrecision": 8,
          "isSpotTradingAllowed": false,
          "filters": []
        },
        {
          "symbol": "BTCEUR",
          "status": "HALT",
          "quoteAsset": "EUR",
          "baseAssetPrecision": 8,
          "quoteAssetPrecision": 8,
          "filters": []
        }
      ]
    }
    "#;

    #[test]
    fn parses_spot_usdt_and_usdc_only() {
        let specs = parse_exchange_info(FIXTURE).unwrap();
        assert_eq!(specs.len(), 2, "expected only USDT + USDC TRADING: {specs:?}");

        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::Binance);
        assert_eq!(btc.market_type, MarketType::Spot);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.tick_size, 1_000_000); // 0.01 × 10^8
        assert_eq!(btc.step_size, 1_000);       // 0.00001 × 10^8

        let eth = specs.iter().find(|s| s.symbol == "ETHUSDC").unwrap();
        assert_eq!(eth.quote, Quote::Usdc);
    }

    #[test]
    fn rejects_unparseable() {
        assert!(parse_exchange_info("not json").is_err());
        assert!(parse_exchange_info("{}").is_err());
    }

    const TICKER_FIXTURE: &str = r#"
    [
      {"symbol":"BTCUSDT","quoteVolume":"1234567.0"},
      {"symbol":"ETHUSDT","quoteVolume":"890123.5"},
      {"symbol":"BADENTRY"}
    ]
    "#;

    #[test]
    fn parses_ticker_volumes() {
        let m = parse_24h_quote_volumes(TICKER_FIXTURE).unwrap();
        assert_eq!(m.len(), 2);
        assert!((m["BTCUSDT"] - 1_234_567.0).abs() < 1e-3);
    }
}
