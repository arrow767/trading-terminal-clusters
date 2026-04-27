use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec};

use crate::scale::parse_scaled;

const DEFAULT_BASE_URL: &str = "https://fapi.binance.com";

/// REST client for Binance USD-M futures `exchangeInfo`.
///
/// Filters the response to PERPETUAL contracts in TRADING status with a
/// USDT or USDC quote — those are the instruments we want to ingest.
/// Non-perpetual (delivery) contracts and any non-trading status are
/// silently skipped.
pub struct BinanceFuturesInfo {
    base_url: String,
    client: reqwest::Client,
}

impl BinanceFuturesInfo {
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

impl Default for BinanceFuturesInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExchangeInfo for BinanceFuturesInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.base_url);
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

pub(crate) fn parse_exchange_info(json: &str) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    let symbols = v
        .get("symbols")
        .and_then(|s| s.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing 'symbols' array".into()))?;

    let mut out = Vec::with_capacity(symbols.len());
    for s in symbols {
        let contract_type = s.get("contractType").and_then(|v| v.as_str()).unwrap_or("");
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if contract_type != "PERPETUAL" || status != "TRADING" {
            continue;
        }

        let quote = match s.get("quoteAsset").and_then(|v| v.as_str()) {
            Some("USDT") => Quote::Usdt,
            Some("USDC") => Quote::Usdc,
            _ => continue,
        };

        let symbol = s
            .get("symbol")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExchangeError::Parse("symbol missing".into()))?
            .to_string();

        let price_scale = s
            .get("pricePrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: pricePrecision missing")))?
            as u8;
        let qty_scale = s
            .get("quantityPrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: quantityPrecision missing")))?
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
            tracing::warn!(symbol = %symbol, "skipping: missing PRICE_FILTER or LOT_SIZE");
            continue;
        }

        out.push(SymbolSpec {
            exchange: Exchange::BinanceF,
            market_type: MarketType::Perp,
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
          "contractType": "PERPETUAL",
          "status": "TRADING",
          "quoteAsset": "USDT",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"}
          ]
        },
        {
          "symbol": "ETHUSDC",
          "contractType": "PERPETUAL",
          "status": "TRADING",
          "quoteAsset": "USDC",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"}
          ]
        },
        {
          "symbol": "BTCUSDT_240927",
          "contractType": "CURRENT_QUARTER",
          "status": "TRADING",
          "quoteAsset": "USDT",
          "pricePrecision": 1,
          "quantityPrecision": 3,
          "filters": []
        },
        {
          "symbol": "DOGEBUSD",
          "contractType": "PERPETUAL",
          "status": "TRADING",
          "quoteAsset": "BUSD",
          "pricePrecision": 5,
          "quantityPrecision": 0,
          "filters": []
        },
        {
          "symbol": "DELISTED",
          "contractType": "PERPETUAL",
          "status": "PENDING_TRADING",
          "quoteAsset": "USDT",
          "pricePrecision": 2,
          "quantityPrecision": 3,
          "filters": []
        }
      ]
    }
    "#;

    #[test]
    fn parses_perp_usdt_and_usdc_filters_others() {
        let specs = parse_exchange_info(FIXTURE).unwrap();
        assert_eq!(specs.len(), 2);

        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::BinanceF);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 3);
        assert_eq!(btc.tick_size, 10); // 0.10 * 100
        assert_eq!(btc.step_size, 1); // 0.001 * 1000

        let eth = specs.iter().find(|s| s.symbol == "ETHUSDC").unwrap();
        assert_eq!(eth.quote, Quote::Usdc);
        assert_eq!(eth.tick_size, 1); // 0.01 * 100
    }

    #[test]
    fn rejects_unparseable_json() {
        assert!(parse_exchange_info("not json").is_err());
        assert!(parse_exchange_info("{}").is_err());
    }
}
