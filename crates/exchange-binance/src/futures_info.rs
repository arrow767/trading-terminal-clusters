use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{count_decimals_trimmed, parse_scaled};

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

#[async_trait]
impl VolumeRanker for BinanceFuturesInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        let url = format!("{}/fapi/v1/ticker/24hr", self.base_url);
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
            // Tolerate malformed individual entries (Binance has emitted
            // partial objects in past incidents) — skip rather than fail
            // the whole rank fetch over one bad row.
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
        let contract_type = s.get("contractType").and_then(|v| v.as_str()).unwrap_or("");
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("");
        // Accept crypto perps AND Binance "TradFi" perps (TRADIFI_PERPETUAL —
        // tokenized stocks/ETFs/indices: AAPL/AMD/SNDK/EWY/COIN/GOOGL/…). Both are
        // linear USDT/USDC perps on the same fapi WS, so they ingest identically;
        // only dated delivery (CURRENT_QUARTER/NEXT_QUARTER) stays excluded.
        let is_perp = matches!(contract_type, "PERPETUAL" | "TRADIFI_PERPETUAL");
        if !is_perp || status != "TRADING" {
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

        // Scale = decimals в tickSize/stepSize (НЕ pricePrecision/
        // quantityPrecision из exchangeInfo). Терминальный код
        // (EngineServer.Specs.cs:CountDecimalsTrimmed) использует ровно
        // эту формулу. Для BTCUSDT futures: pricePrecision = 2, tickSize =
        // "0.10" → decimals = 1. Раньше сервер использовал pricePrecision
        // → 10× mismatch на price y-axis.
        let filters = s
            .get("filters")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExchangeError::Parse(format!("{symbol}: filters missing")))?;
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
            tracing::warn!(symbol = %symbol, "futures skip: missing PRICE_FILTER or LOT_SIZE");
            continue;
        }
        let price_scale = count_decimals_trimmed(tick_str);
        let qty_scale = count_decimals_trimmed(step_str);
        // Graceful skip per symbol на ошибке (см. spot_info.rs — та же
        // защита от падения всего discovery cycle из-за одного экзотика).
        let tick_size = match parse_scaled(tick_str, price_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(symbol = %symbol, tick = tick_str, error = %e, "futures skip: bad tickSize");
                continue;
            }
        };
        let step_size = match parse_scaled(step_str, qty_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(symbol = %symbol, step = step_str, error = %e, "futures skip: bad stepSize");
                continue;
            }
        };
        if tick_size <= 0 || step_size <= 0 {
            tracing::warn!(symbol = %symbol, "futures skip: zero tick/step");
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
          "symbol": "AAPLUSDT",
          "contractType": "TRADIFI_PERPETUAL",
          "status": "TRADING",
          "quoteAsset": "USDT",
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
        assert_eq!(specs.len(), 3); // BTCUSDT + ETHUSDC + AAPLUSDT; BUSD/quarter/non-trading excluded

        // TradFi perp (tokenized stock) ingests like any linear USDT perp.
        let aapl = specs
            .iter()
            .find(|s| s.symbol == "AAPLUSDT")
            .expect("TRADIFI_PERPETUAL must be included");
        assert_eq!(aapl.market_type, MarketType::Perp);
        assert_eq!(aapl.quote, Quote::Usdt);

        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::BinanceF);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdt);
        // Scale = decimals_trimmed(tickSize/stepSize). Для BTCUSDT
        // futures tickSize="0.10" → 1 decimal → scale=10; step "0.001" → 3.
        assert_eq!(btc.price_scale, 1);
        assert_eq!(btc.qty_scale, 3);
        assert_eq!(btc.tick_size, 1);  // 0.10 × 10 = 1
        assert_eq!(btc.step_size, 1);  // 0.001 × 1000 = 1

        let eth = specs.iter().find(|s| s.symbol == "ETHUSDC").unwrap();
        assert_eq!(eth.quote, Quote::Usdc);
        assert_eq!(eth.price_scale, 2); // tick="0.01" → 2 decimals
        assert_eq!(eth.tick_size, 1);   // 0.01 × 100 = 1
    }

    #[test]
    fn rejects_unparseable_json() {
        assert!(parse_exchange_info("not json").is_err());
        assert!(parse_exchange_info("{}").is_err());
    }

    const TICKER_FIXTURE: &str = r#"
    [
      {"symbol":"BTCUSDT","quoteVolume":"123456789.0","priceChange":"0"},
      {"symbol":"ETHUSDT","quoteVolume":"45000000.5","priceChange":"0"},
      {"symbol":"DOGEUSDT","quoteVolume":"100.0","priceChange":"0"},
      {"symbol":"BADENTRY"},
      {"quoteVolume":"99.0"}
    ]
    "#;

    #[test]
    fn parses_ticker_volumes_and_skips_malformed() {
        let m = parse_24h_quote_volumes(TICKER_FIXTURE).unwrap();
        assert_eq!(m.len(), 3);
        assert!((m["BTCUSDT"] - 123_456_789.0).abs() < 1e-3);
        assert!((m["ETHUSDT"] - 45_000_000.5).abs() < 1e-3);
        assert!(!m.contains_key("BADENTRY"));
    }

    #[test]
    fn ticker_rejects_non_array() {
        assert!(parse_24h_quote_volumes("{}").is_err());
        assert!(parse_24h_quote_volumes("not json").is_err());
    }
}
