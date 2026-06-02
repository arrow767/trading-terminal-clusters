//! REST: `GET /api/v5/public/instruments?instType={SPOT|SWAP}`.
//!
//! OKX V5. Поддерживаем USDT/USDC linear:
//!   - SPOT: instId `BTC-USDT` / `ETH-USDC`
//!   - SWAP: instId `BTC-USDT-SWAP` (linear, settle=USDT). Инверсные
//!     `BTC-USD-SWAP` отсекаются фильтром quote∈{USDT,USDC}.
//!
//! Из ответа берём:
//!   - `instId` → canonical `BTCUSDT` (`normalize_inst_id`)
//!   - quote из второго сегмента instId → Quote::Usdt/Usdc (иначе skip)
//!   - `tickSz` → price_scale + tick
//!   - `lotSz` (+ `ctVal` для свопа) → effective lot → qty_scale + step
//!   - `state == "live"` (иначе skip)
//!
//! Для свопа дополнительно регистрируем contract-фракцию (`ctVal`) в
//! `scale::set_ct`, чтобы парсер трейдов перевёл контракты в базовый актив.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use exchange_core::{
    Exchange, ExchangeError, ExchangeInfo, MarketType, Quote, Result, SymbolSpec, VolumeRanker,
};

use crate::scale::{
    count_decimals_trimmed, ct_val_fraction, multiply_decimal_strs, normalize_inst_id,
    parse_scaled, set_ct,
};

const DEFAULT_BASE_URL: &str = "https://www.okx.com";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OkxCategory {
    /// USDT/USDC linear perpetual swaps.
    Swap,
    /// Spot pairs (USDT/USDC quote).
    Spot,
}

impl OkxCategory {
    fn inst_type(self) -> &'static str {
        match self {
            OkxCategory::Swap => "SWAP",
            OkxCategory::Spot => "SPOT",
        }
    }
    fn is_swap(self) -> bool {
        matches!(self, OkxCategory::Swap)
    }
}

pub struct OkxInstrumentsInfo {
    base_url: String,
    category: OkxCategory,
    client: reqwest::Client,
}

impl OkxInstrumentsInfo {
    pub fn new(category: OkxCategory) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("trading-terminal-clusters/okx-0.1")
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
impl ExchangeInfo for OkxInstrumentsInfo {
    async fn fetch_symbols(&self) -> Result<Vec<SymbolSpec>> {
        let url = format!(
            "{}/api/v5/public/instruments?instType={}",
            self.base_url,
            self.category.inst_type()
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
        parse_instruments(&body, self.category)
    }
}

#[async_trait]
impl VolumeRanker for OkxInstrumentsInfo {
    async fn fetch_24h_quote_volumes(&self) -> Result<HashMap<String, f64>> {
        let url = format!(
            "{}/api/v5/market/tickers?instType={}",
            self.base_url,
            self.category.inst_type()
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
        parse_24h_volumes(&body, self.category)
    }
}

/// Second segment of an OKX instId is the quote ccy: `BTC-USDT[-SWAP]` → USDT.
fn quote_of(inst_id: &str) -> Option<Quote> {
    let mut it = inst_id.split('-');
    let _base = it.next()?;
    match it.next()? {
        "USDT" => Some(Quote::Usdt),
        "USDC" => Some(Quote::Usdc),
        _ => None,
    }
}

pub(crate) fn parse_instruments(json: &str, category: OkxCategory) -> Result<Vec<SymbolSpec>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("code").and_then(|x| x.as_str()) != Some("0") {
        return Err(ExchangeError::Parse(format!(
            "okx code != 0: {}",
            v.get("msg").and_then(|x| x.as_str()).unwrap_or("?")
        )));
    }
    let list = v
        .get("data")
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data".into()))?;

    let is_swap = category.is_swap();
    let mut out = Vec::with_capacity(list.len());
    for s in list {
        if s.get("state").and_then(|x| x.as_str()) != Some("live") {
            continue;
        }
        let inst_id = match s.get("instId").and_then(|x| x.as_str()) {
            Some(s) => s,
            None => continue,
        };
        // Linear-only for swap: inverse (`*-USD-SWAP`) and non-USDT/USDC are
        // dropped here by the quote filter. ctType guards future weirdness.
        if is_swap && s.get("ctType").and_then(|x| x.as_str()) == Some("inverse") {
            continue;
        }
        let quote = match quote_of(inst_id) {
            Some(q) => q,
            None => continue, // skip non-USDT/USDC (incl. inverse `-USD-`)
        };

        let symbol = normalize_inst_id(inst_id);

        let tick_str = s.get("tickSz").and_then(|x| x.as_str()).unwrap_or("");
        let lot_str = s.get("lotSz").and_then(|x| x.as_str()).unwrap_or("");
        if tick_str.is_empty() || lot_str.is_empty() {
            tracing::warn!(%inst_id, "okx skip: missing tickSz/lotSz");
            continue;
        }

        // Effective lot = lotSz * ctVal for swap (base asset per min order),
        // == lotSz for spot. qty_scale derives from it — exactly like the live
        // engine, so server qty matches terminal-local at the int64 level.
        let ct_val_str = s.get("ctVal").and_then(|x| x.as_str()).unwrap_or("1");
        let effective_lot = if is_swap {
            multiply_decimal_strs(lot_str, ct_val_str)
        } else {
            lot_str.to_owned()
        };

        let price_scale = count_decimals_trimmed(tick_str);
        let qty_scale = count_decimals_trimmed(&effective_lot);
        let tick_size = match parse_scaled(tick_str, price_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%inst_id, tick = tick_str, error = %e, "okx skip: bad tickSz");
                continue;
            }
        };
        let step_size = match parse_scaled(&effective_lot, qty_scale) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(%inst_id, lot = %effective_lot, error = %e, "okx skip: bad lot");
                continue;
            }
        };
        if tick_size <= 0 || step_size <= 0 {
            tracing::warn!(%inst_id, "okx skip: zero tick/lot");
            continue;
        }

        // Register contract multiplier for swap so the trade parser converts
        // contracts→base. Spot stays 1/1 (never registered).
        if is_swap {
            let (n, d) = ct_val_fraction(ct_val_str);
            set_ct(&symbol, n, d);
        }

        let (exchange, market_type) = if is_swap {
            (Exchange::OkxF, MarketType::Perp)
        } else {
            (Exchange::Okx, MarketType::Spot)
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

pub(crate) fn parse_24h_volumes(json: &str, category: OkxCategory) -> Result<HashMap<String, f64>> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ExchangeError::Parse(e.to_string()))?;
    if v.get("code").and_then(|x| x.as_str()) != Some("0") {
        return Err(ExchangeError::Parse("okx tickers code != 0".into()));
    }
    let list = v
        .get("data")
        .and_then(|l| l.as_array())
        .ok_or_else(|| ExchangeError::Parse("missing data".into()))?;
    let is_swap = category.is_swap();
    let mut out = HashMap::with_capacity(list.len());
    for entry in list {
        let Some(inst_id) = entry.get("instId").and_then(|x| x.as_str()) else {
            continue;
        };
        if quote_of(inst_id).is_none() {
            continue;
        }
        // OKX `volCcy24h`: SPOT → quote-ccy notional directly; SWAP → base-ccy
        // amount (contracts·ctVal), so multiply by `last` to approximate quote
        // notional for ranking. Ordering only — no settlement math.
        let vol_ccy = entry
            .get("volCcy24h")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let notional = if is_swap {
            let last = entry
                .get("last")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            vol_ccy * last
        } else {
            vol_ccy
        };
        out.insert(normalize_inst_id(inst_id), notional);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const SWAP_FIXTURE: &str = r#"
    {
      "code":"0","msg":"",
      "data":[
        {"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"live","ctType":"linear",
         "tickSz":"0.1","lotSz":"1","ctVal":"0.001","settleCcy":"USDT"},
        {"instId":"BTC-USD-SWAP","instType":"SWAP","state":"live","ctType":"inverse",
         "tickSz":"0.1","lotSz":"1","ctVal":"100","settleCcy":"BTC"},
        {"instId":"ETH-USDC-SWAP","instType":"SWAP","state":"live","ctType":"linear",
         "tickSz":"0.01","lotSz":"1","ctVal":"0.01","settleCcy":"USDC"},
        {"instId":"DOGE-USDT-SWAP","instType":"SWAP","state":"preopen","ctType":"linear",
         "tickSz":"0.00001","lotSz":"1","ctVal":"1000","settleCcy":"USDT"}
      ]
    }"#;

    #[test]
    fn parses_linear_swaps_filters_inverse_and_nonlive() {
        let specs = parse_instruments(SWAP_FIXTURE, OkxCategory::Swap).unwrap();
        // BTC-USDT-SWAP + ETH-USDC-SWAP. inverse BTC-USD skip, DOGE preopen skip.
        assert_eq!(specs.len(), 2);
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.exchange, Exchange::OkxF);
        assert_eq!(btc.market_type, MarketType::Perp);
        assert_eq!(btc.quote, Quote::Usdt);
        assert_eq!(btc.price_scale, 1); // "0.1"
        assert_eq!(btc.tick_size, 1);
        // effective lot = 1*0.001 = "0.001" → qty_scale 3, step 1
        assert_eq!(btc.qty_scale, 3);
        assert_eq!(btc.step_size, 1);
        // contract fraction registered
        assert_eq!(crate::scale::get_ct("BTCUSDT"), (1, 1000));
    }

    const SPOT_FIXTURE: &str = r#"
    {
      "code":"0","msg":"",
      "data":[
        {"instId":"BTC-USDT","instType":"SPOT","state":"live",
         "baseCcy":"BTC","quoteCcy":"USDT","tickSz":"0.01","lotSz":"0.00001"},
        {"instId":"BTC-EUR","instType":"SPOT","state":"live",
         "baseCcy":"BTC","quoteCcy":"EUR","tickSz":"0.01","lotSz":"0.00001"}
      ]
    }"#;

    #[test]
    fn parses_spot_filters_non_usdtc() {
        let specs = parse_instruments(SPOT_FIXTURE, OkxCategory::Spot).unwrap();
        assert_eq!(specs.len(), 1);
        let btc = &specs[0];
        assert_eq!(btc.exchange, Exchange::Okx);
        assert_eq!(btc.market_type, MarketType::Spot);
        assert_eq!(btc.price_scale, 2);
        assert_eq!(btc.qty_scale, 5); // "0.00001" (spot lot, no ctVal)
    }

    #[test]
    fn rejects_non_zero_code() {
        let bad = r#"{"code":"50001","msg":"err","data":[]}"#;
        assert!(parse_instruments(bad, OkxCategory::Swap).is_err());
    }
}
