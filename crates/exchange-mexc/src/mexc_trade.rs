//! MEXC trade parsers: spot (protobuf binary) + futures (JSON). Both expose
//! peek_symbol + parse_value so the session can route + scale uniformly.
//!
//! Aggressor: tradeType/`T` 1 = taker buy (Bid), 2 = taker sell (Ask).
//! Futures qty is in CONTRACTS → base via the per-symbol multiplier
//! (`scale::get_ct`, perp); spot qty is already in base.

use exchange_core::{AggressorSide, ExchangeError, Result, SymbolSpec, TradePrint};

use crate::pb;
use crate::scale::{get_ct, parse_scaled};

fn aggressor(trade_type: i32) -> Option<AggressorSide> {
    match trade_type {
        1 => Some(AggressorSide::Bid),
        2 => Some(AggressorSide::Ask),
        _ => None,
    }
}

// ─── Spot (protobuf) ──────────────────────────────────────────────────────────

pub struct MexcSpotTradeParser;

impl MexcSpotTradeParser {
    /// Canonical symbol (BTCUSDT) from the wrapper channel; None if not a
    /// deals frame.
    pub fn peek_symbol(&self, raw: &[u8]) -> Option<String> {
        let w = pb::decode_wrapper(raw)?;
        if !w.channel.contains("deals") {
            return None;
        }
        w.channel.rsplit('@').next().map(|s| s.to_uppercase())
    }

    pub fn parse_value(&self, raw: &[u8], spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let w = match pb::decode_wrapper(raw) {
            Some(w) if w.channel.contains("deals") => w,
            _ => return Ok(Vec::new()),
        };
        let deals = pb::decode_deals(w.body)
            .ok_or_else(|| ExchangeError::Parse("mexc spot: bad deals body".into()))?;
        let mut out = Vec::with_capacity(deals.len());
        for d in &deals {
            let Some(agg) = aggressor(d.trade_type) else {
                continue;
            };
            let price = parse_scaled(d.price, spec.price_scale)?;
            let qty = parse_scaled(d.quantity, spec.qty_scale)?;
            out.push(TradePrint {
                exchange_ts_ns: d.time.saturating_mul(1_000_000),
                aggressor: agg,
                price,
                qty,
                trade_id: 0,
            });
        }
        Ok(out)
    }
}

// ─── Futures (JSON) ───────────────────────────────────────────────────────────

pub struct MexcFuturesTradeParser;

impl MexcFuturesTradeParser {
    /// Venue symbol (BTC_USDT) from a `push.deal` frame; None otherwise.
    pub fn peek_symbol<'a>(&self, v: &'a serde_json::Value) -> Option<&'a str> {
        if v.get("channel").and_then(|x| x.as_str()) != Some("push.deal") {
            return None;
        }
        v.get("symbol").and_then(|x| x.as_str())
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };
        // contracts → base (perp).
        let (ct_n, ct_d) = get_ct(&spec.symbol);
        let t_dir = data.get("T").and_then(|x| x.as_i64()).unwrap_or(0) as i32;
        let Some(agg) = aggressor(t_dir) else {
            return Ok(Vec::new());
        };
        let px = num_str(data.get("p"));
        let vol = num_str(data.get("v"));
        if px.is_empty() || vol.is_empty() {
            return Err(ExchangeError::Parse("mexc fut: missing p/v".into()));
        }
        let price = parse_scaled(&px, spec.price_scale)?;
        let qty = parse_scaled(&vol, spec.qty_scale)? * ct_n / ct_d;
        let ts_ms = data.get("t").and_then(|x| x.as_i64()).unwrap_or(0);
        Ok(vec![TradePrint {
            exchange_ts_ns: ts_ms.saturating_mul(1_000_000),
            aggressor: agg,
            price,
            qty,
            trade_id: 0,
        }])
    }
}

fn num_str(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

    use super::*;

    fn fut_spec() -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::MexcF,
            market_type: MarketType::Perp,
            quote: Quote::Usdt,
            symbol: "BTCUSDT".into(),
            price_scale: 1,
            qty_scale: 4,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn futures_push_deal_contracts_to_base() {
        crate::scale::set_ct("BTCUSDT", 1, 10000);
        let v: serde_json::Value = serde_json::from_str(
            r#"{"channel":"push.deal","symbol":"BTC_USDT","data":{"p":67234.5,"v":100,"T":1,"t":1700000000123}}"#,
        )
        .unwrap();
        let p = MexcFuturesTradeParser;
        assert_eq!(p.peek_symbol(&v), Some("BTC_USDT"));
        let trades = p.parse_value(&v, &fut_spec()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        assert_eq!(trades[0].price, 672345);
        // 100 contracts × (1/10000) at qty_scale 4: parse_scaled("100",4)=1_000_000 ×1/10000 = 100
        assert_eq!(trades[0].qty, 100);
        assert_eq!(trades[0].exchange_ts_ns, 1700000000123 * 1_000_000);
    }

    #[test]
    fn futures_non_deal_peeks_none() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"channel":"push.depth","symbol":"BTC_USDT","data":{}}"#).unwrap();
        assert_eq!(MexcFuturesTradeParser.peek_symbol(&v), None);
    }
}
