//! Parse Hyperliquid `trades` WS message.
//!
//! `{"channel":"trades","data":[{"coin":"BTC","side":"B"|"A","px":"...","sz":"...",
//!   "time":<ms>,"tid":<u64>}, ...]}`. One subscription = one coin, so all deals
//! in a frame share `coin`. Aggressor: `side="B"` (taker bought) → Bid;
//! `"A"`/`"S"` (taker sold) → Ask. HL is native (no contract multiplier).

use exchange_core::{AggressorSide, ExchangeError, Result, SymbolSpec, TradePrint};

use crate::scale::parse_scaled;

pub struct HyperliquidTradeParser;

impl HyperliquidTradeParser {
    /// Native coin from the first deal; None if not a trades frame.
    pub fn peek_symbol<'a>(&self, v: &'a serde_json::Value) -> Option<&'a str> {
        if v.get("channel").and_then(|x| x.as_str()) != Some("trades") {
            return None;
        }
        v.get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|t| t.get("coin"))
            .and_then(|c| c.as_str())
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data").and_then(|d| d.as_array()) {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(data.len());
        for t in data {
            let side = t.get("side").and_then(|x| x.as_str()).unwrap_or("");
            let aggressor = match side {
                "B" => AggressorSide::Bid,
                "A" | "S" => AggressorSide::Ask,
                _ => continue,
            };
            let px = t.get("px").and_then(|x| x.as_str());
            let sz = t.get("sz").and_then(|x| x.as_str());
            let (Some(px), Some(sz)) = (px, sz) else {
                continue;
            };
            let price = parse_scaled(px, spec.price_scale)?;
            let qty = parse_scaled(sz, spec.qty_scale)?;
            let time_ms = t.get("time").and_then(|x| x.as_i64()).unwrap_or(0);
            let trade_id = t.get("tid").and_then(|x| x.as_u64()).unwrap_or(0);
            out.push(TradePrint {
                exchange_ts_ns: time_ms.saturating_mul(1_000_000),
                aggressor,
                price,
                qty,
                trade_id,
            });
        }
        if out.is_empty() && !data.is_empty() {
            // all deals had an unknown side / missing px — not fatal.
            return Err(ExchangeError::Parse("hyperliquid: no usable deals".into()));
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

    use super::*;

    fn spec() -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::Hyperliquid,
            market_type: MarketType::Perp,
            quote: Quote::Usdc,
            symbol: "BTCUSDC".into(),
            price_scale: 1,
            qty_scale: 5,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn parses_trades() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"channel":"trades","data":[
                {"coin":"BTC","side":"B","px":"68960.5","sz":"0.10000","time":1700000000123,"tid":42},
                {"coin":"BTC","side":"A","px":"68960.4","sz":"0.20000","time":1700000000456,"tid":43}
            ]}"#,
        )
        .unwrap();
        let p = HyperliquidTradeParser;
        assert_eq!(p.peek_symbol(&v), Some("BTC"));
        let trades = p.parse_value(&v, &spec()).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        assert_eq!(trades[0].price, 689605); // 68960.5 × 10^1
        assert_eq!(trades[0].qty, 10000); // 0.1 × 10^5
        assert_eq!(trades[0].trade_id, 42);
        assert_eq!(trades[1].aggressor, AggressorSide::Ask);
    }

    #[test]
    fn non_trades_peeks_none() {
        let v: serde_json::Value = serde_json::from_str(r#"{"channel":"pong"}"#).unwrap();
        assert_eq!(HyperliquidTradeParser.peek_symbol(&v), None);
    }
}
