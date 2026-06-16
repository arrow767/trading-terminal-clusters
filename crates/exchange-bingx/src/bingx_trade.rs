//! Parse a BingX `<SYM>@trade` WS message → `Vec<TradePrint>`.
//!
//! Frame: `{"dataType":"BTC-USDT@trade","data": <array|object>}`.
//!   - FUTURES: `data` is an ARRAY `[{p,q,T,m}]` (no trade id).
//!   - SPOT:    `data` is a SINGLE object `{p,q,T,m,t}` (string id `t`).
//!
//! 🔴 Aggressor polarity differs by market (ported byte-for-byte from the live
//! engine `bingx/ws.rs::emit_trade`): FUTURES `m`=isBuyerMaker so `m=true` ⇒
//! seller is aggressor ⇒ Ask; SPOT `m` is INVERTED so `m=true` ⇒ Bid. Decided
//! here from `spec.market_type` (Perp vs Spot). qty is base-asset (no contract).

use exchange_core::{AggressorSide, ExchangeError, MarketType, Result, SymbolSpec, TradePrint};

use crate::scale::parse_scaled;

pub struct BingxTradeParser;

impl BingxTradeParser {
    /// Canonical symbol (BTCUSDT) from `dataType`; None if not a trade frame.
    pub fn peek_symbol(&self, v: &serde_json::Value) -> Option<String> {
        let dt = v.get("dataType").and_then(|x| x.as_str())?;
        if !dt.contains("@trade") {
            return None;
        }
        let venue = dt.split('@').next()?;
        Some(venue.replace('-', "").to_uppercase())
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };
        // futures = array, spot = single object.
        let owned;
        let items: &[serde_json::Value] = if let Some(arr) = data.as_array() {
            arr
        } else {
            owned = [data.clone()];
            &owned
        };
        let is_perp = spec.market_type == MarketType::Perp;
        let mut out = Vec::with_capacity(items.len());
        for it in items {
            let px = it.get("p").and_then(|x| x.as_str());
            let qy = it.get("q").and_then(|x| x.as_str());
            let (Some(px), Some(qy)) = (px, qy) else {
                continue;
            };
            // m = isBuyerMaker. Futures: m=true → sell (Ask). Spot: inverted.
            let m = it.get("m").and_then(|x| x.as_bool()).unwrap_or(false);
            let is_sell = if is_perp { m } else { !m };
            let aggressor = if is_sell {
                AggressorSide::Ask
            } else {
                AggressorSide::Bid
            };
            let price = parse_scaled(px, spec.price_scale)?;
            let qty = parse_scaled(qy, spec.qty_scale)?;
            let ts_ms = it.get("T").and_then(|x| x.as_i64()).unwrap_or(0);
            let trade_id = it
                .get("t")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            out.push(TradePrint {
                exchange_ts_ns: ts_ms.saturating_mul(1_000_000),
                aggressor,
                price,
                qty,
                trade_id,
            });
        }
        if out.is_empty() && !items.is_empty() {
            return Err(ExchangeError::Parse("bingx: no usable deals".into()));
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, Quote};

    use super::*;

    fn spec(market: MarketType, ex: Exchange) -> SymbolSpec {
        SymbolSpec {
            exchange: ex,
            market_type: market,
            quote: Quote::Usdt,
            symbol: "BTCUSDT".into(),
            price_scale: 1,
            qty_scale: 4,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn futures_array_m_true_is_ask() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"dataType":"BTC-USDT@trade","data":[{"p":"68960.5","q":"0.1000","T":1700000000123,"m":true}]}"#,
        )
        .unwrap();
        let p = BingxTradeParser;
        assert_eq!(p.peek_symbol(&v), Some("BTCUSDT".into()));
        let t = p.parse_value(&v, &spec(MarketType::Perp, Exchange::BingxF)).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].aggressor, AggressorSide::Ask); // futures m=true → sell
        assert_eq!(t[0].price, 689605);
        assert_eq!(t[0].qty, 1000);
    }

    #[test]
    fn spot_object_m_true_is_bid_inverted() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"dataType":"BTC-USDT@trade","data":{"p":"68960.5","q":"0.1000","T":1700000000123,"m":true,"t":"42"}}"#,
        )
        .unwrap();
        let t = BingxTradeParser
            .parse_value(&v, &spec(MarketType::Spot, Exchange::Bingx))
            .unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].aggressor, AggressorSide::Bid); // spot m=true → buy (inverted)
        assert_eq!(t[0].trade_id, 42);
    }

    #[test]
    fn non_trade_peeks_none() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"dataType":"BTC-USDT@depth100","data":{}}"#).unwrap();
        assert_eq!(BingxTradeParser.peek_symbol(&v), None);
    }
}
