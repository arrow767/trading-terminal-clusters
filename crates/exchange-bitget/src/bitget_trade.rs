//! Parse Bitget V2 public `trade` WS message.
//!
//! Message shape:
//! ```json
//! {
//!   "action":"snapshot",
//!   "arg":{"instType":"USDT-FUTURES","channel":"trade","instId":"BTCUSDT"},
//!   "data":[
//!     {"ts":"1700000000000","price":"42219.9","size":"0.5","side":"buy","tradeId":"123"}
//!   ]
//! }
//! ```
//!
//! Каждое сообщение — МАССИВ трейдов (как OKX/Bybit). Symbol берём из
//! `arg.instId` (он уже канон `BTCUSDT`, без разделителя).
//!
//! Аггрессор: Bitget `side="buy"` = тейкер купил (поднял ask) → `Bid`;
//! `"sell"` = тейкер продал → `Ask`. Совпадает с Bybit Buy/Sell. Регистр
//! не гарантирован — сравниваем case-insensitive (live-движок делает так же).
//!
//! Bitget USDT-FUTURES линейные: `size` уже в базовом активе — никакого
//! контракт-множителя (в отличие от OKX/KuCoin swap'ов).

use exchange_core::{AggressorSide, ExchangeError, Result, SymbolSpec, TradePrint};

use crate::scale::parse_scaled;

pub struct BitgetTradeParser;

impl BitgetTradeParser {
    /// Canonical symbol из `arg.instId`. None — не трейд-сообщение
    /// (subscribe ack / event / pong).
    pub fn peek_symbol(&self, v: &serde_json::Value) -> Option<String> {
        let arg = v.get("arg")?;
        if arg.get("channel").and_then(|x| x.as_str()) != Some("trade") {
            return None;
        }
        let inst = arg.get("instId").and_then(|x| x.as_str())?;
        Some(inst.to_uppercase())
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data").and_then(|x| x.as_array()) {
            Some(arr) => arr,
            None => return Ok(Vec::new()), // ack / event
        };
        let mut out = Vec::with_capacity(data.len());
        for t in data {
            out.push(parse_trade(t, spec)?);
        }
        Ok(out)
    }
}

fn parse_trade(t: &serde_json::Value, spec: &SymbolSpec) -> Result<TradePrint> {
    // ts — Bitget шлёт строкой ms (примем и число).
    let ts_ms = t
        .get("ts")
        .and_then(|x| {
            x.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| x.as_i64())
        })
        .ok_or_else(|| ExchangeError::Parse("trade: missing ts".into()))?;
    let side_str = t
        .get("side")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing side".into()))?;
    let aggressor = if side_str.eq_ignore_ascii_case("buy") {
        AggressorSide::Bid
    } else if side_str.eq_ignore_ascii_case("sell") {
        AggressorSide::Ask
    } else {
        return Err(ExchangeError::Parse(format!("trade: unknown side={side_str}")));
    };
    let price = t
        .get("price")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing price".into()))?;
    let size = t
        .get("size")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing size".into()))?;
    let price = parse_scaled(price, spec.price_scale)?;
    let qty = parse_scaled(size, spec.qty_scale)?;

    let trade_id: u64 = t
        .get("tradeId")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    Ok(TradePrint {
        exchange_ts_ns: ts_ms.saturating_mul(1_000_000),
        aggressor,
        price,
        qty,
        trade_id,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

    use super::*;

    fn spec(perp: bool) -> SymbolSpec {
        SymbolSpec {
            exchange: if perp { Exchange::BitgetF } else { Exchange::Bitget },
            market_type: if perp { MarketType::Perp } else { MarketType::Spot },
            quote: Quote::Usdt,
            symbol: "BTCUSDT".into(),
            price_scale: 1,
            qty_scale: 3,
            tick_size: 1,
            step_size: 1,
        }
    }

    const FRAME: &str = r#"
    {
      "action":"snapshot",
      "arg":{"instType":"USDT-FUTURES","channel":"trade","instId":"BTCUSDT"},
      "data":[
        {"ts":"1700000000123","price":"67234.5","size":"0.005","side":"buy","tradeId":"1"},
        {"ts":"1700000000456","price":"67234.4","size":"0.010","side":"Sell","tradeId":"2"}
      ]
    }"#;

    #[test]
    fn parses_batched_linear_trades() {
        let v: serde_json::Value = serde_json::from_str(FRAME).unwrap();
        let p = BitgetTradeParser;
        assert_eq!(p.peek_symbol(&v).as_deref(), Some("BTCUSDT"));
        let trades = p.parse_value(&v, &spec(true)).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].trade_id, 1);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        assert_eq!(trades[0].price, 672345); // 67234.5 × 10
        assert_eq!(trades[0].qty, 5); // 0.005 × 1000 (linear, no contract mult)
        assert_eq!(trades[0].exchange_ts_ns, 1_700_000_000_123 * 1_000_000);
        assert_eq!(trades[1].aggressor, AggressorSide::Ask); // case-insensitive "Sell"
        assert_eq!(trades[1].qty, 10);
    }

    #[test]
    fn non_trade_frame_peeks_none() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"event":"subscribe","arg":{"channel":"trade","instId":"BTCUSDT"}}"#)
                .unwrap();
        // event-ack: peek returns symbol but data is absent → no trades.
        assert!(BitgetTradeParser.parse_value(&v, &spec(true)).unwrap().is_empty());
    }

    #[test]
    fn books_channel_peeks_none() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"arg":{"channel":"books","instId":"BTCUSDT"},"data":[]}"#,
        )
        .unwrap();
        assert_eq!(BitgetTradeParser.peek_symbol(&v), None);
    }

    #[test]
    fn rejects_unknown_side() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"arg":{"channel":"trade","instId":"BTCUSDT"},"data":[{"ts":"1","price":"1","size":"1","side":"x"}]}"#,
        )
        .unwrap();
        assert!(BitgetTradeParser.parse_value(&v, &spec(true)).is_err());
    }
}
