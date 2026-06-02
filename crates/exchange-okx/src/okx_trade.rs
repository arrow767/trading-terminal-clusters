//! Parse OKX V5 `trades` WS message.
//!
//! Message shape:
//! ```json
//! {
//!   "arg": {"channel":"trades","instId":"BTC-USDT-SWAP"},
//!   "data": [
//!     {"instId":"BTC-USDT-SWAP","tradeId":"130639474","px":"42219.9",
//!      "sz":"3","side":"buy","ts":"1700000000000"}
//!   ]
//! }
//! ```
//!
//! Каждое сообщение — МАССИВ трейдов (как Bybit). Symbol берём из
//! `arg.instId` (верхний уровень), нормализуем в канон `BTCUSDT`.
//!
//! Аггрессор: OKX `side="buy"` = тейкер купил (поднял ask) → `Bid`;
//! `"sell"` = тейкер продал → `Ask`. Совпадает с Bybit Buy/Sell.
//!
//! Своп: `sz` в КОНТРАКТАХ. Переводим в базовый актив через зарегистрированную
//! при discovery ctVal-фракцию (`scale::get_ct`) — ТОЛЬКО для `MarketType::Perp`,
//! чтобы одноимённый спот не подхватил множитель свопа. Спот: `sz` уже в базе.

use exchange_core::{AggressorSide, ExchangeError, MarketType, Result, SymbolSpec, TradePrint};

use crate::scale::{get_ct, normalize_inst_id, parse_scaled};

pub struct OkxTradeParser;

impl OkxTradeParser {
    /// Canonical symbol из `arg.instId`. None — не трейд-сообщение
    /// (subscribe ack / pong / error).
    pub fn peek_symbol(&self, v: &serde_json::Value) -> Option<String> {
        let inst = v
            .get("arg")
            .and_then(|a| a.get("instId"))
            .and_then(|x| x.as_str())?;
        // только трейд-канал
        if v.get("arg").and_then(|a| a.get("channel")).and_then(|x| x.as_str()) != Some("trades") {
            return None;
        }
        Some(normalize_inst_id(inst))
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data").and_then(|x| x.as_array()) {
            Some(arr) => arr,
            None => return Ok(Vec::new()), // ack / heartbeat
        };
        // Контракт→база только для свопа; спот уже в базовом активе.
        let (ct_n, ct_d) = if spec.market_type == MarketType::Perp {
            get_ct(&spec.symbol)
        } else {
            (1, 1)
        };
        let mut out = Vec::with_capacity(data.len());
        for t in data {
            out.push(parse_trade(t, spec, ct_n, ct_d)?);
        }
        Ok(out)
    }
}

fn parse_trade(
    t: &serde_json::Value,
    spec: &SymbolSpec,
    ct_n: i64,
    ct_d: i64,
) -> Result<TradePrint> {
    // ts — строка ms (OKX всегда шлёт строкой, но примем и число).
    let ts_ms = t
        .get("ts")
        .and_then(|x| x.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| x.as_i64()))
        .ok_or_else(|| ExchangeError::Parse("trade: missing ts".into()))?;
    let side_str = t
        .get("side")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing side".into()))?;
    let aggressor = match side_str {
        "buy" => AggressorSide::Bid,
        "sell" => AggressorSide::Ask,
        other => return Err(ExchangeError::Parse(format!("trade: unknown side={other}"))),
    };
    let px = t
        .get("px")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing px".into()))?;
    let sz = t
        .get("sz")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing sz".into()))?;
    let price = parse_scaled(px, spec.price_scale)?;
    let qty = parse_scaled(sz, spec.qty_scale)? * ct_n / ct_d;

    // OKX отдаёт числовой tradeId строкой — используем для дедупа.
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

    fn spec(swap: bool) -> SymbolSpec {
        SymbolSpec {
            exchange: if swap { Exchange::OkxF } else { Exchange::Okx },
            market_type: if swap { MarketType::Perp } else { MarketType::Spot },
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
      "arg": {"channel":"trades","instId":"BTC-USDT-SWAP"},
      "data": [
        {"instId":"BTC-USDT-SWAP","tradeId":"1","px":"67234.5","sz":"100","side":"buy","ts":"1700000000123"},
        {"instId":"BTC-USDT-SWAP","tradeId":"2","px":"67234.4","sz":"5","side":"sell","ts":"1700000000456"}
      ]
    }"#;

    #[test]
    fn swap_applies_contract_multiplier() {
        // register ctVal 0.001 → (1,1000) for BTCUSDT swap
        crate::scale::set_ct("BTCUSDT", 1, 1000);
        let v: serde_json::Value = serde_json::from_str(FRAME).unwrap();
        let p = OkxTradeParser;
        assert_eq!(p.peek_symbol(&v).as_deref(), Some("BTCUSDT"));
        let trades = p.parse_value(&v, &spec(true)).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].trade_id, 1);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        assert_eq!(trades[0].price, 672345); // 67234.5 × 10
        // 100 contracts × 0.001 = 0.1 base → @scale 3 = 100
        assert_eq!(trades[0].qty, 100);
        assert_eq!(trades[1].aggressor, AggressorSide::Ask);
        assert_eq!(trades[1].qty, 5); // 5 × 0.001 = 0.005 → @scale 3 = 5
    }

    #[test]
    fn spot_qty_is_raw_base() {
        // Even if a swap registered a multiplier, spot must NOT apply it.
        crate::scale::set_ct("BTCUSDT", 1, 1000);
        let v: serde_json::Value = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"100.0","sz":"2","side":"buy","ts":"1"}]}"#,
        )
        .unwrap();
        let trades = OkxTradeParser.parse_value(&v, &spec(false)).unwrap();
        assert_eq!(trades[0].qty, 2000); // 2 × 10^3, NO ctVal applied
    }

    #[test]
    fn non_trade_frame_peeks_none() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"}}"#)
                .unwrap();
        // event-ack has no data; parse returns empty, peek still returns symbol
        // but parse_value yields nothing.
        assert!(OkxTradeParser.parse_value(&v, &spec(true)).unwrap().is_empty());
    }

    #[test]
    fn rejects_unknown_side() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"1","sz":"1","side":"x","ts":"1"}]}"#,
        )
        .unwrap();
        assert!(OkxTradeParser.parse_value(&v, &spec(true)).is_err());
    }
}
