//! Parse KuCoin `/contractMarket/execution` (futures) + `/market/match` (spot)
//! WS messages.
//!
//! Envelope: `{"type":"message","topic":"/contractMarket/execution:XBTUSDTM",
//!            "data":{"price":..,"size":..,"side":"buy"|"sell","ts":<ns>}}`.
//! Spot match carries `time` (ns) instead of `ts`. price/size may be JSON
//! number or string.
//!
//! Aggressor: `side="buy"` = taker bought → `Bid`; `"sell"` → `Ask`.
//! Futures sizes are in CONTRACTS → base via the per-symbol multiplier
//! fraction (`scale::get_ct`), applied ONLY for `MarketType::Perp` (spot is
//! already in base).

use exchange_core::{AggressorSide, ExchangeError, MarketType, Result, SymbolSpec, TradePrint};

use crate::scale::{get_ct, parse_scaled};

pub struct KucoinTradeParser;

impl KucoinTradeParser {
    /// Venue symbol from `topic` (after the ':'); None for non-trade frames.
    pub fn peek_symbol<'a>(&self, v: &'a serde_json::Value) -> Option<&'a str> {
        if v.get("type").and_then(|x| x.as_str()) != Some("message") {
            return None;
        }
        let topic = v.get("topic").and_then(|x| x.as_str())?;
        if !(topic.contains("/execution") || topic.contains("/market/match")) {
            return None;
        }
        topic.rsplit(':').next()
    }

    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data") {
            Some(d) if d.is_object() => d,
            _ => return Ok(Vec::new()),
        };
        let (ct_n, ct_d) = if spec.market_type == MarketType::Perp {
            get_ct(&spec.symbol)
        } else {
            (1, 1)
        };

        let side_str = data
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

        let px = num_str(data.get("price"));
        let sz = num_str(data.get("size"));
        if px.is_empty() || sz.is_empty() {
            return Err(ExchangeError::Parse("trade: missing price/size".into()));
        }
        let price = parse_scaled(&px, spec.price_scale)?;
        let qty = parse_scaled(&sz, spec.qty_scale)? * ct_n / ct_d;

        // ts (futures) / time (spot) — nanoseconds. Без валидного ts трейд
        // принимать НЕЛЬЗЯ: с ts=0 он попал бы в окно «эпоха-1970», а
        // последующие реальные ts сломали бы оконную привязку (часть трейдов
        // дропнулась бы как late). Отвергаем — как все прочие парсеры при
        // отсутствии обязательного поля.
        let ts_ns = {
            let t = num_i64(data.get("ts"));
            if t != 0 {
                t
            } else {
                num_i64(data.get("time"))
            }
        };
        if ts_ns == 0 {
            return Err(ExchangeError::Parse("kucoin trade: missing ts/time".into()));
        }
        let exchange_ts_ns = ts_ns;

        Ok(vec![TradePrint {
            exchange_ts_ns,
            aggressor,
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

fn num_i64(v: Option<&serde_json::Value>) -> i64 {
    match v {
        Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
        Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

    use super::*;

    fn spec(perp: bool) -> SymbolSpec {
        SymbolSpec {
            exchange: if perp { Exchange::KucoinF } else { Exchange::Kucoin },
            market_type: if perp { MarketType::Perp } else { MarketType::Spot },
            quote: Quote::Usdt,
            symbol: "BTCUSDT".into(),
            price_scale: 1,
            qty_scale: 3,
            tick_size: 1,
            step_size: 1,
        }
    }

    #[test]
    fn futures_execution_contracts_to_base() {
        crate::scale::set_ct("BTCUSDT", 1, 1000);
        let v: serde_json::Value = serde_json::from_str(
            r#"{"type":"message","topic":"/contractMarket/execution:XBTUSDTM","data":{"price":"67234.5","size":100,"side":"buy","ts":1700000000123000000}}"#,
        )
        .unwrap();
        let p = KucoinTradeParser;
        assert_eq!(p.peek_symbol(&v), Some("XBTUSDTM"));
        let trades = p.parse_value(&v, &spec(true)).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        assert_eq!(trades[0].price, 672345); // 67234.5 × 10
        // 100 contracts → parse_scaled("100",3)=100000 × 1/1000 = 100 (=0.1 base @3)
        assert_eq!(trades[0].qty, 100);
        assert_eq!(trades[0].exchange_ts_ns, 1700000000123000000);
    }

    #[test]
    fn spot_match_no_contract_mult() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"type":"message","topic":"/market/match:BTC-USDT","data":{"price":"100.0","size":"2","side":"sell","time":1700000000000000000}}"#,
        )
        .unwrap();
        let trades = KucoinTradeParser.parse_value(&v, &spec(false)).unwrap();
        assert_eq!(trades[0].aggressor, AggressorSide::Ask);
        assert_eq!(trades[0].qty, 2000); // 2 × 10^3, no ct
    }

    #[test]
    fn non_trade_peeks_none() {
        let v: serde_json::Value = serde_json::from_str(r#"{"type":"pong"}"#).unwrap();
        assert_eq!(KucoinTradeParser.peek_symbol(&v), None);
    }

    #[test]
    fn rejects_trade_with_missing_timestamp() {
        // No `ts` (futures) / `time` (spot) → MUST reject, never bucket to
        // epoch-1970 (which would mis-window + drop subsequent real trades).
        let v: serde_json::Value = serde_json::from_str(
            r#"{"type":"message","topic":"/contractMarket/execution:XBTUSDTM","data":{"price":"100.0","size":1,"side":"buy"}}"#,
        )
        .unwrap();
        assert!(KucoinTradeParser.parse_value(&v, &spec(true)).is_err());
    }
}
