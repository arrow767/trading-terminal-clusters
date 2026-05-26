//! Parse Bybit V5 `publicTrade` WS message.
//!
//! Message shape:
//! ```json
//! {
//!   "topic":"publicTrade.BTCUSDT",
//!   "type":"snapshot",
//!   "ts":1700000000000,
//!   "data":[
//!     {
//!       "T":1700000000000,   // trade time ms
//!       "s":"BTCUSDT",       // symbol
//!       "S":"Buy",           // taker side: Buy = aggressor BID, Sell = aggressor ASK
//!       "v":"0.001",         // qty (string)
//!       "p":"100.50",        // price (string)
//!       "i":"123456",        // trade id (string)
//!       "BT":false           // block trade flag (ignore)
//!     }, ...
//!   ]
//! }
//! ```
//!
//! Каждое сообщение содержит МАССИВ трейдов — мы выдаём `Vec<TradePrint>`
//! при парсинге. Сравните с Binance aggTrade: 1 трейд на сообщение.
//!
//! Аггрессор: Bybit `S = "Buy"` означает taker купил (BID агрессор),
//! `"Sell"` — taker продал (ASK агрессор). Аналог Binance `m`:
//! - m=true (buyer is maker) ⇒ taker is seller ⇒ AggressorSide::Ask
//! - S="Sell" ⇒ aggressor was seller ⇒ Ask. Совпадает.

use exchange_core::{AggressorSide, ExchangeError, Result, SymbolSpec, TradePrint};

use crate::scale::parse_scaled;

pub struct BybitTradeParser;

impl BybitTradeParser {
    /// Извлечь имя символа из `topic` поля верхнего уровня
    /// (формат `publicTrade.BTCUSDT`). Возвращает None для не-трейд
    /// сообщений (subscribe ack, pong, etc).
    pub fn peek_symbol<'a>(&self, v: &'a serde_json::Value) -> Option<&'a str> {
        let topic = v.get("topic").and_then(|x| x.as_str())?;
        topic.strip_prefix("publicTrade.")
    }

    /// Парсит ВСЕ трейды из одного WS-сообщения. На вход ожидается уже
    /// сдекодированный JSON (caller сам делает `serde_json::from_slice`),
    /// чтобы избежать двойного парсинга когда trace-логи включены.
    ///
    /// Все трейды в одном message имеют ОДИН symbol — gateway never
    /// батчит трейды разных инструментов в один frame.
    pub fn parse_value(&self, v: &serde_json::Value, spec: &SymbolSpec) -> Result<Vec<TradePrint>> {
        let data = match v.get("data").and_then(|x| x.as_array()) {
            Some(arr) => arr,
            // subscribe ack / heartbeat — no data field. Это норма, не ошибка.
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(data.len());
        for t in data {
            let trade = parse_trade(t, spec)?;
            out.push(trade);
        }
        Ok(out)
    }
}

fn parse_trade(t: &serde_json::Value, spec: &SymbolSpec) -> Result<TradePrint> {
    let ts_ms = t
        .get("T")
        .and_then(|x| x.as_i64())
        .ok_or_else(|| ExchangeError::Parse("trade: missing T".into()))?;
    let side_str = t
        .get("S")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing S".into()))?;
    let aggressor = match side_str {
        "Buy" => AggressorSide::Bid,
        "Sell" => AggressorSide::Ask,
        other => {
            return Err(ExchangeError::Parse(format!(
                "trade: unknown S={other}"
            )))
        }
    };
    let price_str = t
        .get("p")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing p".into()))?;
    let qty_str = t
        .get("v")
        .and_then(|x| x.as_str())
        .ok_or_else(|| ExchangeError::Parse("trade: missing v".into()))?;
    let price = parse_scaled(price_str, spec.price_scale)?;
    let qty = parse_scaled(qty_str, spec.qty_scale)?;

    // Trade id: Bybit отдаёт строкой, обычно числовое (но может быть UUID
    // для block-trades). Если parse провалится — берём 0. Это безопасно
    // для агрегатора: ID нужен лишь для дедупа в случае ретранслитов,
    // которых на public WS не бывает.
    let trade_id: u64 = t
        .get("i")
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

    fn spec() -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::BybitF,
            market_type: MarketType::Perp,
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
      "topic": "publicTrade.BTCUSDT",
      "type": "snapshot",
      "ts": 1700000000000,
      "data": [
        {"T":1700000000123,"s":"BTCUSDT","S":"Buy","v":"0.005","p":"67234.5","i":"100001","BT":false},
        {"T":1700000000456,"s":"BTCUSDT","S":"Sell","v":"0.010","p":"67234.4","i":"100002","BT":false}
      ]
    }"#;

    #[test]
    fn parses_batched_trades() {
        let v: serde_json::Value = serde_json::from_str(FRAME).unwrap();
        let p = BybitTradeParser;
        assert_eq!(p.peek_symbol(&v), Some("BTCUSDT"));
        let trades = p.parse_value(&v, &spec()).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].trade_id, 100001);
        assert_eq!(trades[0].aggressor, AggressorSide::Bid);
        // 67234.5 × 10 = 672345
        assert_eq!(trades[0].price, 672345);
        // 0.005 × 1000 = 5
        assert_eq!(trades[0].qty, 5);
        assert_eq!(trades[0].exchange_ts_ns, 1_700_000_000_123 * 1_000_000);

        assert_eq!(trades[1].aggressor, AggressorSide::Ask);
        assert_eq!(trades[1].qty, 10);
    }

    #[test]
    fn empty_data_is_ok() {
        let v: serde_json::Value = serde_json::from_str(r#"{"topic":"publicTrade.BTCUSDT"}"#).unwrap();
        let trades = BybitTradeParser.parse_value(&v, &spec()).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn rejects_unknown_side() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"topic":"publicTrade.BTCUSDT","data":[{"T":1,"S":"Maybe","v":"1","p":"1","i":"1"}]}"#
        ).unwrap();
        assert!(BybitTradeParser.parse_value(&v, &spec()).is_err());
    }

    #[test]
    fn peek_symbol_skips_non_trade_frames() {
        let v: serde_json::Value = serde_json::from_str(r#"{"op":"pong","req_id":"x"}"#).unwrap();
        assert_eq!(BybitTradeParser.peek_symbol(&v), None);
    }
}
