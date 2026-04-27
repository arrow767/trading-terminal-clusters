use exchange_core::{AggressorSide, ExchangeError, Result, SymbolSpec, TradeParser, TradePrint};

use crate::scale::parse_scaled;

/// Parses Binance USD-M futures `aggTrade` events into `TradePrint`.
///
/// Accepts both raw-stream payloads (the inner aggTrade object directly)
/// and combined-stream payloads (`{"stream": "...", "data": {...}}`),
/// since whichever endpoint we connect to determines the wrapper. Any
/// other event type, or an event for a different symbol, returns
/// `Ok(None)` so the runtime can simply discard it without logging.
///
/// Aggressor mapping: Binance's `m` field is "is the buyer the maker?".
/// `m=true` → seller is the taker → `AggressorSide::Ask` (sell hit the
/// bid). `m=false` → buyer is the taker → `AggressorSide::Bid`. This
/// matches the convention used by `cluster-engine::Aggregator` and the
/// existing fat-terminal pipeline.
pub struct BinanceFuturesTradeParser;

impl BinanceFuturesTradeParser {
    /// Inspect a payload and return the contained symbol (e.g. "BTCUSDT")
    /// if it's an aggTrade event. Used by the WS runtime to route a
    /// frame to the right per-symbol sink before doing the full parse.
    pub fn peek_symbol<'a>(&self, v: &'a serde_json::Value) -> Option<&'a str> {
        let data = v.get("data").unwrap_or(v);
        if data.get("e").and_then(|e| e.as_str())? != "aggTrade" {
            return None;
        }
        data.get("s").and_then(|s| s.as_str())
    }

    /// Parse from an already-deserialized JSON value. Caller guarantees
    /// `spec.symbol` matches the event's `s` field — the runtime
    /// dispatches by symbol before calling this, so we skip re-checking.
    pub fn parse_value(
        &self,
        v: &serde_json::Value,
        spec: &SymbolSpec,
    ) -> Result<Option<TradePrint>> {
        let data = v.get("data").unwrap_or(v);
        if data.get("e").and_then(|e| e.as_str()) != Some("aggTrade") {
            return Ok(None);
        }
        match data.get("s").and_then(|s| s.as_str()) {
            Some(s) if s.eq_ignore_ascii_case(&spec.symbol) => {}
            _ => return Ok(None),
        }

        let trade_id = data
            .get("a")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ExchangeError::Parse("aggTrade: missing 'a'".into()))?
            as u64;
        let event_time_ms = data
            .get("T")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ExchangeError::Parse("aggTrade: missing 'T'".into()))?;
        let price_str = data
            .get("p")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExchangeError::Parse("aggTrade: missing 'p'".into()))?;
        let qty_str = data
            .get("q")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ExchangeError::Parse("aggTrade: missing 'q'".into()))?;
        let buyer_is_maker = data
            .get("m")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| ExchangeError::Parse("aggTrade: missing 'm'".into()))?;

        let price = parse_scaled(price_str, spec.price_scale)?;
        let qty = parse_scaled(qty_str, spec.qty_scale)?;
        let aggressor = if buyer_is_maker {
            AggressorSide::Ask
        } else {
            AggressorSide::Bid
        };

        Ok(Some(TradePrint {
            exchange_ts_ns: event_time_ms.saturating_mul(1_000_000),
            aggressor,
            price,
            qty,
            trade_id,
        }))
    }
}

impl TradeParser for BinanceFuturesTradeParser {
    fn parse(&self, raw: &[u8], spec: &SymbolSpec) -> Result<Option<TradePrint>> {
        let v: serde_json::Value =
            serde_json::from_slice(raw).map_err(|e| ExchangeError::Parse(e.to_string()))?;
        self.parse_value(&v, spec)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType, Quote};

    use super::*;

    fn spec() -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::BinanceF,
            market_type: MarketType::Perp,
            quote: Quote::Usdt,
            symbol: "BTCUSDT".into(),
            price_scale: 2,
            qty_scale: 3,
            tick_size: 10,
            step_size: 1,
        }
    }

    const RAW_AGG_TRADE: &str = r#"
    {
      "e": "aggTrade",
      "E": 123456789,
      "s": "BTCUSDT",
      "a": 5933014,
      "p": "67234.50",
      "q": "0.123",
      "f": 100,
      "l": 105,
      "T": 123456785,
      "m": true
    }
    "#;

    const COMBINED_STREAM: &str = r#"
    {
      "stream": "btcusdt@aggTrade",
      "data": {
        "e": "aggTrade",
        "E": 123456789,
        "s": "BTCUSDT",
        "a": 5933015,
        "p": "67234.50",
        "q": "0.500",
        "f": 100,
        "l": 105,
        "T": 123456786,
        "m": false
      }
    }
    "#;

    #[test]
    fn parses_raw_stream_aggressor_ask_when_buyer_is_maker() {
        let p = BinanceFuturesTradeParser;
        let trade = p.parse(RAW_AGG_TRADE.as_bytes(), &spec()).unwrap().unwrap();
        assert_eq!(trade.trade_id, 5_933_014);
        assert_eq!(trade.price, 6_723_450);
        assert_eq!(trade.qty, 123);
        assert_eq!(trade.aggressor, AggressorSide::Ask);
        assert_eq!(trade.exchange_ts_ns, 123_456_785 * 1_000_000);
    }

    #[test]
    fn parses_combined_stream_aggressor_bid_when_buyer_is_taker() {
        let p = BinanceFuturesTradeParser;
        let trade = p
            .parse(COMBINED_STREAM.as_bytes(), &spec())
            .unwrap()
            .unwrap();
        assert_eq!(trade.aggressor, AggressorSide::Bid);
        assert_eq!(trade.qty, 500);
    }

    #[test]
    fn ignores_other_event_types() {
        let p = BinanceFuturesTradeParser;
        let other = r#"{"e":"depthUpdate","s":"BTCUSDT"}"#;
        assert!(p.parse(other.as_bytes(), &spec()).unwrap().is_none());
    }

    #[test]
    fn ignores_other_symbol() {
        let p = BinanceFuturesTradeParser;
        let other = r#"{"e":"aggTrade","s":"ETHUSDT","a":1,"p":"1","q":"1","T":1,"m":false}"#;
        assert!(p.parse(other.as_bytes(), &spec()).unwrap().is_none());
    }

    #[test]
    fn rejects_malformed_payload() {
        let p = BinanceFuturesTradeParser;
        assert!(p.parse(b"not json", &spec()).is_err());
        // Valid JSON but missing required field
        let bad = r#"{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"1","q":"1","T":1}"#;
        assert!(p.parse(bad.as_bytes(), &spec()).is_err());
    }
}
