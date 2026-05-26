//! Bybit V5 adapter для cluster-ingest.
//!
//! Реализует:
//! - REST `/v5/market/instruments-info` → `ExchangeInfo`
//! - REST `/v5/market/tickers` → `VolumeRanker` (turnover24h)
//! - WS `/v5/public/{linear,spot}` → `WsConnector` (client-initiated ping)
//! - `publicTrade.{symbol}` envelope parser → `BybitTradeParser`
//!
//! Использование в supervisor — см. `cluster-ingest::bybit_supervisor`.

mod bybit_trade;
mod bybit_ws;
mod instruments_info;
mod scale;

pub use bybit_trade::BybitTradeParser;
pub use bybit_ws::BybitWs;
pub use instruments_info::{BybitCategory, BybitInstrumentsInfo};
